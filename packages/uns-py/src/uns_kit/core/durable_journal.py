"""Bounded same-host input storage. No external effect is retried implicitly."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


class JournalError(RuntimeError):
    pass


class JournalCapacityError(JournalError):
    pass


class JournalOwnershipError(JournalError):
    pass


def identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}", value):
        raise ValueError("Invalid bounded journal/owner/operation identifier")
    return value


def bounded_json(value: Any, *, max_bytes: int = 768 * 1024) -> bytes:
    nodes = 0

    def visit(item: Any, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > 50000 or depth > 32:
            raise JournalError("State exceeds structural bounds")
        if item is None or type(item) in (str, bool, int, float):
            return
        if type(item) is list:
            for child in item:
                visit(child, depth + 1)
        elif type(item) is dict and all(type(key) is str for key in item):
            for key, child in item.items():
                if key in ("__proto__", "prototype", "constructor"):
                    raise JournalError("Unsafe state key")
                visit(child, depth + 1)
        else:
            raise JournalError("State must be plain JSON")

    visit(value, 0)
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()
    if len(encoded) > max_bytes:
        raise JournalError("State exceeds byte bound")
    return encoded


@dataclass(frozen=True)
class JournalRecord:
    id: int
    topic: str
    payload: bytes
    received_at: float
    retained: bool


class DurableInputJournal:
    """SQLite FULL/WAL journal on a private local POSIX directory.

    Synchronous storage methods may block; async consumers offload them. MQTT
    admission commits synchronously in its network thread, before manual ACK.
    Separate processes may open the same journal. Receiver and consumer each
    have a distinct exclusive kernel lock, released by the OS after a crash.
    """

    def __init__(self, directory: str | Path, *, journal_id: str, contract: str,
                 create: bool = False, max_records: int = 20000,
                 max_bytes: int = 64 * 1024 * 1024,
                 max_message_bytes: int = 1024 * 1024,
                 max_disk_bytes: int = 256 * 1024 * 1024) -> None:
        if os.name != "posix":
            raise JournalError("Durable journal requires local POSIX storage and kernel locks")
        self.journal_id, self.contract = identifier(journal_id), identifier(contract)
        for value in (max_records, max_bytes, max_message_bytes, max_disk_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("Journal bounds must be positive integers")
        if max_records > 1000000 or max_bytes > 1024 ** 3 or max_message_bytes > 16 * 1024 ** 2 or not 1024 ** 2 <= max_disk_bytes <= 2 * 1024 ** 3:
            raise ValueError("Journal configuration exceeds supported storage bounds")
        self.directory = Path(directory)
        if not self.directory.is_absolute():
            raise ValueError("Journal directory must be absolute")
        for part in (self.directory, *self.directory.parents):
            if part.is_symlink():
                raise JournalError("Journal paths cannot contain symlinks")
        if create:
            self.directory.mkdir(mode=0o700, parents=False, exist_ok=True)
        info = self.directory.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise JournalError("Journal requires a private owner-only directory")
        self._limits = (max_records, max_bytes, max_message_bytes, max_disk_bytes)
        self.binding = hashlib.sha256(bounded_json({"protocol": 1, "journalId": journal_id,
            "contract": contract, "limits": list(self._limits)})).hexdigest()
        self._mutex = threading.RLock()
        self._locks: dict[str, int] = {}
        self._closed = False
        database = self.directory / "input.sqlite3"
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(database) + suffix)
            if candidate.is_symlink() or (candidate.exists() and not candidate.is_file()):
                raise JournalError("Unsafe journal database file")
        flags = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
        if create:
            flags |= os.O_CREAT
        fd = os.open(database, flags, 0o600)
        try:
            info = os.fstat(fd)
            if info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise JournalError("Journal database must be private")
            fresh = info.st_size == 0
            if info.st_size > max_disk_bytes or (fresh and not create):
                raise JournalError("Existing journal is empty or exceeds its disk bound")
        finally:
            os.close(fd)
        if not fresh:
            # Refuse foreign/unknown databases before changing any PRAGMA or
            # creating a WAL. Inspection is bounded and read-only.
            reader = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
            try:
                meta = dict(reader.execute("SELECT key,value FROM metadata WHERE key IN ('protocol','binding') AND length(value)<=128"))
                if meta.get("protocol") != "1" or meta.get("binding") != self.binding:
                    raise JournalError("Journal protocol, identity, semantic contract or bounds differ")
            except sqlite3.DatabaseError as error:
                raise JournalError("Existing file is not a supported input journal") from error
            finally:
                reader.close()
        self._db = sqlite3.connect(database, timeout=5, check_same_thread=False, isolation_level=None)
        try:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute("PRAGMA foreign_keys=ON")
            self._db.execute("PRAGMA wal_autocheckpoint=64")
            self._db.execute(f"PRAGMA max_page_count={max_disk_bytes // 4096}")
            with self._transaction() as db:
                if fresh and create and not db.execute("SELECT name FROM sqlite_master WHERE name='metadata'").fetchone():
                    db.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                    db.executemany("INSERT INTO metadata VALUES (?, ?)", [
                        ("protocol", "1"), ("binding", self.binding), ("owner", ""), ("receiver", "")])
                    db.execute("CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL, payload BLOB NOT NULL, received REAL NOT NULL, retained INTEGER NOT NULL, size INTEGER NOT NULL, state TEXT NOT NULL, owner TEXT NOT NULL)")
                    db.execute("CREATE TABLE deliveries(mid INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL, record_id INTEGER NOT NULL)")
                    db.execute("CREATE TABLE transfers(id TEXT PRIMARY KEY, source TEXT NOT NULL, target TEXT NOT NULL, offer TEXT NOT NULL, phase TEXT NOT NULL, snapshot BLOB, digest TEXT)")
                    db.execute("CREATE TABLE resolutions(id TEXT PRIMARY KEY, record_id INTEGER NOT NULL, decision TEXT NOT NULL)")
                meta = dict(db.execute("SELECT key, value FROM metadata"))
                if meta.get("protocol") != "1" or meta.get("binding") != self.binding:
                    raise JournalError("Journal protocol, identity, semantic contract or bounds differ")
            if self._db.execute("PRAGMA quick_check").fetchone() != ("ok",):
                raise JournalError("Journal integrity check failed")
        except BaseException:
            self._db.close()
            raise

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._mutex:
            if self._closed:
                raise JournalError("Journal is closed")
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def _lock(self, role: str) -> None:
        import fcntl
        if role in self._locks:
            raise JournalOwnershipError("This journal handle already owns that role")
        fd = os.open(self.directory / f"{role}.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException as error:
            os.close(fd)
            raise JournalOwnershipError("Journal role is already owned") from error
        self._locks[role] = fd

    def _unlock(self, role: str) -> None:
        fd = self._locks.pop(role, None)
        if fd is not None:
            os.close(fd)

    def acquire_receiver(self, identity: str) -> None:
        identity = identifier(identity)
        with self._mutex:
            self._lock("receiver")
            try:
                with self._transaction() as db:
                    existing = db.execute("SELECT value FROM metadata WHERE key='receiver'").fetchone()[0]
                    if existing and existing != identity:
                        raise JournalOwnershipError("Receiver session identity differs")
                    db.execute("UPDATE metadata SET value=? WHERE key='receiver'", (identity,))
            except BaseException:
                self._unlock("receiver")
                raise

    def release_receiver(self) -> None:
        with self._mutex:
            self._unlock("receiver")

    def admit(self, topic: str, payload: bytes, *, mid: int, duplicate: bool = False, retained: bool = False) -> int:
        if "receiver" not in self._locks:
            raise JournalOwnershipError("Only the receiver may admit input")
        if not isinstance(topic, str) or not topic or "\x00" in topic or any(x in topic for x in ("+", "#")) or len(topic.encode()) > 65535 or type(payload) is not bytes or type(mid) is not int or not 1 <= mid <= 65535:
            raise JournalError("Invalid MQTT delivery")
        size = len(topic.encode()) + len(payload)
        fingerprint = hashlib.sha256(topic.encode() + b"\x00" + payload).hexdigest()
        with self._transaction() as db:
            old = db.execute("SELECT fingerprint, record_id FROM deliveries WHERE mid=?", (mid,)).fetchone()
            if duplicate and old:
                if old[0] != fingerprint:
                    raise JournalError("Ambiguous broker packet identity; reconcile the session")
                return old[1]
            count, total = db.execute("SELECT count(*), coalesce(sum(size),0) FROM records").fetchone()
            disk = sum(file.stat().st_size for file in self.directory.glob("input.sqlite3*") if file.is_file())
            if size > self._limits[2] or count >= self._limits[0] or total + size > self._limits[1] or disk + size + 65536 > self._limits[3]:
                raise JournalCapacityError("Durable input capacity exceeded; delivery not acknowledged")
            cursor = db.execute("INSERT INTO records(topic,payload,received,retained,size,state,owner) VALUES (?,?,?,?,?,'pending','')", (topic, payload, time.time(), int(retained), size))
            record_id = cursor.lastrowid
            assert record_id is not None
            db.execute("INSERT OR REPLACE INTO deliveries VALUES (?,?,?)", (mid, fingerprint, record_id))
            return int(record_id)

    def acquire_consumer(self, owner: str, *, transfer_id: str | None = None) -> None:
        owner = identifier(owner)
        with self._mutex:
            self._lock("consumer")
            try:
                with self._transaction() as db:
                    unresolved = db.execute("SELECT id,source,target,phase FROM transfers WHERE phase!='completed'").fetchall()
                    if unresolved and (len(unresolved) != 1 or transfer_id != unresolved[0][0] or owner != unresolved[0][2] or unresolved[0][3] != "prepared"):
                        raise JournalOwnershipError("A handover fences this journal")
                    prior = db.execute("SELECT value FROM metadata WHERE key='owner'").fetchone()[0]
                    if prior and prior != owner and not transfer_id:
                        raise JournalOwnershipError("Previous owner requires reconciliation")
                    db.execute("UPDATE records SET state='unknown' WHERE state='running'")
                    unknown = db.execute("SELECT count(*) FROM records WHERE state='unknown'").fetchone()[0]
                    if not unknown:
                        db.execute("UPDATE metadata SET value=? WHERE key='owner'", (owner,))
                if unknown:
                    raise JournalError("An input effect is unconfirmed; explicit reconciliation required")
            except BaseException:
                self._unlock("consumer")
                raise

    def release_consumer(self, owner: str, *, transferred: bool = False) -> None:
        with self._transaction() as db:
            if "consumer" not in self._locks:
                return
            if db.execute("SELECT count(*) FROM records WHERE state!='pending'").fetchone()[0]:
                raise JournalError("Cannot release unconfirmed input")
            if not transferred:
                db.execute("UPDATE metadata SET value='' WHERE key='owner' AND value=?", (owner,))
        self._unlock("consumer")

    def take(self, owner: str) -> JournalRecord | None:
        with self._transaction() as db:
            if "consumer" not in self._locks or db.execute("SELECT value FROM metadata WHERE key='owner'").fetchone()[0] != owner:
                raise JournalOwnershipError("Consumer does not own the journal")
            row = db.execute("SELECT id,topic,payload,received,retained,state FROM records ORDER BY id LIMIT 1").fetchone()
            if row is None:
                return None
            if row[5] != "pending":
                raise JournalError("Input requires reconciliation before continuing")
            db.execute("UPDATE records SET state='running',owner=? WHERE id=?", (owner, row[0]))
            return JournalRecord(row[0], row[1], row[2], row[3], bool(row[4]))

    def settle(self, owner: str, record_id: int, *, confirmed: bool) -> None:
        with self._transaction() as db:
            row = db.execute("SELECT owner,state FROM records WHERE id=?", (record_id,)).fetchone()
            if "consumer" not in self._locks or row != (owner, "running"):
                raise JournalOwnershipError("Input completion does not match its running owner")
            if confirmed:
                db.execute("DELETE FROM records WHERE id=?", (record_id,))
            else:
                db.execute("UPDATE records SET state='unknown' WHERE id=?", (record_id,))

    def reconcile(self, record_id: int, *, resolution_id: str, decision: str) -> None:
        """Explicit local operator decision after reviewing the original effect.

        ``retry`` asserts that a replay is safe; the SDK cannot prove it. This
        method grants no platform authorization. No live consumer may exist.
        """
        identifier(resolution_id)
        if decision not in ("completed", "retry"):
            raise ValueError("Decision must be completed or retry")
        with self._mutex:
            self._lock("consumer")
            try:
                with self._transaction() as db:
                    old = db.execute("SELECT record_id,decision FROM resolutions WHERE id=?", (resolution_id,)).fetchone()
                    if old:
                        if old != (record_id, decision):
                            raise JournalError("Resolution ID was reused")
                        return
                    if db.execute("SELECT count(*) FROM resolutions").fetchone()[0] >= 1024:
                        raise JournalCapacityError("Resolution history full; retain/export and review storage")
                    row = db.execute("SELECT state FROM records WHERE id=?", (record_id,)).fetchone()
                    if not row or row[0] not in ("running", "unknown"):
                        raise JournalError("Only an unconfirmed input can be reconciled")
                    db.execute("INSERT INTO resolutions VALUES (?,?,?)", (resolution_id, record_id, decision))
                    if decision == "completed":
                        db.execute("DELETE FROM records WHERE id=?", (record_id,))
                    else:
                        db.execute("UPDATE records SET state='pending',owner='' WHERE id=?", (record_id,))
            finally:
                self._unlock("consumer")

    def status(self) -> dict[str, Any]:
        with self._mutex:
            rows = dict(self._db.execute("SELECT state,count(*) FROM records GROUP BY state"))
            size = self._db.execute("SELECT coalesce(sum(size),0) FROM records").fetchone()[0]
            return {"protocol": 1, "journal_id": self.journal_id, "pending": rows.get("pending", 0),
                "running": rows.get("running", 0), "unknown": rows.get("unknown", 0), "payload_bytes": size}

    def close(self) -> None:
        """Release this handle, not a successful business drain or reconciliation."""
        with self._mutex:
            if not self._closed:
                self._db.close()
                self._closed = True
                for role in tuple(self._locks):
                    self._unlock(role)
