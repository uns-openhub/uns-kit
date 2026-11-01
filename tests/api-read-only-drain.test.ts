import { request } from 'node:http';
import type { Response } from 'express';
import { describe, expect, it } from 'vitest';
import App from '../packages/uns-api/src/app.js';

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(r => { resolve = r; });
  return { promise, resolve };
};
const tick = () => new Promise<void>(r => setTimeout(r, 25));
async function listen(app: App) {
  await app.start();
  if (!app.server.listening) await new Promise<void>(r => app.server.once('listening', r));
  return `http://127.0.0.1:${(app.server.address() as { port: number }).port}/api`;
}
async function close(app: App) {
  app.server.closeAllConnections();
  await new Promise<void>(r => app.server.close(() => r()));
}

describe('explicit read-only HTTP work during ownership transfer', () => {
  // Two unrelated consumers: neither method, path nor application name implies safety.
  for (const [name, method, path] of [
    ['history', 'POST', '/measurements/batch'],
    ['inventory', 'GET', '/parts/search'],
  ]) it(`drains a disconnected ${name} reader only after its work settles`, async () => {
    const app = new App(0, name!, 'reader');
    let owned = true, completed = 0;
    const entered = deferred(), release = deferred(), serverClosed = deferred();
    app.setOwnershipGate(() => owned);
    app.router.all(path!, (req, res) => {
      res.once('close', serverClosed.resolve);
      void app.runReadOnlyRequest(res, async () => {
        entered.resolve(); await release.promise; completed++;
        if (!res.destroyed) res.json({ method: req.method });
      });
    });
    const base = await listen(app);
    const client = request(base + path, { method }); client.on('error', () => {}); client.end();
    try {
      await entered.promise; client.destroy(); await serverClosed.promise;
      owned = false;
      let drained = false;
      const drain = app.drainRequests().then(() => { drained = true; });
      await tick(); expect(drained).toBe(false); expect(completed).toBe(0);
      expect((await fetch(base + path)).status).toBe(503);
      release.resolve(); await drain;
      expect(completed).toBe(1);
      // Retrying the same drain after an unread response cannot rerun the handler.
      await app.drainRequests(); expect(completed).toBe(1);
      owned = true;
      expect((await fetch(base + path, { method })).status).toBe(200);
      owned = false; await app.drainRequests(); expect(completed).toBe(2);
    } finally { release.resolve(); client.destroy(); await close(app); }
  });

  it('waits for declared work even when the response was already sent', async () => {
    const app = new App(0, 'reports', 'early-response');
    const release = deferred(), settled = deferred();
    app.setOwnershipGate(() => true);
    app.router.get('/report', (_req, res) => {
      void app.runReadOnlyRequest(res, async () => {
        res.json({ ok: true }); await release.promise; settled.resolve();
      });
    });
    const base = await listen(app);
    try {
      expect((await fetch(base + '/report')).status).toBe(200);
      let drained = false;
      const drain = app.drainRequests().then(() => { drained = true; });
      await tick(); expect(drained).toBe(false);
      release.resolve(); await settled.promise; await drain;
    } finally { release.resolve(); await close(app); }
  });

  it('settles a failed read without claiming cancellation before the work ends', async () => {
    const app = new App(0, 'directory', 'failed-read');
    const entered = deferred(), release = deferred(), closed = deferred();
    let failure: unknown;
    app.setOwnershipGate(() => true);
    app.router.get('/lookup', (_req, res) => {
      res.once('close', closed.resolve);
      void app.runReadOnlyRequest(res, async () => {
        entered.resolve(); await release.promise; throw new Error('Read backend unavailable');
      }).catch(error => { failure = error; });
    });
    const base = await listen(app);
    const client = request(base + '/lookup'); client.on('error', () => {}); client.end();
    try {
      await entered.promise; client.destroy(); await closed.promise;
      release.resolve(); await app.drainRequests();
      expect(failure).toBeInstanceOf(Error);
    } finally { release.resolve(); client.destroy(); await close(app); }
  });

  for (const method of ['GET', 'POST']) it(`keeps an undeclared ${method} effect unknown after disconnect`, async () => {
    const app = new App(0, 'effects', method);
    const entered = deferred(), closed = deferred();
    let response!: Response, repeated = false;
    app.setOwnershipGate(() => true);
    app.router.all('/effect', (_req, res) => {
      response = res; res.once('close', closed.resolve); entered.resolve();
    });
    const base = await listen(app);
    const client = request(base + '/effect', { method }); client.on('error', () => {}); client.end();
    try {
      await entered.promise; client.destroy(); await closed.promise;
      await expect(app.drainRequests()).rejects.toThrow('without confirmation');
      await expect(app.runReadOnlyRequest(response, () => { repeated = true; })).rejects.toThrow('before the response ends');
      await expect(app.drainRequests()).rejects.toThrow('without confirmation');
      expect(repeated).toBe(false);
    } finally { client.destroy(); await close(app); }
  });

  it('rejects duplicate declarations without running the second callback', async () => {
    const app = new App(0, 'inventory', 'single-owner');
    let secondRan = false, duplicate: unknown;
    app.setOwnershipGate(() => true);
    app.router.get('/read', (_req, res) => {
      void app.runReadOnlyRequest(res, async () => {
        try { await app.runReadOnlyRequest(res, () => { secondRan = true; }); }
        catch (error) { duplicate = error; }
        res.json({ ok: true });
      });
    });
    const base = await listen(app);
    try {
      expect((await fetch(base + '/read')).status).toBe(200);
      await app.drainRequests(); expect(secondRan).toBe(false); expect(duplicate).toBeInstanceOf(Error);
    } finally { await close(app); }
  });

  it('retains standalone operation without an ownership gate', async () => {
    const app = new App(0, 'standalone', 'read');
    app.router.get('/read', (_req, res) => {
      void app.runReadOnlyRequest(res, () => { res.json({ value: 9 }); });
    });
    const base = await listen(app);
    try { expect(await (await fetch(base + '/read')).json()).toEqual({ value: 9 }); }
    finally { await app.stop(); }
  });
});
