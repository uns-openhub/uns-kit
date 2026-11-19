from __future__ import annotations

import re

from ..version import __package_name__, __version__
from .runtime_metadata import RUNTIME_METADATA

LIBRARY_PACKAGE_NAME = __package_name__


class TopicBuilder:
    """
    Mirrors the TypeScript MqttTopicBuilder for infra topics.
    """

    def __init__(
        self,
        package_name: str | None = None,
        package_version: str | None = None,
        process_name: str | None = None,
    ):
        # Backward compatible constructor:
        # - TopicBuilder("pkg", "ver", "proc")  -> old style
        # - TopicBuilder("proc")                -> process-only style
        if process_name is None and package_version is None and package_name is not None:
            process_name = package_name
            package_name = None

        resolved_package_name = package_name or RUNTIME_METADATA.package_name or LIBRARY_PACKAGE_NAME
        resolved_package_version = package_version or RUNTIME_METADATA.package_version or __version__
        resolved_process_name = process_name or "uns-process"

        self.package_name = self.sanitize_topic_part(resolved_package_name)
        self.package_version = self.sanitize_topic_part(resolved_package_version)
        self.process_name = self.sanitize_topic_part(resolved_process_name)
        self._base = f"uns-infra/{self.package_name}/{self.package_version}/{self.process_name}/"
        if not re.match(r"^uns-infra/[^/]+/[^/]+/[^/]+/$", self._base):
            raise ValueError("processStatusTopic must follow 'uns-infra/<package>/<version>/<process>/'")

    @staticmethod
    def sanitize_topic_part(name: str) -> str:
        sanitized = re.sub(r"[^a-zA-Z0-9_-]+", "-", name)
        sanitized = re.sub(r"-{2,}", "-", sanitized)
        sanitized = sanitized.strip("-")
        return sanitized or "uns-process"

    @property
    def process_status_topic(self) -> str:
        return self._base

    def active_topic(self) -> str:
        return f"{self._base}active"

    def handover_topic(self) -> str:
        return f"{self._base}handover"

    def service_metadata_topic(self) -> str:
        return f"{self._base}service-metadata"

    def wildcard_active_topic(self) -> str:
        parts = self._base.strip("/").split("/")
        if len(parts) < 2:
            raise ValueError("processStatusTopic must follow 'uns-infra/<package>/<version>/<process>/'")
        return "/".join(parts[:2]) + "/+/+/active"

    def instance_status_topic(self, instance_name: str) -> str:
        sanitized = self.sanitize_topic_part(instance_name)
        return f"{self._base}{sanitized}/"

    @staticmethod
    def extract_base_topic(full_topic: str) -> str:
        parts = full_topic.split("/")
        if len(parts) < 4:
            raise ValueError("Invalid topic format. Expected 'uns-infra/<package>/<version>/<process>/'.")
        return "/".join(parts[:4]) + "/"
