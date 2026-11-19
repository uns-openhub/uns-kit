from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class BundleMetadata:
    name: str
    display_name: Optional[str] = None
    service_type: Optional[str] = None
    summary: Optional[str] = None
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class BundleScaffold:
    stack: str
    template: str
    features: List[str] = field(default_factory=list)


@dataclass
class BundleRepository:
    provider: Optional[str] = None
    organization: Optional[str] = None
    project: Optional[str] = None
    repository: Optional[str] = None
    default_branch: Optional[str] = None


@dataclass
class BundleDomain:
    inputs: List[Any] = field(default_factory=list)
    outputs: List[Any] = field(default_factory=list)


@dataclass
class BundleServiceSpecDocs:
    goals: List[str] = field(default_factory=list)
    non_goals: List[str] = field(default_factory=list)
    acceptance_criteria: List[str] = field(default_factory=list)


@dataclass
class BundleAgentsDocs:
    project_context: List[str] = field(default_factory=list)
    guardrails: List[str] = field(default_factory=list)
    first_tasks: List[str] = field(default_factory=list)
    verification: List[str] = field(default_factory=list)


@dataclass
class BundleDocs:
    service_spec: BundleServiceSpecDocs = field(default_factory=BundleServiceSpecDocs)
    agents: BundleAgentsDocs = field(default_factory=BundleAgentsDocs)


@dataclass
class ServiceBundle:
    schema_version: int
    kind: str
    metadata: BundleMetadata
    scaffold: BundleScaffold
    repository: Optional[BundleRepository] = None
    domain: Optional[BundleDomain] = None
    docs: BundleDocs = field(default_factory=BundleDocs)


def load_service_bundle(
    bundle_path: Path,
    *,
    expected_stack: str,
    cli_name: str,
    counterpart_cli_name: str,
) -> Tuple[ServiceBundle, str]:
    try:
        raw = bundle_path.read_text()
    except OSError as exc:
        raise ValueError(f"Failed to read bundle JSON at {bundle_path}: {exc}") from exc

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse bundle JSON at {bundle_path}: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError("bundle must be a JSON object.")

    bundle = _validate_service_bundle(
        parsed,
        expected_stack=expected_stack,
        cli_name=cli_name,
        counterpart_cli_name=counterpart_cli_name,
    )
    return bundle, raw


def generate_service_spec_markdown(bundle: ServiceBundle) -> str:
    lines = [
        "# SERVICE_SPEC",
        "",
        "> Generated from `service.bundle.json`. Update the bundle as the source of truth.",
        "",
        "## Service Identity",
        *_render_key_value_list(
            [
                ("Name", bundle.metadata.name),
                ("Display Name", bundle.metadata.display_name),
                ("Service Type", bundle.metadata.service_type),
                ("Owner", bundle.metadata.owner),
                ("Tags", ", ".join(bundle.metadata.tags) if bundle.metadata.tags else "None"),
            ]
        ),
        "",
        "## Summary",
        "",
        bundle.metadata.summary or "Not provided.",
        "",
        "## Description",
        "",
        bundle.metadata.description or "Not provided.",
        "",
        "## Scaffold",
        *_render_key_value_list(
            [
                ("Stack", bundle.scaffold.stack),
                ("Template", bundle.scaffold.template),
                ("Features", ", ".join(bundle.scaffold.features) if bundle.scaffold.features else "None"),
            ]
        ),
    ]

    if bundle.repository:
        lines.extend(
            [
                "",
                "## Repository",
                *_render_key_value_list(
                    [
                        ("Provider", bundle.repository.provider),
                        ("Organization", bundle.repository.organization),
                        ("Project", bundle.repository.project),
                        ("Repository", bundle.repository.repository),
                        ("Default Branch", bundle.repository.default_branch),
                    ]
                ),
            ]
        )

    if bundle.domain:
        lines.extend(
            [
                "",
                "## Domain Inputs",
                *_render_unknown_list(bundle.domain.inputs),
                "",
                "## Domain Outputs",
                *_render_unknown_list(bundle.domain.outputs),
            ]
        )

    lines.extend(
        [
            "",
            "## Goals",
            *_render_string_list(bundle.docs.service_spec.goals),
            "",
            "## Non-Goals",
            *_render_string_list(bundle.docs.service_spec.non_goals),
            "",
            "## Acceptance Criteria",
            *_render_string_list(bundle.docs.service_spec.acceptance_criteria),
            "",
        ]
    )
    return "\n".join(lines)


def generate_agents_markdown(bundle: ServiceBundle) -> str:
    guardrails = list(bundle.docs.agents.guardrails)
    if bundle.scaffold.stack == "python":
        publishing_guardrail = (
            "For UNS topic outputs, use `UnsProxyProcess` with `create_mqtt_proxy`. "
            "Do not implement ad-hoc direct MQTT publishing unless explicitly required."
        )
        if publishing_guardrail not in guardrails:
            guardrails.insert(0, publishing_guardrail)

    lines = [
        "# AGENTS",
        "",
        "> This repository was bootstrapped from `service.bundle.json`. Regenerate derived docs from the bundle instead of treating this file as the source of truth.",
        "",
        "## Service",
        *_render_key_value_list(
            [
                ("Name", bundle.metadata.name),
                ("Display Name", bundle.metadata.display_name),
                ("Stack", bundle.scaffold.stack),
                ("Template", bundle.scaffold.template),
            ]
        ),
        "",
        "## Project Context",
        *_render_string_list(bundle.docs.agents.project_context),
        "",
        "## Guardrails",
        *_render_string_list(guardrails),
        "",
        "## First Tasks",
        *_render_string_list(bundle.docs.agents.first_tasks),
        "",
        "## Verification",
        *_render_string_list(bundle.docs.agents.verification),
        "",
        "## Python SDK upgrades",
        "- Before updating uns-kit, record the installed and target versions and read the bundled uns_kit/MIGRATIONS.md.",
        "- Crossing <0.2.10 to >=0.2.10 adds opt-in durable QoS 1 input and same-host Python journal handover protocol 1. Keep the receiver independent, bind semantic resources/launch IDs and reconcile unknown effects; never declare TS correlated-v2 or Controller process-group support from these helpers. Read docs/python-durable-input-handover.md.",
        "- Crossing <0.2.9 to >=0.2.9 adds managed MQTT input/HTTP drain and strict bounded process-owned output. Register one input handler and await start_input; await accepted effects and reconcile unknown HTTP/input/publish outcomes. Local managed_ownership is not a host lease or hot handover capability.",
        "- Crossing <0.2.8 to >=0.2.8 requires current Controller authority for service API JWTs. Configured UNS grants are not service API permissions. PostgreSQL/Oracle no longer replay failed SQL; async cancellation does not roll back an accepted effect. Await client cleanup and reconcile uncertain operations.",
        "- Crossing <0.2.7 to >=0.2.7 changes async process stop error aggregation and forbids restarting a stopped owner. Opt-in lifecycle hooks must stop new input admission before draining accepted work; timeout does not cancel effects. Keep one application signal owner and never infer hot handover support from the SDK version.",
        "",
    ]
    return "\n".join(lines)


def _validate_service_bundle(
    parsed: Dict[str, Any],
    *,
    expected_stack: str,
    cli_name: str,
    counterpart_cli_name: str,
) -> ServiceBundle:
    schema_version = parsed.get("schemaVersion")
    if schema_version != 1:
        raise ValueError(f"service bundle schemaVersion must be 1. Received {schema_version!r}.")

    kind = parsed.get("kind")
    if kind != "uns-service-bundle":
        raise ValueError(f'service bundle kind must be "uns-service-bundle". Received {kind!r}.')

    metadata = _require_object(parsed.get("metadata"), "metadata")
    scaffold = _require_object(parsed.get("scaffold"), "scaffold")
    repository = _optional_object(parsed.get("repository"), "repository")
    domain = _optional_object(parsed.get("domain"), "domain")
    docs = _optional_object(parsed.get("docs"), "docs")
    _optional_object(parsed.get("provenance"), "provenance")

    bundle = ServiceBundle(
        schema_version=1,
        kind="uns-service-bundle",
        metadata=BundleMetadata(
            name=_require_non_empty_string(metadata.get("name"), "metadata.name"),
            display_name=_optional_non_empty_string(metadata.get("displayName"), "metadata.displayName"),
            service_type=_optional_non_empty_string(metadata.get("serviceType"), "metadata.serviceType"),
            summary=_optional_non_empty_string(metadata.get("summary"), "metadata.summary"),
            description=_optional_non_empty_string(metadata.get("description"), "metadata.description"),
            owner=_optional_non_empty_string(metadata.get("owner"), "metadata.owner"),
            tags=_optional_string_list(metadata.get("tags"), "metadata.tags"),
        ),
        scaffold=BundleScaffold(
            stack=_require_non_empty_string(scaffold.get("stack"), "scaffold.stack"),
            template=_require_non_empty_string(scaffold.get("template"), "scaffold.template"),
            features=_dedupe(_optional_string_list(scaffold.get("features"), "scaffold.features")),
        ),
        repository=BundleRepository(
            provider=_optional_non_empty_string(repository.get("provider"), "repository.provider") if repository else None,
            organization=_optional_non_empty_string(repository.get("organization"), "repository.organization") if repository else None,
            project=_optional_non_empty_string(repository.get("project"), "repository.project") if repository else None,
            repository=_optional_non_empty_string(repository.get("repository"), "repository.repository") if repository else None,
            default_branch=_optional_non_empty_string(repository.get("defaultBranch"), "repository.defaultBranch") if repository else None,
        ) if repository else None,
        domain=BundleDomain(
            inputs=_optional_list(domain.get("inputs"), "domain.inputs"),
            outputs=_optional_list(domain.get("outputs"), "domain.outputs"),
        ) if domain else None,
        docs=BundleDocs(
            service_spec=BundleServiceSpecDocs(
                goals=_optional_string_list(_optional_object(docs.get("serviceSpec"), "docs.serviceSpec").get("goals") if docs and _optional_object(docs.get("serviceSpec"), "docs.serviceSpec") else None, "docs.serviceSpec.goals"),
                non_goals=_optional_string_list(_optional_object(docs.get("serviceSpec"), "docs.serviceSpec").get("nonGoals") if docs and _optional_object(docs.get("serviceSpec"), "docs.serviceSpec") else None, "docs.serviceSpec.nonGoals"),
                acceptance_criteria=_optional_string_list(_optional_object(docs.get("serviceSpec"), "docs.serviceSpec").get("acceptanceCriteria") if docs and _optional_object(docs.get("serviceSpec"), "docs.serviceSpec") else None, "docs.serviceSpec.acceptanceCriteria"),
            ),
            agents=BundleAgentsDocs(
                project_context=_optional_string_list(_optional_object(docs.get("agents"), "docs.agents").get("projectContext") if docs and _optional_object(docs.get("agents"), "docs.agents") else None, "docs.agents.projectContext"),
                guardrails=_optional_string_list(_optional_object(docs.get("agents"), "docs.agents").get("guardrails") if docs and _optional_object(docs.get("agents"), "docs.agents") else None, "docs.agents.guardrails"),
                first_tasks=_optional_string_list(_optional_object(docs.get("agents"), "docs.agents").get("firstTasks") if docs and _optional_object(docs.get("agents"), "docs.agents") else None, "docs.agents.firstTasks"),
                verification=_optional_string_list(_optional_object(docs.get("agents"), "docs.agents").get("verification") if docs and _optional_object(docs.get("agents"), "docs.agents") else None, "docs.agents.verification"),
            ),
        ),
    )

    if bundle.scaffold.stack != expected_stack:
        raise ValueError(
            f'Bundle scaffold.stack is "{bundle.scaffold.stack}". Use {counterpart_cli_name} create --bundle <path> instead of {cli_name}.'
        )

    if bundle.scaffold.template != "default":
        raise ValueError(
            f'Bundle scaffold.template must be "default" for this MVP. Received "{bundle.scaffold.template}".'
        )

    return bundle


def _require_object(value: Any, path: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be an object.")
    return value


def _optional_object(value: Any, path: str) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    return _require_object(value, path)


def _require_non_empty_string(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path} is required and must be a non-empty string.")
    return value.strip()


def _optional_non_empty_string(value: Any, path: str) -> Optional[str]:
    if value is None or value == "":
        return None
    return _require_non_empty_string(value, path)


def _optional_string_list(value: Any, path: str) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{path} must be an array of strings.")
    return [_require_non_empty_string(item, f"{path}[{index}]") for index, item in enumerate(value)]


def _optional_list(value: Any, path: str) -> List[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{path} must be an array.")
    return value


def _dedupe(items: List[str]) -> List[str]:
    seen = set()
    output: List[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            output.append(item)
    return output


def _render_key_value_list(entries: List[Tuple[str, Optional[str]]]) -> List[str]:
    return [f"- {label}: {value if value else 'Not specified'}" for label, value in entries]


def _render_string_list(items: List[str]) -> List[str]:
    if not items:
        return ["- None specified."]
    return [f"- {item}" for item in items]


def _render_unknown_list(items: List[Any]) -> List[str]:
    if not items:
        return ["- None specified."]

    rendered: List[str] = []
    for item in items:
        if isinstance(item, str) and item.strip():
            rendered.append(f"- {item}")
        else:
            rendered.append(f"- `{json.dumps(item, sort_keys=True)}`")
    return rendered
