from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import shutil
import importlib.resources
from pathlib import Path
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, Optional, Tuple

import click

from .service_bundle import (
    ServiceBundle,
    generate_agents_markdown,
    generate_service_spec_markdown,
    load_service_bundle,
)
from .core.config_schema import generate_config_schema
from .core.topic_builder import TopicBuilder
from .version import __package_name__, __version__


CLI_PROG_NAME = "uns-kit-py"
DEFAULT_UNS_DATAHUB_ADDON_METADATA = {
    "schemaVersion": 1,
    "kind": "addon",
    "controllerCompatibility": ">=2 <3",
}


def common_options(func):
    func = click.option("--host", required=True, help="MQTT host (hostname or host:port)")(func)
    func = click.option("--port", type=int, help="MQTT port override")(func)
    func = click.option("--username", help="MQTT username")(func)
    func = click.option("--password", help="MQTT password")(func)
    func = click.option("--tls/--no-tls", default=False, show_default=True, help="Enable TLS")(func)
    func = click.option("--client-id", help="MQTT clientId")(func)
    func = click.option("--package-name", default=__package_name__ or "uns-kit", show_default=True, help="Package name for infra topics")(func)
    func = click.option("--package-version", default=__version__, show_default=True, help="Package version for infra topics")(func)
    func = click.option("--process-name", default="uns-process", show_default=True, help="Process name for infra topics")(func)
    func = click.option("--reconnect-interval", default=1.0, show_default=True, type=float, help="Reconnect backoff start (s)")(func)
    return func


@click.group(context_settings={"help_option_names": ["--help", "-h"]}, help="Lightweight UNS MQTT helper (Python).")
def cli():
    pass


@cli.command("help", help="Show this message.")
def help_cmd():
    click.echo(render_cli_help())


@cli.command("publish", help="Publish a UNS data packet to a topic.")
@common_options
@click.option("--topic", required=True, help="MQTT topic (e.g. raw/data/)")
@click.option("--value", required=False, help="Value to send (stringified if not JSON).")
@click.option("--uom", default=None, help="Unit of measure.")
@click.option("--json", "json_value", default=None, help="JSON string to use as value.")
@click.option("--qos", type=int, default=0, show_default=True)
@click.option("--retain/--no-retain", default=False, show_default=True)
def publish_cmd(**opts):
    asyncio.run(_run_publish(**opts))


async def _run_publish(
    host: str,
    port: Optional[int],
    username: Optional[str],
    password: Optional[str],
    tls: bool,
    client_id: Optional[str],
    package_name: str,
    package_version: str,
    process_name: str,
    reconnect_interval: float,
    topic: str,
    value: Optional[str],
    uom: Optional[str],
    json_value: Optional[str],
    qos: int,
    retain: bool,
):
    # Local import so non-MQTT commands (create/configure/pull-request) don't
    # import MQTT client libraries.
    from .core.client import UnsMqttClient
    from .core.packet import UnsPacket

    tb = TopicBuilder(package_name, package_version, process_name)
    client = UnsMqttClient(
        host=host.split(":")[0],
        port=int(host.split(":")[1]) if ":" in host and port is None else port,
        username=username,
        password=password,
        tls=tls,
        client_id=client_id,
        topic_builder=tb,
        reconnect_interval=reconnect_interval,
    )
    await client.connect()

    payload_obj = json.loads(json_value) if json_value is not None else value
    packet = UnsPacket.data(value=payload_obj, uom=uom)
    await client.publish_packet(topic, packet, qos=qos, retain=retain)
    await client.close()


@cli.command("subscribe", help="Subscribe to one or more topics (resilient).")
@common_options
@click.option("--topic", "topic_filter", required=True, help="Topic filter (e.g. uns-infra/#)")
def subscribe_cmd(**opts):
    asyncio.run(_run_subscribe(**opts))


async def _run_subscribe(
    host: str,
    port: Optional[int],
    username: Optional[str],
    password: Optional[str],
    tls: bool,
    client_id: Optional[str],
    package_name: str,
    package_version: str,
    process_name: str,
    reconnect_interval: float,
    topic_filter: str,
):
    # Local import so non-MQTT commands (create/configure/pull-request) don't
    # import MQTT client libraries.
    from .core.client import UnsMqttClient

    tb = TopicBuilder(package_name, package_version, process_name)
    client = UnsMqttClient(
        host=host.split(":")[0],
        port=int(host.split(":")[1]) if ":" in host and port is None else port,
        username=username,
        password=password,
        tls=tls,
        client_id=client_id,
        topic_builder=tb,
        reconnect_interval=reconnect_interval,
    )
    await client.connect()

    async for msg in client.resilient_messages(topic_filter):
        try:
            print(f"{msg.topic} {msg.payload.decode()}")
        except Exception:
            print(f"{msg.topic} <binary {len(msg.payload)} bytes>")


@cli.command("create", help="Create a new UNS Python app (default template or service bundle).")
@click.argument("name", required=False)
@click.option("--bundle", "bundle_path", type=click.Path(path_type=Path, dir_okay=False), help="Path to service.bundle.json")
@click.option("--dest", "dest_path", type=click.Path(path_type=Path, file_okay=False), help="Target directory override for bundle-based create")
@click.option("--allow-existing", is_flag=True, default=False, help="Allow bundle scaffolding into an existing non-empty directory")
def create(name: Optional[str], bundle_path: Optional[Path], dest_path: Optional[Path], allow_existing: bool):
    if bundle_path is not None:
        if name:
            raise click.ClickException("Do not pass a positional project name with --bundle. Use --dest to override the target directory.")
        _create_from_bundle(bundle_path, dest_path, allow_existing)
        return

    if not name:
        raise click.ClickException("Missing project name. Example: uns-kit-py create my-app")
    if dest_path is not None:
        raise click.ClickException("--dest can only be used with --bundle.")
    if allow_existing:
        raise click.ClickException("--allow-existing can only be used with --bundle.")

    target_path = Path(name).resolve()
    project_name = target_path.name
    initialized_git = _scaffold_python_project(target_path, project_name, allow_existing=False)
    _print_python_create_success(target_path, initialized_git)


def _create_from_bundle(bundle_path: Path, dest_path: Optional[Path], allow_existing: bool) -> None:
    try:
        bundle, raw_bundle = load_service_bundle(
            bundle_path.resolve(),
            expected_stack="python",
            cli_name="uns-kit-py",
            counterpart_cli_name="uns-kit",
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    target_path = (dest_path or Path(bundle.metadata.name)).resolve()
    initialized_git = _scaffold_python_project(target_path, bundle.metadata.name, allow_existing=allow_existing)
    _apply_python_bundle_features(target_path, bundle)
    _write_python_bundle_artifacts(target_path, bundle, raw_bundle)
    _print_python_create_success(target_path, initialized_git)


def _scaffold_python_project(target_path: Path, project_name: str, *, allow_existing: bool) -> bool:
    template_root = importlib.resources.files("uns_kit").joinpath("templates/default")
    _ensure_target_dir(target_path, allow_existing=allow_existing)
    _copy_template_tree(template_root, target_path)

    initial_app_version = "0.0.0"
    pyproject_path = target_path / "pyproject.toml"
    if pyproject_path.exists():
        try:
            initial_app_version = _read_poetry_version(pyproject_path)
        except Exception:
            pass

    config_path = target_path / "config.json"
    _write_config_file(config_path, project_name)
    _write_runtime_profiles(target_path, project_name)
    generate_config_schema(target_path)

    if pyproject_path.exists():
        try:
            _personalize_pyproject(pyproject_path, project_name)
            _configure_local_uns_kit_dependency(pyproject_path)
        except Exception:
            pass

    pkg_path = target_path / "package.json"
    if pkg_path.exists():
        try:
            pkg_data = json.loads(pkg_path.read_text())
            pkg_data["name"] = project_name
            pkg_data["version"] = initial_app_version
            _ensure_uns_datahub_addon_metadata(pkg_data)
            pkg_path.write_text(json.dumps(pkg_data, indent=2) + "\n")
        except Exception:
            pass

    return _init_git_repository(target_path)


def _ensure_target_dir(target_path: Path, *, allow_existing: bool) -> None:
    if target_path.exists():
        if not target_path.is_dir():
            raise click.ClickException(f"Path exists and is not a directory: {target_path}")
        if any(target_path.iterdir()) and not allow_existing:
            raise click.ClickException(f"Destination is not empty: {target_path}")
        return

    target_path.mkdir(parents=True, exist_ok=True)


def _copy_template_tree(source_root: Any, destination_root: Path) -> None:
    for entry in source_root.iterdir():
        destination = destination_root / entry.name
        if entry.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            _copy_template_tree(entry, destination)
            continue

        if destination.exists():
            continue

        destination.parent.mkdir(parents=True, exist_ok=True)
        with entry.open("rb") as src_handle, destination.open("wb") as dst_handle:
            shutil.copyfileobj(src_handle, dst_handle)


def _apply_python_bundle_features(target_path: Path, bundle: ServiceBundle) -> None:
    for feature in bundle.scaffold.features:
        normalized = feature.strip().lower()
        if normalized == "vscode":
            _apply_vscode_configuration(target_path, False)
            continue
        if normalized == "devops":
            _configure_devops_from_bundle(target_path, bundle)
            continue
        raise click.ClickException(
            'Unknown bundle feature "{feature}". Supported Python bundle features are: devops, vscode.'.format(
                feature=feature
            )
        )


def _configure_devops_from_bundle(target_path: Path, bundle: ServiceBundle) -> None:
    provider = (bundle.repository.provider if bundle.repository else None) or "azure-devops"
    if provider != "azure-devops":
        raise click.ClickException(
            f'Bundle feature "devops" only supports repository.provider="azure-devops" in this MVP. Received "{provider}".'
        )

    organization = bundle.repository.organization if bundle.repository else None
    project = bundle.repository.project if bundle.repository else None
    if not organization or not project:
        raise click.ClickException(
            'Bundle feature "devops" requires repository.organization and repository.project for non-interactive scaffolding.'
        )

    result = _apply_devops_configuration(
        target_path,
        organization=organization,
        project=project,
        overwrite=False,
        ensure_remote=False,
    )
    _log_python_devops_result(result, include_remote_details=False)


def _write_python_bundle_artifacts(target_path: Path, bundle: ServiceBundle, raw_bundle: str) -> None:
    (target_path / "service.bundle.json").write_text(raw_bundle)
    (target_path / "SERVICE_SPEC.md").write_text(generate_service_spec_markdown(bundle))
    (target_path / "AGENTS.md").write_text(generate_agents_markdown(bundle))


def _print_python_create_success(target_path: Path, initialized_git: bool) -> None:
    click.echo(f"Created UNS Python app at {target_path}")
    click.echo("Next steps:")
    if target_path != Path.cwd():
        click.echo(f"  1) cd {target_path}")
        click.echo("  2) poetry install")
        click.echo("  3) poetry run python src/main.py")
        click.echo("  4) Copy a tracked config-development profile to config.json and set local secrets outside Git")
        if initialized_git:
            click.echo("  5) git status  # verify the new repository")
        return

    click.echo("  1) poetry install")
    click.echo("  2) poetry run python src/main.py")
    click.echo("  3) Copy a tracked config-development profile to config.json and set local secrets outside Git")
    if initialized_git:
        click.echo("  4) git status  # verify the new repository")


@cli.command("configure-devops", help="Add Azure DevOps settings and pipeline to a project.")
@click.argument("dest", required=False, default=".")
@click.option("--overwrite/--no-overwrite", default=False, show_default=True, help="Overwrite existing azure-pipelines.yml")
def configure_devops(dest: str, overwrite: bool):
    dest_path = Path(dest).resolve()
    _ensure_git_repo(dest_path)
    config_path = dest_path / "config.json"
    if not config_path.exists():
        click.echo(f"config.json not found at {config_path}, generating default.")
        _write_config_file(config_path)

    config = json.loads(config_path.read_text())
    org, project = _prompt_devops(config)
    result = _apply_devops_configuration(
        dest_path,
        organization=org,
        project=project,
        overwrite=overwrite,
        ensure_remote=True,
    )
    _log_python_devops_result(result, include_remote_details=True)


def _prompt_devops(config: dict) -> Tuple[str, str]:
    default_org = (config.get("devops") or {}).get("organization") or os.environ.get("AZURE_ORG") or ""
    default_proj = (config.get("devops") or {}).get("project") or os.environ.get("AZURE_PROJECT") or ""
    org = click.prompt("Azure DevOps organization", default=default_org or None, type=str)
    project = click.prompt("Azure DevOps project", default=default_proj or None, type=str)
    return org.strip(), project.strip()


def _apply_devops_configuration(
    dest_path: Path,
    *,
    organization: str,
    project: str,
    overwrite: bool,
    ensure_remote: bool,
) -> Dict[str, Any]:
    config_path = dest_path / "config.json"
    if not config_path.exists():
        _write_config_file(config_path)

    config = json.loads(config_path.read_text())
    if not isinstance(config, dict):
        raise click.ClickException(f"Expected a JSON object in {config_path}.")
    config.setdefault("devops", {})
    config["devops"]["provider"] = "azure-devops"
    config["devops"]["organization"] = organization
    config["devops"]["project"] = project
    config_path.write_text(json.dumps(config, indent=2) + "\n")

    git_remote_message = None
    if ensure_remote and not _git_has_remote(dest_path, "origin"):
        token = (os.environ.get("AZURE_PAT") or "").strip()
        if not token:
            token = click.prompt(
                f"Azure DevOps PAT (create one at https://dev.azure.com/{organization}/_usersSettings/tokens)",
                hide_input=True,
                type=str,
            ).strip()
        if len(token) < 10:
            raise click.ClickException("AZURE_PAT token looks too short.")

        repo_name = dest_path.name
        repo = _azure_ensure_repo(organization, project, repo_name, token)
        remote_url = repo.get("remoteUrl") or f"https://{organization}@dev.azure.com/{organization}/{project}/_git/{repo_name}"
        _git(dest_path, ["remote", "add", "origin", remote_url])
        git_remote_message = f"Added git remote origin -> {remote_url}"

    pkg_path = dest_path / "package.json"
    pkg_changed = False
    package_message = None
    if pkg_path.exists():
        try:
            pkg = json.loads(pkg_path.read_text())
            scripts = pkg.setdefault("scripts", {})
            if isinstance(scripts, dict):
                before = dict(scripts)
                scripts.setdefault("configure-devops", "poetry run uns-kit-py configure-devops")
                scripts.setdefault("configure-vscode", "poetry run uns-kit-py configure-vscode")
                scripts.setdefault("write-config", "poetry run uns-kit-py write-config")
                scripts.setdefault("pull-request", "poetry run uns-kit-py pull-request")
                pkg_changed = scripts != before
            pkg_path.write_text(json.dumps(pkg, indent=2) + "\n")
            package_message = f"Updated scripts in {pkg_path}"
        except Exception:
            package_message = None

    pipeline_template = importlib.resources.files("uns_kit").joinpath("templates/azure-pipelines.yml")
    pipeline_target = dest_path / "azure-pipelines.yml"
    if pipeline_target.exists() and not overwrite:
        pipeline_message = "azure-pipelines.yml already exists (skipped). Use --overwrite to replace."
    else:
        with pipeline_template.open("rb") as src_handle, pipeline_target.open("wb") as dst_handle:
            shutil.copyfileobj(src_handle, dst_handle)
        pipeline_message = f"Wrote {pipeline_target}"

    return {
        "organization": organization,
        "project": project,
        "package_message": package_message,
        "pkg_changed": pkg_changed,
        "pipeline_message": pipeline_message,
        "git_remote_message": git_remote_message,
        "config_message": f"Updated devops settings in {config_path}",
    }


def _log_python_devops_result(result: Dict[str, Any], *, include_remote_details: bool) -> None:
    click.echo(result["config_message"])
    if include_remote_details and result.get("git_remote_message"):
        click.echo(result["git_remote_message"])
    if result.get("package_message"):
        click.echo(result["package_message"])
    if result.get("pipeline_message"):
        click.echo(result["pipeline_message"])
    click.echo("DevOps configuration complete. Next steps:")
    click.echo("  1) Commit config.json and azure-pipelines.yml")
    if include_remote_details:
        click.echo("  2) Set AZURE_PAT in your CI or local env for pipeline access")


def _runtime_profile(project_name: str, profile: str) -> Dict[str, Any]:
    sanitized = TopicBuilder.sanitize_topic_part(project_name)
    is_host_development = profile == "development-host"
    is_production = profile == "production"
    return {
        "$schema": "./config.schema.json",
        "infra": {
            "host": "localhost" if is_host_development else "mosquitto",
            "port": 1883,
            "tls": False,
            "clientId": sanitized,
            "mqttSubToTopics": [],
            "keepalive": 60,
            "clean": True,
        },
        "uns": {
            "graphql": "http://localhost:3200/graphql",
            "rest": "http://localhost:3200/api",
            "processName": sanitized,
            "instanceMode": "wait",
            "handover": True,
            "jwksWellKnownUrl": "http://localhost:3200/api/.well-known/jwks.json",
            "kidWellKnownUrl": "http://localhost:3200/api/.well-known/kid",
            "env": "prod" if is_production else "dev",
            "supervisor": {
                "enabled": False,
                "restartOnExit": False,
                "maxMemoryMb": 512,
                "restartOnUnhealthy": False,
                "unhealthyAfterMs": 60000,
                "restartCooldownMs": 300000,
            },
        },
    }


def _write_config_file(path: Path, project_name: Optional[str] = None) -> None:
    project_name = project_name or path.resolve().parent.name
    path.write_text(json.dumps(_runtime_profile(project_name, "development-host"), indent=2) + "\n")


def _write_runtime_profiles(target_path: Path, project_name: str) -> None:
    profiles = {
        "config-development-host.json": "development-host",
        "config-development-podman.json": "development-podman",
        "config-production.json": "production",
    }
    for filename, profile in profiles.items():
        path = target_path / filename
        path.write_text(json.dumps(_runtime_profile(project_name, profile), indent=2) + "\n")


@cli.command("generate-config-schema", help="Generate config.schema.json from the core schema and project extension.")
@click.argument("dest", required=False, default=".")
def generate_config_schema_cmd(dest: str):
    dest_path = Path(dest).resolve()
    output_path = generate_config_schema(dest_path)
    click.echo(f"Generated {output_path}")


@cli.command("configure-api", help="Copy Python service API scaffold and add the uns-kit api extra.")
@click.argument("dest", required=False, default=".")
@click.option("--overwrite/--no-overwrite", default=False, show_default=True, help="Overwrite existing API example files")
def configure_api(dest: str, overwrite: bool):
    _configure_python_feature(
        Path(dest).resolve(),
        template_name="api",
        extras=["api"],
        label="UNS service API",
        success_message="API configuration complete.",
        overwrite=overwrite,
    )


@cli.command("configure-cron", help="Copy UNS cron examples and add the uns-kit cron extra.")
@click.argument("dest", required=False, default=".")
@click.option("--overwrite/--no-overwrite", default=False, show_default=True, help="Overwrite existing cron example files")
def configure_cron(dest: str, overwrite: bool):
    _configure_python_feature(
        Path(dest).resolve(),
        template_name="cron",
        extras=["cron"],
        label="UNS cron",
        success_message="Cron configuration complete.",
        overwrite=overwrite,
    )


@cli.command("configure-data-offer", help="Copy Python data-offer/service-api scaffold and add the uns-kit api extra.")
@click.argument("dest", required=False, default=".")
@click.option("--overwrite/--no-overwrite", default=False, show_default=True, help="Overwrite existing data-offer scaffold files")
def configure_data_offer(dest: str, overwrite: bool):
    _configure_python_feature(
        Path(dest).resolve(),
        template_name="data-offer",
        extras=["api"],
        label="UNS data-offer",
        success_message="Data-offer configuration complete.",
        overwrite=overwrite,
    )


@cli.command("configure-vscode", help="Add VS Code settings for Python development.")
@click.argument("dest", required=False, default=".")
@click.option("--overwrite/--no-overwrite", default=False, show_default=True, help="Overwrite existing .vscode files")
def configure_vscode(dest: str, overwrite: bool):
    _apply_vscode_configuration(Path(dest).resolve(), overwrite)


@cli.command("configure-workspace", help="Create a VS Code workspace file for the project.")
@click.argument("dest", required=False, default=".")
@click.option("--overwrite/--no-overwrite", default=False, show_default=True, help="Overwrite existing workspace file")
def configure_workspace(dest: str, overwrite: bool):
    dest_path = Path(dest).resolve()
    _write_workspace_file(dest_path, overwrite)


@cli.command("upgrade", help="Update an existing UNS Python project to current conventions.")
@click.argument("dest", required=False, default=".")
def upgrade_project(dest: str) -> None:
    dest_path = Path(dest).resolve()
    pkg_path = dest_path / "package.json"
    if not pkg_path.exists():
        raise click.ClickException(f"package.json not found at {pkg_path}")

    try:
        pkg_data = json.loads(pkg_path.read_text())
    except Exception as exc:
        raise click.ClickException(f"Could not read {pkg_path}: {exc}") from exc
    if not isinstance(pkg_data, dict):
        raise click.ClickException(f"Expected a JSON object in {pkg_path}.")

    changed = _ensure_uns_datahub_addon_metadata(pkg_data)
    if changed:
        pkg_path.write_text(json.dumps(pkg_data, indent=2) + "\n")
        click.echo("Added package.json unsDatahub add-on metadata.")
    else:
        click.echo("Preserved existing package.json unsDatahub metadata.")


def _ensure_uns_datahub_addon_metadata(pkg_data: Dict[str, Any]) -> bool:
    if "unsDatahub" in pkg_data:
        return False
    pkg_data["unsDatahub"] = dict(DEFAULT_UNS_DATAHUB_ADDON_METADATA)
    return True


def _configure_python_feature(
    dest_path: Path,
    *,
    template_name: str,
    extras: list[str],
    label: str,
    success_message: str,
    overwrite: bool,
) -> None:
    pyproject_path = dest_path / "pyproject.toml"
    if not pyproject_path.exists():
        raise click.ClickException(f"pyproject.toml not found at {pyproject_path}")

    template_root = importlib.resources.files("uns_kit").joinpath("templates", template_name)
    copied, overwritten, skipped = _copy_template_tree_to_path(template_root, dest_path, overwrite=overwrite)
    resolved_extras = _ensure_uns_kit_dependency_extras(pyproject_path, extras)

    click.echo(f"{label} assets processed.")
    if copied:
        click.echo("Added files:")
        for file in copied:
            click.echo(f"  {file}")
    if overwritten:
        click.echo("Overwritten files:")
        for file in overwritten:
            click.echo(f"  {file}")
    if skipped:
        click.echo("Skipped existing files:")
        for file in skipped:
            click.echo(f"  {file}")
    if not copied and not overwritten and not skipped:
        click.echo("No template files were copied.")

    click.echo(
        "Updated uns-kit dependency extras in pyproject.toml: {extras}".format(
            extras=", ".join(resolved_extras)
        )
    )
    click.echo(success_message)


def _apply_vscode_configuration(dest_path: Path, overwrite: bool) -> None:
    vscode_dir = dest_path / ".vscode"
    vscode_dir.mkdir(parents=True, exist_ok=True)

    template_root = importlib.resources.files("uns_kit").joinpath("templates/vscode")
    for filename in ("settings.json", "launch.json", "extensions.json"):
        src = template_root / filename
        dst = vscode_dir / filename
        if dst.exists() and not overwrite:
            click.echo(f"{dst} already exists (skipped). Use --overwrite to replace.")
            continue
        with src.open("rb") as src_handle, dst.open("wb") as dst_handle:
            shutil.copyfileobj(src_handle, dst_handle)
        click.echo(f"Wrote {dst}")

    _merge_vscode_schema_mapping(vscode_dir / "settings.json")
    _write_workspace_file(dest_path, overwrite)
    click.echo("VS Code configuration complete.")


def _merge_vscode_schema_mapping(settings_path: Path) -> None:
    settings: dict[str, Any] = {}
    if settings_path.exists():
        try:
            settings = json.loads(settings_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise click.ClickException(f"Expected valid JSON in {settings_path}: {exc}") from exc
        if not isinstance(settings, dict):
            raise click.ClickException(f"Expected a JSON object in {settings_path}.")

    schemas = settings.setdefault("json.schemas", [])
    if not isinstance(schemas, list):
        raise click.ClickException(f"Expected json.schemas to be an array in {settings_path}.")

    mapping = {
        "fileMatch": ["config.json", "config-*.json"],
        "url": "./config.schema.json",
    }
    if mapping not in schemas:
        schemas.append(mapping)
        settings_path.write_text(json.dumps(settings, indent=2) + "\n", encoding="utf-8")


def _write_workspace_file(dest_path: Path, overwrite: bool) -> None:
    project_name = dest_path.name
    workspace_file = dest_path / f"{project_name}.code-workspace"
    if workspace_file.exists() and not overwrite:
        click.echo(f"{workspace_file} already exists (skipped). Use --overwrite to replace.")
        return
    template = importlib.resources.files("uns_kit").joinpath("templates/workspace/workspace.json")
    shutil.copyfile(template, workspace_file)
    click.echo(f"Wrote {workspace_file}")


def _copy_template_tree_to_path(source_root: Any, destination_root: Path, *, overwrite: bool) -> tuple[list[str], list[str], list[str]]:
    copied: list[str] = []
    overwritten: list[str] = []
    skipped: list[str] = []

    def _copy_dir_with_tracking(current_source: Any, current_destination: Path) -> None:
        current_destination.mkdir(parents=True, exist_ok=True)
        for entry in current_source.iterdir():
            target = current_destination / entry.name
            if entry.is_dir():
                _copy_dir_with_tracking(entry, target)
                continue

            relative_name = str(target.relative_to(destination_root))
            target_exists = target.exists()
            if target_exists and not overwrite:
                skipped.append(relative_name)
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            with entry.open("rb") as src_handle, target.open("wb") as dst_handle:
                shutil.copyfileobj(src_handle, dst_handle)

            if target_exists:
                overwritten.append(relative_name)
            else:
                copied.append(relative_name)

    _copy_dir_with_tracking(source_root, destination_root)
    return copied, overwritten, skipped


def _ensure_uns_kit_dependency_extras(pyproject_path: Path, extras: Iterable[str]) -> list[str]:
    requested = [extra.strip() for extra in extras if extra and extra.strip()]
    if not requested:
        return []

    lines = pyproject_path.read_text().splitlines()
    out: list[str] = []
    in_dependencies = False
    changed = False
    resolved_extras: list[str] = []

    for line in lines:
        stripped = line.strip()
        if stripped == "[tool.poetry.dependencies]":
            in_dependencies = True
            out.append(line)
            continue
        if in_dependencies and stripped.startswith("[") and stripped.endswith("]"):
            in_dependencies = False

        if in_dependencies and re.match(r"^uns-kit\s*=", stripped):
            updated_line, resolved_extras = _merge_uns_kit_dependency_extras(line, requested)
            out.append(updated_line)
            changed = changed or updated_line != line
            continue

        out.append(line)

    if not resolved_extras:
        raise click.ClickException(f"Could not find uns-kit dependency in {pyproject_path}")

    if changed:
        pyproject_path.write_text("\n".join(out) + "\n")

    return resolved_extras


def _merge_uns_kit_dependency_extras(line: str, requested_extras: list[str]) -> tuple[str, list[str]]:
    indent_match = re.match(r"^(\s*)", line)
    indent = indent_match.group(1) if indent_match else ""
    current = line.strip()
    _, _, rhs = current.partition("=")
    dependency_spec = rhs.strip()

    existing_extras = _parse_poetry_inline_extras(dependency_spec)
    resolved_extras = _merge_extra_names(existing_extras, requested_extras)

    if dependency_spec.startswith("{") and dependency_spec.endswith("}"):
        path_match = re.search(r'path\s*=\s*"([^"]+)"', dependency_spec)
        develop_match = re.search(r"develop\s*=\s*(true|false)", dependency_spec)
        version_match = re.search(r'version\s*=\s*"([^"]+)"', dependency_spec)

        if path_match:
            parts = [f'path = "{path_match.group(1)}"']
            if develop_match:
                parts.append(f"develop = {develop_match.group(1)}")
            parts.append(_format_poetry_extras(resolved_extras))
            return f'{indent}uns-kit = {{ {", ".join(parts)} }}', resolved_extras

        version = version_match.group(1) if version_match else "*"
        return (
            f'{indent}uns-kit = {{ version = "{version}", {_format_poetry_extras(resolved_extras)} }}',
            resolved_extras,
        )

    version_match = re.match(r'"([^"]+)"', dependency_spec)
    version = version_match.group(1) if version_match else "*"
    return (
        f'{indent}uns-kit = {{ version = "{version}", {_format_poetry_extras(resolved_extras)} }}',
        resolved_extras,
    )


def _parse_poetry_inline_extras(spec: str) -> list[str]:
    extras_match = re.search(r"extras\s*=\s*\[([^\]]*)\]", spec)
    if not extras_match:
        return []

    values = re.findall(r'"([^"]+)"', extras_match.group(1))
    return _merge_extra_names(values, [])


def _merge_extra_names(existing: Iterable[str], requested: Iterable[str]) -> list[str]:
    merged: list[str] = []
    for value in [*existing, *requested]:
        normalized = value.strip()
        if normalized and normalized not in merged:
            merged.append(normalized)
    return merged


def _format_poetry_extras(extras: Iterable[str]) -> str:
    rendered = ", ".join(f'"{extra}"' for extra in extras)
    return f"extras = [{rendered}]"

@cli.command("pull-request", help="Create an Azure DevOps pull request for a Python project (bumps version, commits, pushes, opens PR).")
@click.argument("dest", required=False, default=".")
@click.option("--target-branch", default=None, help="Override PR target branch (e.g. main). Defaults to Azure repo default branch.")
def pull_request(dest: str, target_branch: Optional[str]):
    dest_path = Path(dest).resolve()

    if not _git_has_remote(dest_path, "origin"):
        raise click.ClickException("Git remote origin is not configured. Run `uns-kit-py configure-devops` first.")

    # Require a git repo and clean tree (mirrors TS behavior).
    _assert_git_clean(dest_path)
    current_branch = _git_output(dest_path, ["rev-parse", "--abbrev-ref", "HEAD"]).strip()
    if not current_branch or current_branch == "HEAD":
        raise click.ClickException("Could not determine current branch. Ensure you are on a branch (not detached HEAD).")
    base_commit = _git_output(dest_path, ["rev-parse", "HEAD"]).strip()
    if not base_commit:
        raise click.ClickException("Could not determine current commit. Ensure the repository has at least one commit.")

    config = _load_json(dest_path / "config.json")
    devops = (config.get("devops") if isinstance(config, dict) else {}) or {}
    org = (devops.get("organization") or "").strip()
    project = (devops.get("project") or "").strip()
    if not org or not project:
        raise click.ClickException("Missing devops.organization/devops.project in config.json. Run `uns-kit-py configure-devops` first.")

    token = (os.environ.get("AZURE_PAT") or "").strip()
    if not token:
        token = click.prompt(
            f"Azure DevOps PAT (create one at https://dev.azure.com/{org}/_usersSettings/tokens)",
            hide_input=True,
            type=str,
        ).strip()
    if len(token) < 10:
        raise click.ClickException("AZURE_PAT token looks too short.")

    repo_name = _infer_repo_name(dest_path)
    repo = _azure_get_repo(org, project, repo_name, token)
    repo_id = str(repo.get("id") or "").strip()
    if not repo_id:
        raise click.ClickException("Azure repository id missing; cannot create PR.")

    repo_default_branch_ref = (repo.get("defaultBranch") or "").strip()
    repo_default_branch_name = repo_default_branch_ref.replace("refs/heads/", "") if repo_default_branch_ref else ""
    if repo_default_branch_name and current_branch == repo_default_branch_name:
        raise click.ClickException(f"Refusing to create PR from default branch ({repo_default_branch_name}). Create a feature branch first.")

    explicit_target = bool(target_branch and target_branch.strip())
    if target_branch:
        target_ref = f"refs/heads/{target_branch.strip()}"
    else:
        target_ref = repo_default_branch_ref or "refs/heads/master"

    branches = _azure_list_refs(org, project, repo_id, token, "heads/")
    # If the default branch is missing on the remote, prefer an existing main/master, otherwise create it.
    if target_ref not in branches:
        if not explicit_target:
            if "refs/heads/main" in branches:
                target_ref = "refs/heads/main"
            elif "refs/heads/master" in branches:
                target_ref = "refs/heads/master"

    if target_ref not in branches:
        # New repo edge-case: Azure may have defaultBranch=master but no master branch exists yet because
        # the first push was a feature branch. Create the target branch at the current HEAD (before bumping
        # version) so the PR has a meaningful diff.
        branch_name = target_ref.replace("refs/heads/", "")
        click.echo(f"Remote target branch {target_ref} does not exist; creating/pushing it...")
        existing = _git_output(dest_path, ["branch", "--list", branch_name]).strip()
        if not existing:
            _git(dest_path, ["branch", branch_name, base_commit])
        _git(dest_path, ["push", "-u", "origin", branch_name], token=token)
        branches = _azure_list_refs(org, project, repo_id, token, "heads/")
        if target_ref not in branches:
            raise click.ClickException(f"Failed to create remote branch {target_ref}.")

    target_branch_name = target_ref.replace("refs/heads/", "")
    if target_branch_name and current_branch == target_branch_name:
        raise click.ClickException(f"Refusing to create PR from target branch ({target_branch_name}). Create a feature branch first.")

    pyproject_path = dest_path / "pyproject.toml"
    current_version = _read_poetry_version(pyproject_path)
    new_version = click.prompt("Version tag for this PR", default=current_version, type=str).strip()
    if not new_version:
        raise click.ClickException("Version is required.")
    _assert_version_tag_unique(org, project, repo_id, token, new_version)

    _write_poetry_version(pyproject_path, new_version)
    pkg_path = dest_path / "package.json"
    if pkg_path.exists():
        _write_package_json_version(pkg_path, new_version)
        _git(dest_path, ["add", "pyproject.toml", "package.json"])
    else:
        _git(dest_path, ["add", "pyproject.toml"])
    _git(dest_path, ["commit", "-m", f"Set new production version: {new_version}"])
    _git(dest_path, ["push", "origin", current_branch], token=token)

    title = click.prompt("Title for the pull request", type=str).strip()
    description = click.prompt("Description for the pull request", default="", type=str)

    pr = _azure_create_pr(
        org=org,
        project=project,
        repo_id=repo_id,
        token=token,
        source_branch=current_branch,
        target_ref=target_ref,
        title=title,
        description=description,
        label=new_version,
    )
    pr_id = pr.get("pullRequestId")
    pr_url = f"https://dev.azure.com/{org}/{project}/_git/{repo_name}/pullrequest/{pr_id}" if pr_id else pr.get("url")
    click.echo(f"Pull request created: {pr_url}")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise click.ClickException(f"Missing {path}.")
    try:
        data = json.loads(path.read_text())
        if not isinstance(data, dict):
            raise ValueError("not a JSON object")
        return data
    except Exception as exc:
        raise click.ClickException(f"Failed to read {path}: {exc}")

def _init_git_repository(target_dir: Path) -> bool:
    """
    Initialize a git repository in target_dir if it's not already inside a work tree.
    Mirrors the TS CLI behavior: don't create nested repos when inside an existing git repo.
    """
    try:
        inside = _git_output(target_dir, ["rev-parse", "--is-inside-work-tree"]).strip()
        if inside == "true":
            return False
    except click.ClickException as exc:
        # If git is available but target_dir is not a repo, rev-parse prints "not a git repository".
        # In that case we proceed with git init. For other errors, fail soft.
        msg = str(exc)
        if "not a git repository" not in msg.lower():
            return False

    try:
        _git_output(target_dir, ["init"])
        click.echo("Initialized empty Git repository.")
        return True
    except click.ClickException:
        return False


def _git_auth_header(token: str) -> str:
    encoded = base64.b64encode(f":{token}".encode("utf-8")).decode("ascii")
    return f"Authorization: Basic {encoded}"


def _git(cwd: Path, args: list[str], *, token: Optional[str] = None) -> None:
    _git_output(cwd, args, token=token)


def _git_output(cwd: Path, args: list[str], *, token: Optional[str] = None) -> str:
    try:
        env = os.environ.copy()
        env["GIT_TERMINAL_PROMPT"] = env.get("GIT_TERMINAL_PROMPT", "0")
        cmd: list[str] = ["git"]
        if token:
            cmd.extend(["-c", f"http.extraheader={_git_auth_header(token)}"])
        cmd.extend(args)
        out = subprocess.check_output(cmd, cwd=str(cwd), stderr=subprocess.STDOUT, env=env)
        return out.decode("utf-8", errors="replace")
    except FileNotFoundError:
        raise click.ClickException("Git is not installed or not available on PATH.")
    except subprocess.CalledProcessError as exc:
        message = exc.output.decode("utf-8", errors="replace") if exc.output else str(exc)
        raise click.ClickException(f"Git command failed in {cwd}: {message.strip()}")


def _assert_git_clean(cwd: Path) -> None:
    status = _git_output(cwd, ["status", "--porcelain"]).strip()
    if status:
        raise click.ClickException("Repository needs to be clean. Please commit or stash changes before creating a PR.")


def _infer_repo_name(cwd: Path) -> str:
    # Prefer parsing origin remote; fallback to folder name.
    try:
        remote = _git_output(cwd, ["remote", "get-url", "origin"]).strip()
    except Exception:
        remote = ""

    if remote:
        # https://dev.azure.com/org/project/_git/repo
        m = re.search(r"/_git/([^/]+?)(?:\.git)?$", remote)
        if m:
            return m.group(1)
        # git@ssh.dev.azure.com:v3/org/project/repo
        m = re.search(r":v3/[^/]+/[^/]+/([^/]+?)(?:\.git)?$", remote)
        if m:
            return m.group(1)
        # fallback last path segment
        tail = remote.rstrip("/").split("/")[-1]
        if tail:
            return tail.replace(".git", "")

    return cwd.name


def _read_poetry_version(pyproject_path: Path) -> str:
    text = pyproject_path.read_text()
    in_section = False
    for line in text.splitlines():
        if line.strip() == "[tool.poetry]":
            in_section = True
            continue
        if in_section and line.startswith("[") and line.strip().endswith("]"):
            break
        if in_section:
            m = re.match(r'^version\s*=\s*"([^"]+)"', line.strip())
            if m:
                return m.group(1)
    raise click.ClickException(f"Could not find tool.poetry version in {pyproject_path}")


def _write_poetry_version(pyproject_path: Path, new_version: str) -> None:
    lines = pyproject_path.read_text().splitlines()
    out: list[str] = []
    in_section = False
    changed = False
    for line in lines:
        if line.strip() == "[tool.poetry]":
            in_section = True
            out.append(line)
            continue
        if in_section and line.startswith("[") and line.strip().endswith("]"):
            in_section = False
        if in_section and not changed and re.match(r"^version\s*=", line.strip()):
            out.append(f'version = "{new_version}"')
            changed = True
        else:
            out.append(line)
    if not changed:
        raise click.ClickException(f"Could not update version in {pyproject_path} (tool.poetry section missing?)")
    pyproject_path.write_text("\n".join(out) + "\n")


def _write_package_json_version(package_json_path: Path, new_version: str) -> None:
    try:
        data = json.loads(package_json_path.read_text())
    except Exception as exc:
        raise click.ClickException(f"Could not read {package_json_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise click.ClickException(f"Expected a JSON object in {package_json_path}.")

    data["version"] = new_version
    package_json_path.write_text(json.dumps(data, indent=2) + "\n")


def _assert_version_tag_unique(org: str, project: str, repo_id: str, token: str, version: str) -> None:
    tags = _azure_list_refs(org, project, repo_id, token, "tags/")
    if f"refs/tags/{version}" in tags:
        raise click.ClickException(f"Version tag {version} already exists on the remote. Choose another version.")


def _azure_list_refs(org: str, project: str, repo_id: str, token: str, ref_filter: str) -> list[str]:
    filter_q = urllib.parse.quote(ref_filter, safe="/")
    url = f"https://dev.azure.com/{org}/{project}/_apis/git/repositories/{repo_id}/refs?filter={filter_q}&api-version=7.1-preview.1"
    data = _azure_request("GET", url, token)
    value = data.get("value", [])
    refs: list[str] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                name = item.get("name")
                if isinstance(name, str):
                    refs.append(name)
    return refs


def _azure_request(method: str, url: str, token: str, data: Optional[dict] = None) -> dict:
    raw = None if data is None else json.dumps(data).encode("utf-8")
    # Azure DevOps PAT auth is HTTP Basic with PAT as the password.
    # Some environments behave better with a non-empty username.
    auth = base64.b64encode(f"pat:{token}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(url, data=raw, method=method)
    req.add_header("Authorization", f"Basic {auth}")
    req.add_header("Accept", "application/json")
    if raw is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            body_bytes = resp.read()
            body = body_bytes.decode("utf-8", errors="replace")
            if not body:
                return {}
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                content_type = resp.headers.get("Content-Type", "")
                snippet = body[:500]
                raise click.ClickException(
                    "Azure DevOps API returned a non-JSON response "
                    f"(status {getattr(resp, 'status', '?')}, content-type {content_type}). "
                    "This usually means the PAT is invalid/expired or lacks permissions.\n"
                    f"URL: {url}\n"
                    f"Response (first 500 chars): {snippet}",
                )
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise click.ClickException(f"Azure DevOps API error {e.code}: {body}")
    except urllib.error.URLError as e:
        raise click.ClickException(f"Azure DevOps API request failed: {e.reason}")


def _azure_get_repo(org: str, project: str, repo_name: str, token: str) -> dict:
    url = f"https://dev.azure.com/{org}/{project}/_apis/git/repositories/{repo_name}?api-version=7.1-preview.1"
    return _azure_request("GET", url, token)


def _azure_ensure_repo(org: str, project: str, repo_name: str, token: str) -> dict:
    try:
        return _azure_get_repo(org, project, repo_name, token)
    except click.ClickException:
        # create if not found
        url = f"https://dev.azure.com/{org}/{project}/_apis/git/repositories?api-version=7.1-preview.1"
        payload = {"name": repo_name}
        return _azure_request("POST", url, token, payload)


def _azure_create_pr(
    *,
    org: str,
    project: str,
    repo_id: str,
    token: str,
    source_branch: str,
    target_ref: str,
    title: str,
    description: str,
    label: str,
) -> dict:
    if not repo_id:
        raise click.ClickException("Azure repository id missing; cannot create PR.")
    url = f"https://dev.azure.com/{org}/{project}/_apis/git/repositories/{repo_id}/pullrequests?api-version=7.1-preview.1"
    payload = {
        "sourceRefName": f"refs/heads/{source_branch}",
        "targetRefName": target_ref,
        "title": title,
        "description": description,
        "labels": [{"name": label}],
    }
    return _azure_request("POST", url, token, payload)


def _ensure_git_repo(dest_path: Path) -> None:
    _init_git_repository(dest_path)


def _git_has_remote(dest_path: Path, name: str) -> bool:
    try:
        _git_output(dest_path, ["remote", "get-url", name])
        return True
    except click.ClickException:
        return False


def _to_distribution_name(name: str) -> str:
    # PyPI distribution name: keep it readable and normalized.
    value = re.sub(r"[^a-zA-Z0-9_-]+", "-", name).strip("-").lower()
    value = re.sub(r"-{2,}", "-", value)
    return value or "uns-py-app"


def _personalize_pyproject(pyproject_path: Path, project_name: str) -> None:
    dist_name = _to_distribution_name(project_name)

    text = pyproject_path.read_text()
    text = re.sub(r'(?m)^name\s*=\s*"[^"]+"\s*$', f'name = "{dist_name}"', text)
    pyproject_path.write_text(text)


def _configure_local_uns_kit_dependency(pyproject_path: Path) -> None:
    """
    For monorepo sandbox apps, prefer a local editable dependency on packages/uns-py.
    Falls back to the template default (`uns-kit = "*"`) when the local package path
    is not available.
    """
    local_uns_py = (pyproject_path.parent / "../packages/uns-py").resolve()
    if not local_uns_py.exists():
        return

    text = pyproject_path.read_text()
    text = re.sub(
        r'(?m)^uns-kit\s*=\s*".*"\s*$',
        'uns-kit = { path = "../packages/uns-py", develop = true }',
        text,
    )
    pyproject_path.write_text(text)


def render_cli_help(prog_name: str = CLI_PROG_NAME) -> str:
    return (
        f"\n{prog_name} v{__version__}\n"
        "\n"
        f"Usage: {prog_name} <command> [options]\n"
        "\n"
        "Commands:\n"
        "  create <name>           Scaffold a new UNS Python application\n"
        "  create --bundle <path>  Scaffold a new UNS Python application from service.bundle.json\n"
        "  configure-api [dir]     Copy service API scaffold and add the api extra\n"
        "  configure-cron [dir]    Copy UNS cron examples and add the cron extra\n"
        "  configure-data-offer [dir] Copy service API + data-offer scaffold and add the api extra\n"
        "  configure-devops [dir]  Configure Azure DevOps tooling in an existing project\n"
        "  configure-vscode [dir]  Add VS Code workspace configuration files\n"
        "  configure-workspace [dir] Create a VS Code workspace file\n"
        "  upgrade [dir]           Update an existing project to current conventions\n"
        "  publish                 Publish a UNS data packet to a topic\n"
        "  subscribe               Subscribe to one or more topics (resilient)\n"
        "  pull-request [dir]      Create an Azure DevOps pull request for a Python project\n"
        "  help                    Show this message\n"
    )


def main(argv: Optional[list[str]] = None):
    args = list(argv if argv is not None else sys.argv[1:])

    if not args or args == ["help"] or args == ["--help"] or args == ["-h"]:
        click.echo(render_cli_help())
        return

    if args == ["--version"] or args == ["-V"]:
        click.echo(f"{CLI_PROG_NAME} v{__version__}")
        return

    if args and args[0] == "help":
        if len(args) == 1:
            click.echo(render_cli_help())
            return
        cli.main(args=[*args[1:], "--help"], prog_name=CLI_PROG_NAME, standalone_mode=True)
        return

    cli.main(args=args, prog_name=CLI_PROG_NAME, standalone_mode=True)


if __name__ == "__main__":
    main()
