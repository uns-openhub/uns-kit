from __future__ import annotations

import json
from pathlib import Path
import shutil

from click.testing import CliRunner

from uns_kit.cli import cli


def test_create_from_valid_python_bundle(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    bundle_path = _write_bundle(
        tmp_path / "service.bundle.json",
        scaffold={"stack": "python", "template": "default", "features": ["vscode", "devops"]},
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["create", "--bundle", str(bundle_path)])

    assert result.exit_code == 0, result.output
    target = tmp_path / "uns-example-service"
    assert (target / "service.bundle.json").exists()
    assert (target / "SERVICE_SPEC.md").exists()
    assert (target / "AGENTS.md").exists()
    assert (target / ".vscode" / "settings.json").exists()
    assert (target / "azure-pipelines.yml").exists()
    assert (target / "config.schema.json").exists()
    assert (target / "src" / "config" / "project_config_extension.py").exists()

    assert (target / "service.bundle.json").read_text() == bundle_path.read_text()
    assert "UNS Example Service" in (target / "SERVICE_SPEC.md").read_text()
    agents_text = (target / "AGENTS.md").read_text()
    assert "bootstrapped from `service.bundle.json`" in agents_text
    assert "pnpm build" in agents_text
    assert "For UNS topic outputs, use `UnsProxyProcess` with `create_mqtt_proxy`." in agents_text

    config = json.loads((target / "config.json").read_text())
    assert config["devops"]["provider"] == "azure-devops"
    assert config["devops"]["organization"] == "sijit"
    assert config["devops"]["project"] == "industry40"
    assert config["uns"]["supervisor"]["enabled"] is False
    assert config["uns"]["supervisor"]["maxMemoryMb"] == 512
    for filename, expected_host, expected_env in (
        ("config-development-host.json", "localhost", "dev"),
        ("config-development-podman.json", "mosquitto", "dev"),
        ("config-production.json", "mosquitto", "prod"),
    ):
        profile = json.loads((target / filename).read_text())
        assert profile["infra"]["host"] == expected_host
        assert profile["uns"]["processName"] == "uns-example-service"
        assert profile["uns"]["env"] == expected_env
        assert "email" not in profile["uns"]
        assert "password" not in profile["uns"]
        assert "input" not in profile
        assert "output" not in profile
    package_json = json.loads((target / "package.json").read_text())
    assert package_json["unsDatahub"] == {
        "schemaVersion": 1,
        "kind": "addon",
        "controllerCompatibility": ">=2 <3",
    }


def test_create_rejects_invalid_bundle(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    bundle_path = _write_bundle(tmp_path / "service.bundle.json", schemaVersion=2)

    result = CliRunner().invoke(cli, ["create", "--bundle", str(bundle_path)])

    assert result.exit_code != 0
    assert "schemaVersion must be 1" in result.output


def test_create_rejects_wrong_stack_for_python_cli(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    bundle_path = _write_bundle(
        tmp_path / "service.bundle.json",
        scaffold={"stack": "ts", "template": "default", "features": []},
    )

    result = CliRunner().invoke(cli, ["create", "--bundle", str(bundle_path)])

    assert result.exit_code != 0
    assert 'Bundle scaffold.stack is "ts"' in result.output
    assert "uns-kit create --bundle <path>" in result.output


def test_create_supports_dest_for_bundle(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    bundle_path = _write_bundle(
        tmp_path / "service.bundle.json",
        scaffold={"stack": "python", "template": "default", "features": []},
    )

    target = tmp_path / "custom-target"
    result = CliRunner().invoke(cli, ["create", "--bundle", str(bundle_path), "--dest", str(target)])

    assert result.exit_code == 0, result.output
    assert (target / "service.bundle.json").exists()
    assert (target / "pyproject.toml").exists()


def test_create_requires_allow_existing_for_non_empty_destinations(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    bundle_path = _write_bundle(
        tmp_path / "service.bundle.json",
        scaffold={"stack": "python", "template": "default", "features": []},
    )

    target = tmp_path / "existing-target"
    target.mkdir(parents=True)
    (target / "keep.txt").write_text("keep\n")

    result = CliRunner().invoke(cli, ["create", "--bundle", str(bundle_path), "--dest", str(target)])

    assert result.exit_code != 0
    assert "Destination is not empty" in result.output


def test_create_allows_existing_when_flag_is_passed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    bundle_path = _write_bundle(
        tmp_path / "service.bundle.json",
        scaffold={"stack": "python", "template": "default", "features": []},
    )

    target = tmp_path / "existing-target"
    target.mkdir(parents=True)
    (target / "keep.txt").write_text("keep\n")

    result = CliRunner().invoke(
        cli,
        ["create", "--bundle", str(bundle_path), "--dest", str(target), "--allow-existing"],
    )

    assert result.exit_code == 0, result.output
    assert (target / "keep.txt").read_text() == "keep\n"
    assert (target / "service.bundle.json").exists()


def test_legacy_create_name_still_works(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(cli, ["create", "legacy-app"])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "legacy-app" / "pyproject.toml").exists()
    assert (tmp_path / "legacy-app" / "config.schema.json").exists()
    assert not (tmp_path / "legacy-app" / "service.bundle.json").exists()
    package_json = json.loads((tmp_path / "legacy-app" / "package.json").read_text())
    assert package_json["unsDatahub"] == {
        "schemaVersion": 1,
        "kind": "addon",
        "controllerCompatibility": ">=2 <3",
    }


def test_upgrade_adds_addon_metadata_and_preserves_it(tmp_path: Path) -> None:
    target = tmp_path / "existing-app"
    target.mkdir()
    package_path = target / "package.json"
    package_path.write_text(json.dumps({"name": "existing-app", "version": "1.2.3"}, indent=2) + "\n")

    runner = CliRunner()
    first = runner.invoke(cli, ["upgrade", str(target)])
    assert first.exit_code == 0, first.output
    first_package = json.loads(package_path.read_text())
    assert first_package["unsDatahub"] == {
        "schemaVersion": 1,
        "kind": "addon",
        "controllerCompatibility": ">=2 <3",
    }

    first_package["unsDatahub"] = {"schemaVersion": 2, "kind": "custom"}
    package_path.write_text(json.dumps(first_package, indent=2) + "\n")
    second = runner.invoke(cli, ["upgrade", str(target)])
    assert second.exit_code == 0, second.output
    assert json.loads(package_path.read_text())["unsDatahub"] == {"schemaVersion": 2, "kind": "custom"}


def test_generate_config_schema_merges_project_extension(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(cli, ["create", "legacy-app"])
    assert result.exit_code == 0, result.output

    extension_path = tmp_path / "legacy-app" / "src" / "config" / "project_config_extension.py"
    extension_path.write_text(
        "\n".join(
            [
                "from uns_kit.core import strict_object, string_schema",
                "",
                "project_extras_schema = strict_object(",
                "    {",
                '        "service": strict_object(',
                '            {"mode": string_schema(enum=["demo", "prod"])},',
                '            required=["mode"],',
                "        )",
                "    },",
                '    required=["service"],',
                ")",
                "",
            ]
        ),
        encoding="utf-8",
    )

    generate_result = CliRunner().invoke(cli, ["generate-config-schema", str(tmp_path / "legacy-app")])
    assert generate_result.exit_code == 0, generate_result.output

    schema = json.loads((tmp_path / "legacy-app" / "config.schema.json").read_text())
    assert "service" in schema["properties"]
    assert "service" in schema["required"]
    assert schema["properties"]["service"]["required"] == ["mode"]
    assert "supervisor" in schema["properties"]["uns"]["properties"]


def test_configure_vscode_adds_json_schema_mapping(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(cli, ["create", "legacy-app"])
    assert result.exit_code == 0, result.output

    vscode_dir = tmp_path / "legacy-app" / ".vscode"
    if vscode_dir.exists():
        shutil.rmtree(vscode_dir)

    vscode_result = CliRunner().invoke(cli, ["configure-vscode", str(tmp_path / "legacy-app")])
    assert vscode_result.exit_code == 0, vscode_result.output

    settings = json.loads((vscode_dir / "settings.json").read_text())
    assert {"fileMatch": ["config.json", "config-*.json"], "url": "./config.schema.json"} in settings["json.schemas"]


def test_legacy_create_uses_proxy_based_uns_publishing_pattern(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(cli, ["create", "legacy-app"])

    assert result.exit_code == 0, result.output
    main_text = (tmp_path / "legacy-app" / "src" / "main.py").read_text()
    assert "UnsProxyProcess" in main_text
    assert "create_mqtt_proxy" in main_text
    assert "publish_mqtt_message" in main_text
    assert "UnsMqttClient" not in main_text
    assert "publish_packet(\"raw/data/\"" not in main_text


def _write_bundle(path: Path, **overrides):
    bundle = {
        "schemaVersion": 1,
        "kind": "uns-service-bundle",
        "metadata": {
            "name": "uns-example-service",
            "displayName": "UNS Example Service",
            "serviceType": "microservice",
            "summary": "Short summary",
            "description": "Longer description",
            "owner": "team-name",
            "tags": ["tag1", "tag2"],
        },
        "scaffold": {
            "stack": "python",
            "template": "default",
            "features": ["vscode"],
        },
        "repository": {
            "provider": "azure-devops",
            "organization": "sijit",
            "project": "industry40",
            "repository": "uns-example-service",
            "defaultBranch": "master",
        },
        "domain": {
            "inputs": [],
            "outputs": [],
        },
        "docs": {
            "serviceSpec": {
                "goals": ["Goal 1"],
                "nonGoals": ["Non-goal 1"],
                "acceptanceCriteria": ["Criterion 1"],
            },
            "agents": {
                "projectContext": ["Context 1"],
                "guardrails": ["Guardrail 1"],
                "firstTasks": ["Task 1"],
                "verification": ["pnpm build"],
            },
        },
        "analytics": None,
        "provenance": {"origin": "manual"},
    }
    bundle.update(overrides)
    path.write_text(json.dumps(bundle, indent=2) + "\n")
    return path
