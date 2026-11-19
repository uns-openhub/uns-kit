from __future__ import annotations

import json

import pytest

from uns_kit import load_background_service_access_declaration, parse_background_service_access_declaration


def declaration(rules=None):
    return {"protocol": 1, "permissions": {"scopes": ["read:uns"], "accessRules": rules or ["example/site/#"]}}


def test_background_parser_normalizes_without_granting_other_protocols() -> None:
    value = declaration(["warehouse/pumps/+", "example/site/#", "example/site/#"])
    assert parse_background_service_access_declaration(value)["permissions"]["accessRules"] == ["example/site/#", "warehouse/pumps/+"]
    assert len(value["permissions"]["accessRules"]) == 3
    for value in (
        {**declaration(), "protocol": True},
        {"protocol": 2, "requirements": []},
        {**declaration(), "publications": {}},
        {"protocol": 1, "permissions": {"scopes": ["admin"], "accessRules": ["#"]}},
        {"protocol": 1, "permissions": {"scopes": ["read:uns"], "accessRules": []}},
    ):
        with pytest.raises(ValueError):
            parse_background_service_access_declaration(value)


@pytest.mark.parametrize("rule", ["example/#/suffix", "example//path", "/root", "example/bad+", "", "x" * 513])
def test_invalid_filters_are_rejected(rule) -> None:
    with pytest.raises(ValueError):
        parse_background_service_access_declaration(declaration([rule]))


def test_package_loader_reads_inline_and_file_without_running_application_code(tmp_path) -> None:
    package = tmp_path / "package.json"
    package.write_text(json.dumps({"unsDatahub": {"serviceAccess": declaration()}}))
    assert load_background_service_access_declaration(package) == declaration()
    (tmp_path / "requirements.json").write_text(json.dumps(declaration()))
    package.write_text(json.dumps({"unsDatahub": {"serviceAccess": "requirements.json"}}))
    assert load_background_service_access_declaration(package) == declaration()
    (tmp_path / "main.py").write_text("raise AssertionError('application code must not run')")


@pytest.mark.parametrize("reference", ["../outside.json", "/outside.json", "C:/outside.json", "a\\b.json", "a//b.json", "./requirements.json"])
def test_package_loader_rejects_traversal_and_unsafe_references(tmp_path, reference) -> None:
    package = tmp_path / "package.json"
    package.write_text(json.dumps({"unsDatahub": {"serviceAccess": reference}}))
    with pytest.raises(ValueError):
        load_background_service_access_declaration(package)


def test_package_loader_rejects_linked_and_oversized_files(tmp_path) -> None:
    package = tmp_path / "package.json"
    (tmp_path / "actual.json").write_text(json.dumps(declaration()))
    (tmp_path / "linked.json").symlink_to(tmp_path / "actual.json")
    package.write_text(json.dumps({"unsDatahub": {"serviceAccess": "linked.json"}}))
    with pytest.raises(OSError):
        load_background_service_access_declaration(package)
    (tmp_path / "large.json").write_text(" " * 65537)
    package.write_text(json.dumps({"unsDatahub": {"serviceAccess": "large.json"}}))
    with pytest.raises(ValueError, match="file_invalid"):
        load_background_service_access_declaration(package)
