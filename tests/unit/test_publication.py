import errno
import os
from pathlib import Path
from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

from etl_pipeline import cli
from etl_pipeline.exceptions import ETLError
from etl_pipeline.publication import preflight_output, publish_candidate


def test_preflight_consent_and_aliases(tmp_path: Path) -> None:
    source = tmp_path / "input.txt"
    source.write_bytes(b"source")
    output = tmp_path / "output.duckdb"
    preflight_output(source, output, replace=False)
    output.write_bytes(b"old")
    with pytest.raises(ETLError, match="--replace"):
        preflight_output(source, output, replace=False)
    preflight_output(source, output, replace=True)
    assert output.read_bytes() == b"old"
    with pytest.raises(ETLError, match="same file"):
        preflight_output(source, source, replace=True)
    alias = tmp_path / "alias"
    os.link(source, alias)
    with pytest.raises(ETLError, match="same file"):
        preflight_output(source, alias, replace=True)
    assert source.read_bytes() == alias.read_bytes() == b"source"


@pytest.mark.parametrize("kind", ["symlink", "dangling", "directory", "missing_parent"])
def test_unsuitable_destinations(tmp_path: Path, kind: str) -> None:
    source = tmp_path / "input.txt"
    source.write_bytes(b"source")
    output = tmp_path / "output.duckdb"
    if kind == "symlink":
        output.symlink_to(source)
    elif kind == "dangling":
        output.symlink_to(tmp_path / "missing")
    elif kind == "directory":
        output.mkdir()
    else:
        output = tmp_path / "missing" / "output.duckdb"
    with pytest.raises(ETLError, match="regular file|parent directory"):
        preflight_output(source, output, replace=True)
    assert source.read_bytes() == b"source"
    if kind in {"symlink", "dangling"}:
        assert output.is_symlink()
    elif kind == "directory":
        assert output.is_dir()
    else:
        assert not output.parent.exists()


@pytest.mark.parametrize("existing_output", [False, True])
def test_destination_wal_blocks_preflight_and_publication(
    tmp_path: Path, existing_output: bool
) -> None:
    source = tmp_path / "input.txt"
    source.write_bytes(b"source")
    output = tmp_path / "output.duckdb"
    candidate = tmp_path / "candidate.duckdb"
    candidate.write_bytes(b"new")
    if existing_output:
        output.write_bytes(b"old")
    preflight_output(source, output, replace=True)
    wal = Path(f"{output}.wal")
    wal.write_bytes(b"wal")
    with pytest.raises(ETLError, match="Destination WAL"):
        preflight_output(source, output, replace=True)
    with pytest.raises(ETLError, match="Destination WAL.*Candidate retained"):
        publish_candidate(candidate, output, replace=True)
    assert wal.read_bytes() == b"wal"
    assert candidate.read_bytes() == b"new"
    assert output.read_bytes() == b"old" if existing_output else not output.exists()


@pytest.mark.parametrize("replace", [False, True])
def test_atomic_publication(tmp_path: Path, replace: bool) -> None:
    candidate = tmp_path / "candidate.duckdb"
    candidate.write_bytes(b"new")
    output = tmp_path / "output.duckdb"
    if replace:
        output.write_bytes(b"old")
    publish_candidate(candidate, output, replace=replace)
    assert output.read_bytes() == b"new"
    if replace:
        assert not candidate.exists()
    else:
        # Publication returns before cleanup, so cleanup cannot mask success.
        assert candidate.samefile(output)
        candidate.unlink()
        assert output.read_bytes() == b"new"


def test_link_success_does_not_attempt_fallible_cleanup(tmp_path: Path, monkeypatch):
    candidate = tmp_path / "candidate.duckdb"
    candidate.write_bytes(b"new")
    output = tmp_path / "output.duckdb"
    unlink = Mock(side_effect=PermissionError("cleanup denied"))
    monkeypatch.setattr(Path, "unlink", unlink)
    publish_candidate(candidate, output, replace=False)
    unlink.assert_not_called()
    # The future orchestrator owns warning-only cleanup after marking success.
    with pytest.raises(PermissionError, match="cleanup denied"):
        candidate.unlink()
    assert candidate.samefile(output)
    assert output.read_bytes() == b"new"


def test_collision_at_link_is_not_overwritten(tmp_path: Path, monkeypatch) -> None:
    candidate = tmp_path / "candidate.duckdb"
    candidate.write_bytes(b"new")
    output = tmp_path / "output.duckdb"
    link = os.link

    def late_collision(source, destination):
        destination.write_bytes(b"late")
        link(source, destination)

    monkeypatch.setattr(os, "link", late_collision)
    with pytest.raises(ETLError, match="Candidate retained") as error:
        publish_candidate(candidate, output, replace=False)
    assert str(candidate) in str(error.value)
    assert output.read_bytes() == b"late"
    assert candidate.read_bytes() == b"new"


@pytest.mark.parametrize(
    ("replace", "error_number"), [(True, errno.EACCES), (False, errno.EOPNOTSUPP)]
)
def test_denied_primitive_has_no_fallback(
    tmp_path: Path, monkeypatch, replace: bool, error_number: int
) -> None:
    candidate = tmp_path / "candidate.duckdb"
    candidate.write_bytes(b"new")
    output = tmp_path / "output.duckdb"
    if replace:
        output.write_bytes(b"old")
    operation = Mock(side_effect=OSError(error_number, "primitive refused"))
    monkeypatch.setattr(os, "replace" if replace else "link", operation)
    with pytest.raises(ETLError, match="primitive refused.*Candidate retained"):
        publish_candidate(candidate, output, replace=replace)
    operation.assert_called_once_with(candidate, output)
    assert candidate.read_bytes() == b"new"
    assert output.read_bytes() == b"old" if replace else not output.exists()


def test_preflight_inspection_error(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(Path, "lstat", Mock(side_effect=PermissionError("denied")))
    with pytest.raises(ETLError, match="Cannot inspect output.*denied"):
        preflight_output(tmp_path / "input", tmp_path / "output", replace=True)


@pytest.mark.parametrize("kind", ["consent", "symlink", "alias", "wal"])
def test_cli_refuses_before_processing(tmp_path: Path, monkeypatch, kind: str) -> None:
    source = tmp_path / "input.txt"
    source.write_bytes(b"source")
    output = tmp_path / "output.duckdb"
    messages = {
        "consent": "--replace",
        "symlink": "regular file",
        "alias": "same file",
        "wal": "Destination WAL",
    }
    if kind == "symlink":
        output.symlink_to(source)
    elif kind == "alias":
        os.link(source, output)
    elif kind == "wal":
        Path(f"{output}.wal").write_bytes(b"wal")
    else:
        output.write_bytes(b"old")
    boundaries = [
        "create_engine",
        "initialize_database",
        "ExtractStage",
        "TransformStage",
        "LoadStage",
        "ProgressManager",
    ]
    mocks = {name: Mock() for name in boundaries}
    for name, mock in mocks.items():
        monkeypatch.setattr(cli, name, mock)
    args = ["--input", str(source), "--output", str(output)]
    if kind != "consent":
        args.append("--replace")
    result = CliRunner().invoke(cli.app, args)
    assert result.exit_code == 1
    assert messages[kind] in result.output
    for mock in mocks.values():
        mock.assert_not_called()
    assert source.read_bytes() == b"source"
    if kind == "consent":
        assert output.read_bytes() == b"old"
    elif kind == "symlink":
        assert output.is_symlink()
    elif kind == "alias":
        assert output.samefile(source)
    else:
        assert not output.exists()
        assert Path(f"{output}.wal").read_bytes() == b"wal"
