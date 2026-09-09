"""Exercise packaging with synthetic local state, never the checkout's private data."""

import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

from tests.installed_package_smoke import RESOURCES, SDIST_ROOT_FILES, validate_sdist


def test_sdist_excludes_local_state(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[1]
    project = tmp_path / "project"
    project.mkdir()
    (project / ".gitignore").write_text(".beads/\n.opencode/\n", encoding="utf-8")
    for name in SDIST_ROOT_FILES - {"PKG-INFO"}:
        shutil.copyfile(repo / name, project / name)
    for name in {f"src/{name}" for name in RESOURCES} | {
        "src/etl_pipeline/__init__.py",
        "tests/test_example.py",
    }:
        target = project / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("", encoding="utf-8")
    # Nested ignores reproduced the original default-Hatchling selection failure.
    for directory in (".beads", ".opencode", "arbitrary-private"):
        local = project / directory
        local.mkdir()
        (local / ".gitignore").write_text("*\n", encoding="utf-8")
    for name in (
        ".beads/backup/canary.darc",
        ".beads/interactions/canary.log",
        ".opencode/node_modules/canary/index.js",
        "arbitrary-private/canary.txt",
        "src/etl_pipeline/__pycache__/canary.pyc",
        "tests/.pytest_cache/canary",
        "tests/.ruff_cache/canary",
        "tests/.mypy_cache/canary",
        "tests/.hypothesis/canary",
        "tests/.coverage.canary",
    ):
        target = project / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("synthetic private canary\n", encoding="utf-8")
    uv = shutil.which("uv")
    assert uv is not None, "uv is required for packaging regression coverage"
    subprocess.run(  # noqa: S603 - fixed tool and synthetic test-owned project
        [uv, "build", "--sdist", "--out-dir", str(tmp_path / "dist")],
        cwd=project,
        check=True,
    )
    (sdist,) = (tmp_path / "dist").glob("*.tar.gz")
    validate_sdist(sdist)
    with tarfile.open(sdist) as archive:
        names = archive.getnames()
    assert not any("canary" in name for name in names)
    assert any(name.endswith("/tests/test_example.py") for name in names)


@pytest.mark.parametrize(
    "name",
    [
        ".beads/backup/synthetic.darc",
        ".beads/interactions/synthetic.log",
        ".opencode/node_modules/synthetic.js",
        "arbitrary-private/synthetic.txt",
        "tests/__pycache__/synthetic.pyc",
        "tests/.pytest_cache/synthetic",
        "../escape",
    ],
)
def test_sdist_inspector_rejects_unexpected_members(tmp_path: Path, name: str) -> None:
    sdist = tmp_path / "synthetic.tar.gz"
    with tarfile.open(sdist, "w:gz") as archive:
        for required in SDIST_ROOT_FILES | {f"src/{name}" for name in RESOURCES}:
            archive.addfile(tarfile.TarInfo(f"synthetic/{required}"))
        archive.addfile(tarfile.TarInfo(f"synthetic/{name}"))
    with pytest.raises(AssertionError, match="synthetic/"):
        validate_sdist(sdist)
