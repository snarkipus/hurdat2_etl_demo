"""Linux installed-distribution acceptance, reusable without pytest or CI machinery.

Run: uv run --locked python tests/installed_package_smoke.py --temp-root /tmp/opencode
Requires uv, Python 3.13 and access to the package index/DuckDB Spatial download
(or their caches). Builds wheel FROM sdist via uv build's default sequence.
Only the fixture and shared independent oracle cross into the temporary workspace;
no application source, pytest config, or Alembic config is copied. Temporary files
are removed on exit; commands, artifact hashes and installed versions go to stdout.
"""

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

RESOURCES = {
    "etl_pipeline/cli.py",
    "etl_pipeline/migrations/__init__.py",
    "etl_pipeline/migrations/env.py",
    "etl_pipeline/migrations/script.py.mako",
    "etl_pipeline/migrations/versions/53dc9abc36a8_initial_schema_setup.py",
    "etl_pipeline/migrations/versions/6accd1b8062d_rename_location_wkt_to_geom_in_.py",
}

PROBE = """
import importlib.metadata as metadata
import json, pathlib, sys
import etl_pipeline
repo = pathlib.Path(sys.argv[1]).resolve()
prefix = pathlib.Path(sys.prefix).resolve()
assert sys.version_info[:2] == (3, 13)
assert sys.flags.isolated and not sys.flags.optimize
assert not prefix.is_relative_to(repo)
assert all(not pathlib.Path(p).resolve().is_relative_to(repo) for p in sys.path)
assert pathlib.Path(etl_pipeline.__file__).resolve().is_relative_to(prefix)
distribution = metadata.distribution('etl-pipeline')
direct = json.loads(distribution.read_text('direct_url.json'))
assert not direct.get('dir_info', {}).get('editable')
assert direct['url'].endswith('.whl')
assert any(e.name == 'etl-pipeline' and e.value == 'etl_pipeline.cli:app'
           and e.group == 'console_scripts' for e in distribution.entry_points)
for d in metadata.distributions():
    direct = json.loads(d.read_text('direct_url.json') or '{}')
    assert not direct.get('dir_info', {}).get('editable')
print('Python:', sys.version)
print('Application:', etl_pipeline.__file__)
print('Installed versions:', json.dumps(dict(sorted(
    (d.metadata['Name'], d.version) for d in metadata.distributions())), sort_keys=True))
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--temp-root", required=True, type=Path)
    args = parser.parse_args()
    if sys.flags.optimize:
        parser.error("run without -O/PYTHONOPTIMIZE so smoke assertions remain enabled")
    repo = Path(__file__).resolve().parents[1]
    root = args.temp_root.resolve()
    if not root.is_dir() or root.is_relative_to(repo):
        parser.error("--temp-root must be an existing directory outside the checkout")
    uv = shutil.which("uv")
    if uv is None:
        parser.error("uv must be on PATH")
    # Do not inherit editable environment, Python startup, or coverage injection.
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("PYTHON", "UV_", "COV_CORE_", "CONDA"))
        and key != "VIRTUAL_ENV"
    }
    env.update(NO_COLOR="1", TERM="dumb")

    def run(*command: str, cwd: Path) -> str:
        print("+", *command, flush=True)
        result = subprocess.run(  # noqa: S603 - fixed tools and test-owned paths
            command,
            cwd=cwd,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        print(result.stdout, end="", flush=True)
        result.check_returncode()
        return result.stdout

    lock = (repo / "uv.lock").read_bytes()
    with tempfile.TemporaryDirectory(prefix="etl-wheel-", dir=root) as temporary:
        work = Path(temporary)
        dist = work / "dist"
        run(uv, "build", "--out-dir", str(dist), cwd=repo)
        (wheel,) = dist.glob("*.whl")
        (sdist,) = dist.glob("*.tar.gz")
        with zipfile.ZipFile(wheel) as archive:
            assert RESOURCES <= set(archive.namelist())
            (entrypoint,) = (
                n for n in archive.namelist() if n.endswith("/entry_points.txt")
            )
            assert (
                "etl-pipeline = etl_pipeline.cli:app"
                in archive.read(entrypoint).decode()
            )
        with tarfile.open(sdist) as archive:
            names = {
                name.split("/", 1)[1] for name in archive.getnames() if "/" in name
            }
            assert {f"src/{name}" for name in RESOURCES} <= names
        for artifact in (wheel, sdist):
            print(
                artifact.name,
                "sha256:",
                hashlib.sha256(artifact.read_bytes()).hexdigest(),
            )

        requirements = work / "runtime.txt"
        run(
            uv,
            "export",
            "--quiet",
            "--locked",
            "--no-dev",
            "--no-emit-project",
            "--output-file",
            str(requirements),
            cwd=repo,
        )
        venv = work / "venv"
        run(uv, "venv", "--python", "3.13", str(venv), cwd=work)
        python = str(venv / "bin" / "python")
        run(
            uv,
            "pip",
            "sync",
            "--python",
            python,
            "--require-hashes",
            str(requirements),
            cwd=work,
        )
        run(uv, "pip", "install", "--python", python, "--no-deps", str(wheel), cwd=work)
        run(uv, "pip", "check", "--python", python, cwd=work)
        run(python, "-I", "-c", PROBE, str(repo), cwd=work)
        shutil.copyfile(repo / "tests/unit/data/test_data.txt", work / "input.txt")
        shutil.copyfile(repo / "tests/baseline.py", work / "baseline.py")
        assert not (work / "alembic.ini").exists()
        # Execute the installed console script, not a source module/CliRunner.
        # -I ignores PYTHONPATH, user site and script/cwd import paths even if
        # a caller has configured them; the new venv contains no editable install.
        cli = str(venv / "bin" / "etl-pipeline")
        help_text = run(python, "-I", cli, "--help", cwd=work)
        for option in ("--input", "--output", "--log-level", "--replace"):
            assert option in help_text
        run(
            python,
            "-I",
            cli,
            "--input",
            "input.txt",
            "--output",
            "output.duckdb",
            "--log-level",
            "DEBUG",
            cwd=work,
        )
        run(
            python,
            "-I",
            "-c",
            "import pathlib, runpy; "
            "runpy.run_path('baseline.py')['assert_baseline'](pathlib.Path('output.duckdb'))",
            cwd=work,
        )
        assert not list(work.glob("*.wal"))
        assert not list(work.glob(".output.duckdb.*"))
        print("Installed-wheel CLI, packaged migrations and source-value oracle: PASS")
    assert (repo / "uv.lock").read_bytes() == lock, "Smoke changed the runtime lock"
    print("uv.lock sha256:", hashlib.sha256(lock).hexdigest())


if __name__ == "__main__":
    main()
