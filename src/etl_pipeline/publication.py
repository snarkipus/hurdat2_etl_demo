"""Local-filesystem preflight and publication of closed, finalized candidates."""

import os
import stat
from pathlib import Path

from .exceptions import ETLError


def _check_destination(output_db: Path, *, replace: bool) -> None:
    if not output_db.parent.is_dir():
        raise ETLError(f"Output parent directory must exist: {output_db.parent}")
    # lexists also detects dangling symlinks; never follow or adopt a WAL.
    wal = Path(f"{output_db}.wal")
    if os.path.lexists(wal):
        raise ETLError(
            f"Destination WAL exists; quiesce writers before retrying: {wal}"
        )
    try:
        mode = output_db.lstat().st_mode
    except FileNotFoundError:
        return
    if not stat.S_ISREG(mode):
        raise ETLError(f"Output must be a regular file, not a symlink: {output_db}")
    if not replace:
        raise ETLError(
            f"Output already exists; use --replace to replace it: {output_db}"
        )


def preflight_output(input_file: Path, output_db: Path, *, replace: bool) -> None:
    """Refuse unsafe destinations before any database or stage work.

    Preserve the lexical output entry: callers must not resolve its symlinks.
    Input accessibility is validated by the CLI. Parents are ordinary local
    directories and operators must quiesce external writers.
    """
    try:
        _check_destination(output_db, replace=replace)
        if output_db.exists() and input_file.samefile(output_db):
            raise ETLError(f"Input and output identify the same file: {output_db}")
    except OSError as error:
        raise ETLError(f"Cannot inspect output {output_db}: {error}") from error


def publish_candidate(candidate: Path, output_db: Path, *, replace: bool) -> None:
    """Atomically publish a verified, closed regular-file sibling candidate.

    The caller owns candidate creation/finalization and distinct path identities.
    Recheck destination suitability/WAL immediately before the atomic primitive.
    Failure retains the candidate and never deletes or copies over the output.

    Return means publication succeeded. With no replacement, BOTH names remain:
    the caller must mark success BEFORE unlinking the redundant candidate name,
    and treat any unlink failure as a warning identifying that leftover path.
    This function deliberately performs no post-publication housekeeping.
    """
    try:
        _check_destination(output_db, replace=replace)
        if replace:
            os.replace(candidate, output_db)
        else:
            os.link(candidate, output_db)
    except (OSError, ETLError) as error:
        raise ETLError(
            f"Cannot publish to {output_db}: {error}. Candidate retained: {candidate}"
        ) from error
