"""Test configuration and shared fixtures."""

import sys
from pathlib import Path

import pytest

# Add src to PYTHONPATH
root = Path(__file__).parent.parent
sys.path.insert(0, str(root / "src"))

@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Return path to test data directory."""
    return Path(__file__).parent / "data" / "hurdat2"

@pytest.fixture(scope="session")
def test_data_file(test_data_dir: Path) -> Path:
    """Return path to test data file."""
    return test_data_dir / "test_data.txt"
