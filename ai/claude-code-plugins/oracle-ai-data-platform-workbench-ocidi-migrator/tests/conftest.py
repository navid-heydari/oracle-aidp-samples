import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent))

SNAPSHOT = ROOT / "tests" / "fixtures" / "snapshot_sales"


@pytest.fixture(scope="session")
def snapshot_path():
    return SNAPSHOT


@pytest.fixture(scope="session")
def snapshot():
    from ocidi2aidp.snapshot import Snapshot
    return Snapshot.load(SNAPSHOT)


@pytest.fixture()
def config():
    from ocidi2aidp.config import MigrationConfig
    return MigrationConfig()


@pytest.fixture(scope="session")
def migrated(tmp_path_factory):
    """One strict migration of the fixture, shared read-only by many tests."""
    from ocidi2aidp.config import MigrationConfig
    from ocidi2aidp.migrate import migrate
    out = tmp_path_factory.mktemp("migrated")
    report = migrate(SNAPSHOT, out, MigrationConfig(), strict=True)
    return out, report


@pytest.fixture()
def fresh_migration(tmp_path):
    """A private migration a test may modify."""
    from ocidi2aidp.config import MigrationConfig
    from ocidi2aidp.migrate import migrate
    out = tmp_path / "out"
    report = migrate(SNAPSHOT, out, MigrationConfig(), strict=True)
    return out, report
