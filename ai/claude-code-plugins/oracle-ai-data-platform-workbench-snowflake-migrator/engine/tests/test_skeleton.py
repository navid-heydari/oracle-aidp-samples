"""The engine package tree exists and the fork's dead weight is gone."""
import importlib
import pathlib

import pytest

ENGINE = pathlib.Path(__file__).resolve().parents[1]

PACKAGES = [
    "snowflake_source",
    "snowflake_source.dialect",
    "snowflake_source.extract",
    "plan",
    "target",
    "report",
]

# Fork modules that MUST survive the prune -- the OCI transport layer.
KEPT = [
    "target/aidp_executor.py",
    "target/cluster_session.py",
    "target/cluster_lifecycle.py",
]

# Dead weight for MVP-1: no notebook migration, no data movement, no dbutils.
PRUNED = [
    "scripts/job_migrate.py",
    "scripts/agent_migrate.py",
    "scripts/build_dag.py",
    "scripts/cell_analyzer.py",
    "scripts/fuse_scanner.py",
    "scripts/acceptance_contract.py",
    "scripts/extract_catalog_databricks.py",
    "aidp_compat",
    "schemas",
    "run_migration.sh",
    "setup.py",
]


@pytest.mark.parametrize("pkg", PACKAGES)
def test_package_importable(pkg):
    assert importlib.import_module(pkg) is not None


@pytest.mark.parametrize("rel", KEPT)
def test_transport_modules_kept(rel):
    assert (ENGINE / rel).is_file(), f"{rel} must survive the prune"


@pytest.mark.parametrize("rel", PRUNED)
def test_dead_weight_pruned(rel):
    assert not (ENGINE / rel).exists(), f"{rel} is dead weight for MVP-1"


def test_corpus_relocated():
    assert (ENGINE / "snowflake_source/corpus/00_rappi_setup.sql").is_file()
    assert (ENGINE / "snowflake_source/corpus/validate.py").is_file()
