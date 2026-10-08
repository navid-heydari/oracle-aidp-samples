"""Local pytest configuration for the lineage conformance suite.

Registers the suite's markers. A conftest.py is collected relative to the test file, so
the markers register wherever pytest is invoked from; a pytest.ini is only read when it is
the rootdir's config, so it would be ignored when running from the repository root.
"""


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "existence: proves the AIDP lineage API is released and live"
    )
    config.addinivalue_line(
        "markers", "population: probes whether the lineage graph is populated"
    )
    config.addinivalue_line(
        "markers", "legacy: probes the previous API generation (aidp.{region}/20240831)"
    )
