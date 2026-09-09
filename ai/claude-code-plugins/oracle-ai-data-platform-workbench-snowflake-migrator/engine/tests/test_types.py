"""Snowflake -> Spark/Delta type mapping. Pure; no connection."""
import pytest

from snowflake_source.dialect.types import TypeMapping, map_type


@pytest.mark.parametrize("dt", ["NUMBER", "DECIMAL", "NUMERIC"])
def test_number_preserves_precision_and_scale(dt):
    m = map_type(dt, precision=18, scale=2)
    assert m.spark_type == "DECIMAL(18,2)"
    assert m.blocked is False


def test_number_scale_zero_is_still_decimal():
    # NUMBER(38,0) is Snowflake's default integer. Mapping it to BIGINT would
    # overflow: 38 digits does not fit in 64 bits.
    assert map_type("NUMBER", precision=38, scale=0).spark_type == "DECIMAL(38,0)"


def test_number_without_precision_is_blocked_not_guessed():
    m = map_type("NUMBER")
    assert m.blocked is True
    assert "precision" in m.reason.lower()


def test_timestamp_ntz_maps_to_ntz_not_timestamp():
    # Spark's bare TIMESTAMP is session-timezone-dependent; NTZ is not.
    m = map_type("TIMESTAMP_NTZ")
    assert m.spark_type == "TIMESTAMP_NTZ"
    assert m.warning is None


@pytest.mark.parametrize("dt", ["TIMESTAMP_LTZ", "TIMESTAMP_TZ"])
def test_zoned_timestamps_map_with_a_warning(dt):
    m = map_type(dt)
    assert m.spark_type == "TIMESTAMP"
    assert m.warning is not None and "timezone" in m.warning.lower()


@pytest.mark.parametrize("dt,expected", [
    ("TEXT", "STRING"), ("VARCHAR", "STRING"), ("CHAR", "STRING"),
    ("BOOLEAN", "BOOLEAN"), ("DATE", "DATE"), ("BINARY", "BINARY"),
    ("FLOAT", "DOUBLE"), ("DOUBLE", "DOUBLE"),
])
def test_direct_mappings(dt, expected):
    assert map_type(dt).spark_type == expected


def test_varchar_length_is_recorded_as_a_warning():
    m = map_type("TEXT", char_length=100)
    assert m.spark_type == "STRING"
    assert "100" in m.warning


@pytest.mark.parametrize("dt", ["VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY", "GEOMETRY"])
def test_semistructured_and_geo_are_blocked(dt):
    m = map_type(dt)
    assert m.blocked is True
    assert m.spark_type is None
    assert m.reason


def test_unknown_type_is_blocked_never_defaulted():
    m = map_type("SOME_FUTURE_TYPE")
    assert m.blocked is True
    assert "unmapped" in m.reason.lower()


def test_case_and_whitespace_insensitive():
    assert map_type("  number ", precision=5, scale=2).spark_type == "DECIMAL(5,2)"


def test_mapping_is_immutable():
    with pytest.raises(Exception):
        map_type("DATE").spark_type = "STRING"
