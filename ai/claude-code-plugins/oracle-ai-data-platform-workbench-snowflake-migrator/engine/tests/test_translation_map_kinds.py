"""The translation map agrees with the plan about views refused by KIND.

_dialect re-ran translate_view_body over every VIEW record without the
object_kind_block check plan and ddl apply first. So TRANSLATION_MAP.md and
the SUMMARY section said a SECURE view -- refused by the plan, never
emitted by ddl -- was "dialect-translated", with T01_IFF applied to it, or
"carried verbatim". And extract_view_body, whose CREATE VIEW regex has no
MATERIALIZED, raised for a materialized view whose DDL WAS captured; that
ValueError went into the same bucket as a missing DDL, so the map said
"with no SQL captured". Both contradicted plan.json and ddl_plan.json, and
the built-in demo reproduces the first.
"""
from report.render import render_translation_map, translation_map_section
from report.translation_map import build_translation_map


def _view(ident, ddl, **meta):
    return {"source_identifier": ident, "object_type": "VIEW", "columns": [],
            "view_ddl_get_ddl": ddl, "source_metadata": meta}


def _estate():
    return {"inventory": [
        _view("DB.S.SECURE_VW",
              "create or replace secure view SECURE_VW as "
              "select IFF(A > 0, 1, 0) X from DB.S.T;", is_secure="true"),
        _view("DB.S.SUMMARY_MV",
              "create or replace materialized view SUMMARY_MV as "
              "select A from DB.S.T;", is_materialized="true"),
    ]}


def test_a_view_refused_by_kind_is_not_counted_as_translated():
    tmap = build_translation_map(_estate(), {}, None)
    t = tmap["totals"]
    assert t["views_translated"] == 0
    assert t["views_verbatim"] == 0
    assert t["views_without_sql"] == 0, "the materialized view's DDL WAS captured"
    assert t["views_blocked_by_kind"] == 2
    for rule in tmap["dialect_rules"]:
        assert "DB.S.SECURE_VW" not in rule["applied_to"], rule["rule_id"]
        assert "DB.S.SECURE_VW" not in rule["objects"], rule["rule_id"]


def test_the_rendered_map_and_summary_name_the_kind_bucket():
    tmap = build_translation_map(_estate(), {}, None)
    md = render_translation_map(tmap)
    assert "**2** refused by kind" in md
    assert "**0** dialect-translated" in md
    assert "secure view" in md and "materialized view" in md
    section = "\n".join(translation_map_section(tmap))
    assert "2 refused by kind" in section


def test_captured_but_unparseable_sql_is_not_no_sql_captured():
    inv = {"inventory": [
        _view("DB.S.ODD", "this is not a create view statement"),
        _view("DB.S.NONE", None),
    ]}
    t = build_translation_map(inv, {}, None)["totals"]
    assert t["views_without_sql"] == 1
    assert t["views_unparseable"] == 1
    md = render_translation_map(build_translation_map(inv, {}, None))
    assert "**1** unparseable" in md


def test_the_demo_map_agrees_with_the_demo_plan(tmp_path):
    import json
    from emulation.runbook import run_demo
    run_demo(tmp_path)

    def read(name):
        return json.loads((tmp_path / name).read_text(encoding="utf-8"))

    plan = read("plan.json")
    tmap = build_translation_map(read("inventory.json"), plan,
                                 read("ddl_plan.json"))
    by_kind = {c["source_identifier"] for c in plan["cannot_migrate"]
               if c.get("object_type") == "VIEW"
               and c.get("category") == "unsupported_object"}
    assert by_kind, "the demo refuses a secure view by kind"
    assert {v["source_identifier"] for v in tmap["views_blocked_by_kind"]} \
        == by_kind
    for rule in tmap["dialect_rules"]:
        assert not by_kind & set(rule["objects"]), rule["rule_id"]
