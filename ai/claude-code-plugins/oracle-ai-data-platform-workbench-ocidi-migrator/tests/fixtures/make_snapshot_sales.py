"""Write tests/fixtures/snapshot_sales: a hand-built OCI-DI workspace snapshot.

There was no live OCI-DI workspace while this tool was written, so this file
is the stand-in. Every object follows the field names of the OCI Python SDK
models (``oci.data_integration.models``, 2.165.1 -- camelCase as in each
model's ``attribute_map``). Where the SDK leaves a location open (which
config parameter carries a source's data asset), the choice made here is the
one ``ocidi2aidp.di`` documents. Replace or extend with a real ``extract``
snapshot as soon as one exists.

    python tests/fixtures/make_snapshot_sales.py
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

OUT = Path(__file__).parent / "snapshot_sales"


# ------------------------------------------------------------------ helpers
def ref(model_type, key, name):
    return {"refValue": {"modelType": model_type, "key": key, "name": name}}


ADW = ("ORACLE_ADWC_DATA_ASSET", "da-adw-sales", "ADW_SALES")
ADW_CONN = ("ORACLE_ADWC_CONNECTION", "conn-adw-sales", "ADW_SALES_CONN")
OS = ("ORACLE_OBJECT_STORAGE_DATA_ASSET", "da-os-landing", "OS_LANDING")
OS_CONN = ("ORACLE_OBJECT_STORAGE_CONNECTION", "conn-os-landing", "OS_LANDING_CONN")
PG = ("GENERIC_JDBC_DATA_ASSET", "da-pg-crm", "PG_CRM")
PG_CONN = ("GENERIC_JDBC_CONNECTION", "conn-pg-crm", "PG_CRM_CONN")
FUSION = ("FUSION_APP_DATA_ASSET", "da-fusion-erp", "FUSION_ERP")
FUSION_CONN = ("BICC_CONNECTION", "conn-fusion-erp", "FUSION_ERP_CONN")


def location(asset, conn, schema):
    return {"configParamValues": {
        "dataAssetParam": ref(*asset),
        "connectionParam": ref(*conn),
        "schemaParam": ref("SCHEMA", f"sch-{schema.lower()}", schema),
    }}


def field(key, name, typ, model_type="SHAPE_FIELD"):
    return {"modelType": model_type, "key": key, "name": name, "type": typ}


def dtype(name):
    return {"modelType": "DATA_TYPE", "key": f"dt-{name}", "name": name, "dtType": name}


def ctype(base, **cfg):
    return {"modelType": "CONFIGURED_TYPE", "key": f"ct-{base}", "wrappedType": dtype(base),
            "configValues": {"configParamValues": {k: {"intValue": v} for k, v in cfg.items()}}}


def derived(key, name, expr, typ="VARCHAR"):
    return {"modelType": "DERIVED_FIELD", "key": key, "name": name, "type": typ,
            "expr": {"modelType": "EXPRESSION", "key": f"{key}-x", "exprString": expr}}


def entity(model_type, key, name, fields, *, sql=None, fmt=None, unique=None):
    e = {"modelType": model_type, "key": key, "name": name, "resourceName": name,
         "shape": {"modelType": "SHAPE", "key": f"{key}-shape",
                   "type": {"modelType": "COMPOSITE_TYPE", "key": f"{key}-ctype",
                            "elements": fields}}}
    if sql:
        e["sqlQuery"] = sql
    if fmt:
        e["dataFormat"] = fmt
    if unique:
        e["uniqueKeys"] = [{"modelType": "PRIMARY_KEY", "key": f"{key}-pk", "name": "PK",
                            "attributeRefs": [{"position": i + 1, "attribute": {"name": n}}
                                              for i, n in enumerate(unique)]}]
    return e


def port(key, name="OUTPUT1", fields=(), model_type="OUTPUT_PORT", **extra):
    return {"modelType": model_type, "key": key, "name": name, "portType": "DATA",
            "fields": list(fields), **extra}


def in_port(key, name="INPUT1", fields=()):
    return port(key, name, fields, "INPUT_PORT")


class Flow:
    """Builds DataFlow JSON with linked FlowNodes."""

    def __init__(self, key, name, folder, params=()):
        self.data = {"modelType": "DATA_FLOW", "key": key, "name": name, "identifier": name,
                     "modelVersion": "20200430", "objectVersion": 1,
                     "parentRef": {"parent": folder}, "nodes": [],
                     "parameters": [dict(modelType="PARAMETER", key=f"{key}-p-{p['name']}", **p)
                                    for p in params]}
        self.n = 0

    def node(self, name, operator):
        operator.setdefault("key", f"{self.data['key']}-{name}-op")
        operator.setdefault("identifier", name)
        operator.setdefault("name", name)
        operator.setdefault("inputPorts", [])
        operator.setdefault("outputPorts", [])
        node = {"modelType": "FLOW_NODE", "key": f"{self.data['key']}-{name}", "name": name,
                "operator": operator, "inputLinks": [], "outputLinks": []}
        self.data["nodes"].append(node)
        return node

    def link(self, src, dst, *, src_port=0, dst_port=0, field_map=None):
        self.n += 1
        ol = {"modelType": "OUTPUT_LINK", "key": f"{self.data['key']}-ol{self.n}",
              "port": src["operator"]["outputPorts"][src_port]["key"],
              "toLinks": [f"{self.data['key']}-il{self.n}"]}
        il = {"modelType": "INPUT_LINK", "key": f"{self.data['key']}-il{self.n}",
              "port": dst["operator"]["inputPorts"][dst_port]["key"],
              "fromLink": ol["key"]}
        if field_map:
            il["fieldMap"] = field_map
        src["outputLinks"].append(ol)
        dst["inputLinks"].append(il)


def source(flow, name, asset, conn, schema, ent, fields, **op):
    keyed = [field(f"{flow.data['key']}-{name}-{n}", n, t) for n, t in fields]
    return flow.node(name, {
        "modelType": "SOURCE_OPERATOR", "entity": entity(ent.pop("model_type", "TABLE_ENTITY"),
                                                         f"ent-{name.lower()}", **ent,
                                                         fields=keyed),
        "opConfigValues": location(asset, conn, schema),
        "outputPorts": [port(f"{flow.data['key']}-{name}-out", fields=keyed)], **op})


def target(flow, name, asset, conn, schema, table, fields, mode, merge_key=None, load_order=1,
           field_keys=False):
    keyed = [field(f"tgt-{table.lower()}-{n}", n, t) for n, t in fields]
    woc = {"modelType": "WRITE_OPERATION_CONFIG", "key": f"{name}-woc", "writeMode": mode}
    if merge_key:
        woc["mergeKey"] = {"modelType": "UNIQUE_KEY", "key": f"{name}-mk", "name": "MK",
                           "attributeRefs": [{"position": i + 1, "attribute": {"name": k}}
                                             for i, k in enumerate(merge_key)]}
    return flow.node(name, {
        "modelType": "TARGET_OPERATOR",
        "entity": entity("TABLE_ENTITY", f"ent-{table.lower()}", table, keyed),
        "opConfigValues": location(asset, conn, schema), "writeOperationConfig": woc,
        "loadOrder": load_order, "isPredefinedShape": True,
        "inputPorts": [in_port(f"{flow.data['key']}-{name}-in")]})


def simple(flow, name, model_type, n_inputs=1, out_fields=(), **op):
    return flow.node(name, {
        "modelType": model_type,
        "inputPorts": [in_port(f"{flow.data['key']}-{name}-in{i}", f"INPUT{i + 1}")
                       for i in range(n_inputs)],
        "outputPorts": [port(f"{flow.data['key']}-{name}-out", fields=out_fields)], **op})


def expr(text):
    return {"modelType": "EXPRESSION", "exprString": text}


T = {"INT": dtype("INTEGER"), "NUM": dtype("NUMBER"), "VC": dtype("VARCHAR2"),
     "TS": dtype("TIMESTAMP"), "DATE": dtype("DATE"),
     "AMT": ctype("NUMBER", precision=12, scale=2), "ID": ctype("NUMBER", precision=10)}


# ------------------------------------------------------------- data flows
def df_load_customers():
    f = Flow("df-load-customers", "DF_LOAD_CUSTOMERS", "fld-ingest", params=[
        {"name": "P_SINCE", "typeName": "TIMESTAMP", "defaultValue": "2020-01-01 00:00:00"}])
    src = source(f, "SOURCE_1", OS, OS_CONN, "landing",
                 {"name": "customers/customers.csv", "model_type": "FILE_ENTITY",
                  "fmt": {"type": "CSV", "formatAttribute": {
                      "modelType": "CSV_FORMAT", "hasHeader": True, "delimiter": ","}}},
                 [("CUSTOMER_ID", T["INT"]), ("FIRST_NAME", T["VC"]), ("LAST_NAME", T["VC"]),
                  ("EMAIL", T["VC"]), ("COUNTRY", T["VC"]), ("UPDATED_AT", T["TS"])])
    ex = simple(f, "EXPRESSION_1", "EXPRESSION_OPERATOR", out_fields=[
        derived("ex1-full", "FULL_NAME", "INITCAP(CONCAT(SOURCE_1.CUSTOMERS.FIRST_NAME, ' ', "
                                         "SOURCE_1.CUSTOMERS.LAST_NAME))"),
        derived("ex1-email", "EMAIL_NORM", "LOWER(TRIM(EXPRESSION_1.CUSTOMERS.EMAIL))"),
        derived("ex1-seg", "SEGMENT", "DECODE(EXPRESSION_1.CUSTOMERS.COUNTRY, 'US', 'DOMESTIC', "
                                      "'CA', 'DOMESTIC', 'INTERNATIONAL')"),
    ])
    flt = simple(f, "FILTER_1", "FILTER_OPERATOR",
                 filterCondition=expr("FILTER_1.CUSTOMERS.UPDATED_AT >= $P_SINCE "
                                      "AND EXPRESSION_1.CUSTOMERS.EMAIL_NORM IS NOT NULL"))
    tgt = target(f, "TARGET_1", ADW, ADW_CONN, "SALES_DW", "DIM_CUSTOMER",
                 [("CUSTOMER_ID", T["ID"]), ("FULL_NAME", ctype("VARCHAR2", length=200)),
                  ("EMAIL", ctype("VARCHAR2", length=200)), ("COUNTRY", ctype("VARCHAR2", length=2)),
                  ("SEGMENT", ctype("VARCHAR2", length=20)), ("UPDATED_AT", T["TS"])],
                 "MERGE", merge_key=["CUSTOMER_ID"])
    f.link(src, ex)
    f.link(ex, flt)
    direct = [("df-load-customers-SOURCE_1-CUSTOMER_ID", "tgt-dim_customer-CUSTOMER_ID"),
              ("ex1-full", "tgt-dim_customer-FULL_NAME"),
              ("ex1-email", "tgt-dim_customer-EMAIL"),
              ("df-load-customers-SOURCE_1-COUNTRY", "tgt-dim_customer-COUNTRY"),
              ("ex1-seg", "tgt-dim_customer-SEGMENT"),
              ("df-load-customers-SOURCE_1-UPDATED_AT", "tgt-dim_customer-UPDATED_AT")]
    f.link(flt, tgt, field_map={
        "modelType": "COMPOSITE_FIELD_MAP", "key": "fm-dim-customer",
        "fieldMaps": [{"modelType": "DIRECT_FIELD_MAP", "key": f"dfm{i}",
                       "sourceTypedObject": s, "targetTypedObject": t}
                      for i, (s, t) in enumerate(direct)]})
    return f.data


def df_orders_daily():
    f = Flow("df-orders-daily", "DF_ORDERS_DAILY", "fld-marts", params=[
        {"name": "P_DISCOUNT", "typeName": "DECIMAL", "defaultValue": "0.10"}])
    orders = source(f, "SOURCE_ORDERS", ADW, ADW_CONN, "SALES", {"name": "ORDERS"},
                    [("ORDER_ID", T["ID"]), ("CUSTOMER_ID", T["ID"]), ("PRODUCT_ID", T["ID"]),
                     ("ORDER_DATE", T["DATE"]), ("AMOUNT", T["AMT"]), ("STATUS", T["VC"]),
                     ("UPDATED_AT", T["TS"])],
                    readOperationConfig={
                        "modelType": "READ_OPERATION_CONFIG", "key": "roc-orders",
                        "incrementalReadConfig": {"lastExtractedFieldDate": [{
                            "incrementalFieldName": "UPDATED_AT",
                            "incrementalComparator": "GREATERTHAN"}]}})
    custs = source(f, "SOURCE_CUSTOMERS", ADW, ADW_CONN, "SALES", {"name": "CUSTOMERS"},
                   [("CUSTOMER_ID", T["ID"]), ("COUNTRY", T["VC"])])
    prods = source(f, "SOURCE_PRODUCTS", ADW, ADW_CONN, "SALES", {"name": "PRODUCTS"},
                   [("PRODUCT_ID", T["ID"]), ("CATEGORY", T["VC"])])
    valid = simple(f, "FILTER_VALID", "FILTER_OPERATOR",
                   filterCondition=expr("SOURCE_ORDERS.ORDERS.STATUS <> 'CANCELLED'"))
    join = simple(f, "JOIN_CUST", "JOINER_OPERATOR", n_inputs=2, joinType="INNER",
                  joinCondition=expr("FILTER_VALID.ORDERS.CUSTOMER_ID = "
                                     "SOURCE_CUSTOMERS.CUSTOMERS.CUSTOMER_ID"))
    look = simple(f, "LOOKUP_PROD", "LOOKUP_OPERATOR", n_inputs=2,
                  lookupCondition=expr("JOIN_CUST.ORDERS.PRODUCT_ID = "
                                       "SOURCE_PRODUCTS.PRODUCTS.PRODUCT_ID"),
                  multiMatchStrategy="RETURN_ANY", isSkipNoMatch=False,
                  nullFillValues={"CATEGORY": "UNKNOWN"})
    enrich = simple(f, "EXPR_ENRICH", "EXPRESSION_OPERATOR", out_fields=[
        derived("en-month", "ORDER_MONTH", "TO_CHAR(LOOKUP_PROD.ORDERS.ORDER_DATE, 'yyyy-MM')"),
        derived("en-net", "NET_AMOUNT", "ROUND(NVL(LOOKUP_PROD.ORDERS.AMOUNT, 0) * "
                                        "(1 - $P_DISCOUNT), 2)", "DECIMAL"),
        derived("en-large", "IS_LARGE", "CASE WHEN LOOKUP_PROD.ORDERS.AMOUNT >= 1000 THEN 'Y' "
                                        "ELSE 'N' END"),
    ])
    split = f.node("SPLIT_SIZE", {
        "modelType": "SPLIT_OPERATOR", "dataRoutingStrategy": "FIRST",
        "inputPorts": [in_port("df-orders-daily-SPLIT_SIZE-in")],
        "outputPorts": [
            port("split-large", "LARGE", model_type="CONDITIONAL_OUTPUT_PORT",
                 splitCondition=expr("EXPR_ENRICH.ORDERS.IS_LARGE = 'Y'")),
            port("split-unmatched", "UNMATCHED")]})
    agg = f.node("AGG_DAILY", {
        "modelType": "AGGREGATOR_OPERATOR",
        "groupByColumns": {"modelType": "DYNAMIC_PROXY_FIELD", "key": "agg-gb", "name": "GB",
                           "type": {"modelType": "DYNAMIC_TYPE", "key": "agg-gb-t",
                                    "typeHandler": {"modelType": "RULE_TYPE_CONFIGS", "key": "rtc",
                                                    "projectionRules": [{
                                                        "modelType": "NAME_LIST_RULE",
                                                        "key": "nlr", "ruleType": "INCLUDE",
                                                        "names": ["ORDER_DATE", "COUNTRY",
                                                                  "CATEGORY"]}]}}},
        "inputPorts": [in_port("df-orders-daily-AGG_DAILY-in")],
        "outputPorts": [port("df-orders-daily-AGG_DAILY-out", fields=[
            derived("agg-total", "TOTAL_NET", "SUM(EXPR_ENRICH.ORDERS.NET_AMOUNT)", "DECIMAL"),
            derived("agg-count", "ORDER_COUNT", "COUNT(*)", "INTEGER")])]})
    daily = target(f, "TARGET_DAILY", ADW, ADW_CONN, "SALES_DW", "FACT_ORDERS_DAILY",
                   [("ORDER_DATE", T["DATE"]), ("COUNTRY", ctype("VARCHAR2", length=2)),
                    ("CATEGORY", ctype("VARCHAR2", length=50)),
                    ("TOTAL_NET", ctype("NUMBER", precision=14, scale=2)),
                    ("ORDER_COUNT", T["INT"])], "OVERWRITE", load_order=2)
    large = target(f, "TARGET_LARGE", ADW, ADW_CONN, "SALES_DW", "LARGE_ORDERS",
                   [("ORDER_ID", T["ID"]), ("CUSTOMER_ID", T["ID"]), ("AMOUNT", T["AMT"]),
                    ("NET_AMOUNT", T["AMT"]), ("ORDER_MONTH", ctype("VARCHAR2", length=7))],
                   "APPEND", load_order=1)
    f.link(orders, valid)
    f.link(valid, join, dst_port=0)
    f.link(custs, join, dst_port=1)
    f.link(join, look, dst_port=0)
    f.link(prods, look, dst_port=1)
    f.link(look, enrich)
    f.link(enrich, split)
    f.link(enrich, agg)
    f.link(agg, daily, field_map={"modelType": "RULE_BASED_FIELD_MAP", "key": "rb-daily",
                                  "mapType": "MAPBYNAME"})
    f.link(split, large, src_port=0, field_map={"modelType": "RULE_BASED_FIELD_MAP",
                                                "key": "rb-large", "mapType": "MAPBYNAME"})
    return f.data


def df_enrich_sentiment():
    f = Flow("df-enrich-sentiment", "DF_ENRICH_SENTIMENT", "fld-marts")
    src = source(f, "SOURCE_REVIEWS", ADW, ADW_CONN, "SALES", {"name": "REVIEWS"},
                 [("REVIEW_ID", T["ID"]), ("REVIEW_TEXT", T["VC"])])
    fn = simple(f, "SENTIMENT_FN", "FUNCTION_OPERATOR",
                out_fields=[field("fn-score", "SENTIMENT_SCORE", T["NUM"], "OUTPUT_FIELD")],
                ociFunction={"modelType": "OCI_FUNCTION",
                             "functionId": "ocid1.fnfunc.oc1.iad.exampleexampleexample",
                             "regionId": "us-ashburn-1", "payloadFormat": "JSON"})
    tgt = target(f, "TARGET_SENTIMENT", ADW, ADW_CONN, "SALES_DW", "REVIEW_SENTIMENT",
                 [("REVIEW_ID", T["ID"]), ("SENTIMENT_SCORE", T["NUM"])], "APPEND")
    f.link(src, fn)
    f.link(fn, tgt)
    return f.data


def df_crm_contacts():
    f = Flow("df-crm-contacts", "DF_CRM_CONTACTS", "fld-ingest")
    pg = source(f, "SOURCE_CONTACTS", PG, PG_CONN, "public", {"name": "contacts"},
                [("CONTACT_ID", T["INT"]), ("EMAIL", T["VC"]), ("UPDATED_AT", T["TS"])])
    legacy = source(f, "SOURCE_LEGACY", OS, OS_CONN, "landing",
                    {"name": "contacts_legacy/*.json", "model_type": "FILE_ENTITY",
                     "fmt": {"type": "JSON", "formatAttribute": {"modelType": "JSON_FORMAT",
                                                                 "encoding": "UTF-8"}}},
                    [("CONTACT_ID", T["INT"]), ("EMAIL", T["VC"]), ("UPDATED_AT", T["TS"])])
    union = simple(f, "UNION_ALL", "UNION_OPERATOR", n_inputs=2, unionType="NAME", isAll=True)
    hashed = simple(f, "EXPR_HASH", "EXPRESSION_OPERATOR", out_fields=[
        derived("hash-bucket", "EMAIL_BUCKET", "MOD(ORA_HASH(UNION_ALL.CONTACTS.EMAIL), 16)",
                "INTEGER")])
    dist = simple(f, "DISTINCT_1", "DISTINCT_OPERATOR")
    srt = simple(f, "SORT_1", "SORT_OPERATOR", sortKey={"sortRules": [
        {"isAscending": False, "wrappedRule": {"modelType": "NAME_LIST_RULE", "key": "sr1",
                                               "names": ["UPDATED_AT"]}}]})
    tgt = target(f, "TARGET_CONTACTS", ADW, ADW_CONN, "SALES_DW", "CONTACTS",
                 [("CONTACT_ID", T["INT"]), ("EMAIL", T["VC"]), ("UPDATED_AT", T["TS"]),
                  ("EMAIL_BUCKET", T["INT"])], "OVERWRITE")
    f.link(pg, union, dst_port=0)
    f.link(legacy, union, dst_port=1)
    f.link(union, hashed)
    f.link(hashed, dist)
    f.link(dist, srt)
    f.link(srt, tgt)
    return f.data


def df_load_products():
    f = Flow("df-load-products", "DF_LOAD_PRODUCTS", "fld-ingest")
    src = source(f, "SOURCE_PRODUCTS", ADW, ADW_CONN, "SALES", {"name": "PRODUCTS"},
                 [("PRODUCT_ID", T["ID"]), ("CATEGORY", T["VC"]), ("LIST_PRICE", T["AMT"])])
    tgt = target(f, "TARGET_PRODUCTS", ADW, ADW_CONN, "SALES_DW", "DIM_PRODUCT",
                 [("PRODUCT_ID", T["ID"]), ("CATEGORY", ctype("VARCHAR2", length=50)),
                  ("LIST_PRICE", T["AMT"])], "OVERWRITE")
    f.link(src, tgt, field_map={"modelType": "RULE_BASED_FIELD_MAP", "key": "rb-prod",
                                "mapType": "MAPBYNAME"})
    return f.data


def df_bicc_gl():
    f = Flow("df-bicc-gl", "DF_BICC_GL", "fld-ingest")
    src = source(f, "SOURCE_GL", FUSION, FUSION_CONN, "FscmTopModelAM.FinExtractAM",
                 {"name": "GlJournalExtractPVO"}, [("JE_HEADER_ID", T["NUM"]),
                                                    ("AMOUNT", T["AMT"])])
    tgt = target(f, "TARGET_GL", ADW, ADW_CONN, "SALES_DW", "GL_JOURNALS",
                 [("JE_HEADER_ID", T["NUM"]), ("AMOUNT", T["AMT"])], "APPEND")
    f.link(src, tgt)
    return f.data


# ------------------------------------------------------------------ tasks
def task(model_type, key, name, folder, **extra):
    return {"modelType": model_type, "key": key, "name": name, "identifier": name,
            "modelVersion": "20200430", "objectVersion": 1, "parentRef": {"parent": folder},
            **extra}


def bindings(**values):
    return {"bindings": {k: {"simpleValue": v} for k, v in values.items()}}


class Pipeline(Flow):
    def __init__(self, key, name, folder):
        super().__init__(key, name, folder)
        self.data["modelType"] = "PIPELINE"
        self.data["variables"] = []

    def op(self, name, model_type, **extra):
        return self.node(name, {"modelType": model_type,
                                "inputPorts": [in_port(f"{self.data['key']}-{name}-in")],
                                "outputPorts": [port(f"{self.data['key']}-{name}-out")], **extra})

    def task(self, name, task_obj, **extra):
        ref_ = {k: task_obj[k] for k in ("modelType", "key", "name", "identifier")}
        return self.op(name, "TASK_OPERATOR", taskType=task_obj["modelType"], task=ref_,
                       triggerRule=extra.pop("triggerRule", "ALL_SUCCESS"), **extra)


def build():
    flows = {d["key"]: d for d in (df_load_customers(), df_orders_daily(), df_enrich_sentiment(),
                                   df_crm_contacts(), df_load_products(), df_bicc_gl())}
    by_name = {d["name"]: d for d in flows.values()}
    tasks = [
        task("INTEGRATION_TASK", "t-load-customers", "IT_LOAD_CUSTOMERS", "fld-ingest",
             dataFlow=by_name["DF_LOAD_CUSTOMERS"],
             configProviderDelegate=bindings(P_SINCE="2021-01-01 00:00:00")),
        task("INTEGRATION_TASK", "t-orders-daily", "IT_ORDERS_DAILY", "fld-marts",
             dataFlow=by_name["DF_ORDERS_DAILY"]),
        task("INTEGRATION_TASK", "t-enrich-sentiment", "IT_ENRICH_SENTIMENT", "fld-marts",
             dataFlow=by_name["DF_ENRICH_SENTIMENT"]),
        task("INTEGRATION_TASK", "t-crm-contacts", "IT_CRM_CONTACTS", "fld-ingest",
             dataFlow=by_name["DF_CRM_CONTACTS"]),
        task("DATA_LOADER_TASK", "t-dl-products", "DL_PRODUCTS", "fld-ingest",
             dataFlow=by_name["DF_LOAD_PRODUCTS"], isSingleLoad=True, parallelLoadLimit=1),
        task("INTEGRATION_TASK", "t-bicc-gl", "IT_BICC_GL", "fld-ingest",
             dataFlow=by_name["DF_BICC_GL"]),
        task("SQL_TASK", "t-sql-stats", "SQL_REFRESH_STATS", "fld-marts",
             sqlScriptType="STORED_PROCEDURE",
             script={"modelType": "SCRIPT", "key": "scr-stats", "name": "SALES_DW.REFRESH_STATS"},
             opConfigValues=location(ADW, ADW_CONN, "SALES_DW")),
        task("REST_TASK", "t-rest-notify", "REST_NOTIFY", "fld-marts",
             methodType="POST", endpoint=expr("https://hooks.example.com/etl/notify"),
             headers={"Content-Type": "application/json"},
             jsonData='{"pipeline": "nightly", "status": "FAILED"}',
             authDetails={"modelType": "NO_AUTH_REST_DETAILS", "key": "auth-none"},
             apiCallMode="SYNCHRONOUS"),
        task("OCI_DATAFLOW_TASK", "t-ocidf-score", "OCIDF_SCORE", "fld-marts",
             dataflowApplication={"applicationId": "ocid1.dataflowapplication.oc1.iad.example",
                                  "compartmentId": "ocid1.compartment.oc1..example"},
             driverShapeDetails={"shape": "VM.Standard.E4.Flex"}),
    ]
    tmap = {t["name"]: t for t in tasks}

    nightly = Pipeline("pl-nightly", "PL_NIGHTLY", "fld-marts")
    start = nightly.op("START_1", "START_OPERATOR")
    t_c = nightly.task("LOAD_CUSTOMERS", tmap["IT_LOAD_CUSTOMERS"])
    t_p = nightly.task("LOAD_PRODUCTS", tmap["DL_PRODUCTS"])
    merge = nightly.op("MERGE_1", "MERGE_OPERATOR", triggerRule="ALL_SUCCESS")
    t_o = nightly.task("ORDERS_DAILY", tmap["IT_ORDERS_DAILY"], retryAttempts=2, retryDelay=5,
                       retryDelayUnit="MINUTES")
    t_s = nightly.task("REFRESH_STATS", tmap["SQL_REFRESH_STATS"])
    t_n = nightly.task("NOTIFY_FAILURE", tmap["REST_NOTIFY"], triggerRule="ALL_FAILED")
    end = nightly.op("END_1", "END_OPERATOR", triggerRule="ALL_COMPLETE")
    for a, b in ((start, t_c), (start, t_p), (t_c, merge), (t_p, merge), (merge, t_o),
                 (t_o, t_s), (t_o, t_n), (t_s, end), (t_n, end)):
        nightly.link(a, b)

    weekly = Pipeline("pl-weekly", "PL_WEEKLY", "fld-marts")
    w_start = weekly.op("START_1", "START_OPERATOR")
    w_enrich = weekly.task("ENRICH", tmap["IT_ENRICH_SENTIMENT"])
    w_dec = weekly.op("DECISION_1", "DECISION_OPERATOR", triggerRule="ALL_SUCCESS",
                      opConfigValues={"configParamValues": {"decisionCondition": {
                          "objectValue": expr("ENRICH.SYS.STATUS = 'SUCCESS'")}}})
    w_dec["operator"]["outputPorts"] = [port("pl-weekly-dec-true", "TRUE"),
                                        port("pl-weekly-dec-false", "FALSE")]
    w_crm = weekly.task("CRM", tmap["IT_CRM_CONTACTS"])
    w_score = weekly.task("SCORE", tmap["OCIDF_SCORE"])
    w_end = weekly.op("END_1", "END_OPERATOR", triggerRule="ALL_COMPLETE")
    weekly.link(w_start, w_enrich)
    weekly.link(w_enrich, w_dec)
    weekly.link(w_dec, w_crm, src_port=0)
    weekly.link(w_dec, w_score, src_port=1)
    weekly.link(w_crm, w_end)
    weekly.link(w_score, w_end)

    pipelines = [nightly.data, weekly.data]
    tasks += [
        task("PIPELINE_TASK", "t-pt-nightly", "PT_NIGHTLY", "fld-marts", pipeline=nightly.data),
        task("PIPELINE_TASK", "t-pt-weekly", "PT_WEEKLY", "fld-marts", pipeline=weekly.data),
    ]
    tmap = {t["name"]: t for t in tasks}

    published = {}
    for name in ("IT_LOAD_CUSTOMERS", "IT_ORDERS_DAILY", "DL_PRODUCTS", "IT_ENRICH_SENTIMENT",
                 "PT_NIGHTLY", "PT_WEEKLY"):
        po = json.loads(json.dumps(tmap[name]))
        po["key"] = "po-" + tmap[name]["key"][2:]
        po["parentRef"] = {"parent": "app-prod"}
        published[po["key"]] = po

    schedules = {
        "sch-nightly": {"modelType": "SCHEDULE", "key": "sch-nightly", "name": "SCH_NIGHTLY",
                        "identifier": "SCH_NIGHTLY", "timezone": "America/Chicago",
                        "isDaylightAdjustmentEnabled": True,
                        "frequencyDetails": {"modelType": "DAILY", "frequency": "DAILY",
                                             "interval": 1,
                                             "time": {"hour": 2, "minute": 30, "second": 0}}},
        "sch-monday": {"modelType": "SCHEDULE", "key": "sch-monday", "name": "SCH_MONDAY",
                       "identifier": "SCH_MONDAY", "timezone": "UTC",
                       "frequencyDetails": {"modelType": "CUSTOM", "frequency": "CUSTOM",
                                            "customExpression": "0 6 * * 1"}},
        "sch-hourly": {"modelType": "SCHEDULE", "key": "sch-hourly", "name": "SCH_EVERY_2H",
                       "identifier": "SCH_EVERY_2H", "timezone": "UTC",
                       "frequencyDetails": {"modelType": "HOURLY", "frequency": "HOURLY",
                                            "interval": 2, "time": {"minute": 15}}},
        "sch-month-end": {"modelType": "SCHEDULE", "key": "sch-month-end",
                          "name": "SCH_LAST_FRIDAY", "identifier": "SCH_LAST_FRIDAY",
                          "timezone": "Europe/London",
                          "frequencyDetails": {"modelType": "MONTHLY_RULE",
                                               "frequency": "MONTHLY", "interval": 1,
                                               "weekOfMonth": "LAST", "dayOfWeek": "FRIDAY",
                                               "time": {"hour": 23, "minute": 0}}},
    }

    def ts(key, name, po_key, sched, enabled=True, **extra):
        return {"modelType": "TASK_SCHEDULE", "key": key, "name": name, "identifier": name,
                "parentRef": {"parent": po_key}, "scheduleRef": schedules[sched],
                "isEnabled": enabled, "isConcurrentAllowed": False, "retryAttempts": 0,
                **extra}

    task_schedules = {t["key"]: t for t in (
        ts("ts-nightly", "TS_NIGHTLY", "po-pt-nightly", "sch-nightly",
           startTimeMillis=1767225600000),
        ts("ts-weekly", "TS_WEEKLY", "po-pt-weekly", "sch-monday"),
        ts("ts-enrich", "TS_ENRICH", "po-enrich-sentiment", "sch-hourly", enabled=False),
        ts("ts-products-me", "TS_PRODUCTS_MONTH_END", "po-dl-products", "sch-month-end",
           configProviderDelegate=bindings()),
    )}

    runs = {"po-orders-daily": {
        "modelType": "TASK_RUN", "key": "run-orders-1", "name": "IT_ORDERS_DAILY_run",
        "status": "SUCCESS", "taskKey": "po-orders-daily",
        "startTimeMillis": 1790834400000, "endTimeMillis": 1790834700000}}

    workspace = {"id": "ocid1.disworkspace.oc1.iad.example", "displayName": "SALES_DI",
                 "lifecycleState": "ACTIVE"}
    projects = [{"modelType": "PROJECT", "key": "proj-sales", "name": "SALES",
                 "identifier": "SALES"}]
    folders = [{"modelType": "FOLDER", "key": "fld-ingest", "name": "Ingest",
                "identifier": "INGEST", "parentRef": {"parent": "proj-sales"}},
               {"modelType": "FOLDER", "key": "fld-marts", "name": "Marts", "identifier": "MARTS",
                "parentRef": {"parent": "proj-sales"}}]
    assets = [
        {"modelType": ADW[0], "key": ADW[1], "name": ADW[2], "identifier": ADW[2],
         "serviceName": "salesadw_low", "defaultConnection": {"key": ADW_CONN[1]}},
        {"modelType": OS[0], "key": OS[1], "name": OS[2], "identifier": OS[2],
         "namespace": "idxyzsample", "ociRegion": "us-ashburn-1"},
        {"modelType": PG[0], "key": PG[1], "name": PG[2], "identifier": PG[2],
         "host": "crm-db.internal.example", "port": "5432", "dataAssetType": "POSTGRESQL"},
        {"modelType": FUSION[0], "key": FUSION[1], "name": FUSION[2], "identifier": FUSION[2],
         "serviceUrl": "https://fa-example.oraclecloud.com"},
    ]
    connections = [
        {"modelType": ADW_CONN[0], "key": ADW_CONN[1], "name": ADW_CONN[2],
         "username": "DI_USER", "passwordSecret": {"secretConfig": {
             "modelType": "OCI_VAULT_SECRET_CONFIG",
             "secretId": "ocid1.vaultsecret.oc1.iad.example"}},
         "parentRef": {"parent": ADW[1]}},
    ]
    udf_lib = [{"modelType": "FUNCTION_LIBRARY", "key": "lib-common", "name": "COMMON"}]
    udfs = [{"modelType": "USER_DEFINED_FUNCTION", "key": "udf-fullname", "name": "FULL_NAME",
             "identifier": "FULL_NAME", "parentRef": {"parent": "lib-common"},
             "signatures": [{"modelType": "FUNCTION_SIGNATURE", "key": "sig1",
                             "name": "FULL_NAME", "retType": {"name": "VARCHAR"},
                             "arguments": [{"name": "FIRST"}, {"name": "LAST"}]}],
             "expr": expr("CONCAT(FIRST, ' ', LAST)")}]

    if OUT.exists():
        shutil.rmtree(OUT)

    def put(kind, obj, app=None, name=None):
        d = OUT / kind / app if app else OUT / kind
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{name or obj['key']}.json").write_text(json.dumps(obj, indent=2) + "\n")

    for kind, objs in (("projects", projects), ("folders", folders), ("data_assets", assets),
                       ("connections", connections), ("data_flows", list(flows.values())),
                       ("tasks", tasks), ("pipelines", pipelines),
                       ("function_libraries", udf_lib), ("user_defined_functions", udfs)):
        for o in objs:
            put(kind, o)
    put("applications", {"modelType": "APPLICATION", "key": "app-prod", "name": "APP_PROD",
                         "identifier": "APP_PROD", "lifecycleState": "ACTIVE"})
    for po in published.values():
        put("published_objects", po, "app-prod")
    for s in schedules.values():
        put("schedules", s, "app-prod")
    for t in task_schedules.values():
        put("task_schedules", t, "app-prod")
    for task_key, run in runs.items():
        put("task_runs", run, "app-prod", task_key)
    (OUT / "workspace.json").write_text(json.dumps(workspace, indent=2) + "\n")
    (OUT / "manifest.json").write_text(json.dumps({
        "workspace_name": "SALES_DI", "region": "us-ashburn-1",
        "extracted_at": "2026-10-09T00:00:00Z", "tool": "hand-built fixture",
        "note": "Shapes follow oci.data_integration models 2.165.1; not a real export."},
        indent=2) + "\n")


if __name__ == "__main__":
    build()
    print(f"wrote {OUT}")
