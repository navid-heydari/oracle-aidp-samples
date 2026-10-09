"""How AIDP reaches each OCI-DI data asset.

Every source a migrated notebook reads is addressed through a notebook
parameter (``SRC_<ASSET>``) whose default comes from ``ocidi-config`` or, when
the config is silent, from a derived name. The notebook never holds a JDBC
URL or a credential; ``REVIEW.md`` lists what has to exist on AIDP for each
parameter's default to resolve.

AIDP external-catalog source types (``aidp catalog create`` allowed values,
CLI 4.2.1): ADW, ALH, KAFKA, ATP, ORACLE, EXADATA, MYSQL, AZURE_SQLSERVER,
SNOWFLAKE, DB2, ORACLE_ANALYTICS.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..naming import ident

# DI data asset modelType -> AIDP external catalog sourceType
_CATALOG = {
    "ORACLE_ADWC_DATA_ASSET": "ADW",
    "ORACLE_ATP_DATA_ASSET": "ATP",
    "ORACLE_DATA_ASSET": "ORACLE",
    "ORACLE_PEOPLESOFT_DATA_ASSET": "ORACLE",
    "ORACLE_SIEBEL_DATA_ASSET": "ORACLE",
    "ORACLE_EBS_DATA_ASSET": "ORACLE",
    "MYSQL_DATA_ASSET": "MYSQL",
    "MYSQL_HEATWAVE_DATA_ASSET": "MYSQL",
}
# GENERIC_JDBC_DATA_ASSET.dataAssetType -> AIDP external catalog sourceType
_JDBC_CATALOG = {
    "SNOWFLAKE": "SNOWFLAKE",
    "DB2": "DB2", "IBM_DB2": "DB2",
    "AZURE_SQL": "AZURE_SQLSERVER", "MICROSOFT_AZURE_SQL_DATABASE": "AZURE_SQLSERVER",
    "AZURE_SQL_DATABASE": "AZURE_SQLSERVER",
    "AUTONOMOUS_AI_LAKEHOUSE": "ALH", "ALH": "ALH",
}
_JDBC_DRIVERS = {
    "POSTGRESQL": ("org.postgresql.Driver", "aidp-postgresql"),
    "SQLSERVER": ("com.microsoft.sqlserver.jdbc.SQLServerDriver", "aidp-sqlserver"),
    "MICROSOFT_SQLSERVER": ("com.microsoft.sqlserver.jdbc.SQLServerDriver", "aidp-sqlserver"),
    "AMAZON_RDS_SQLSERVER": ("com.microsoft.sqlserver.jdbc.SQLServerDriver", "aidp-sqlserver"),
    "AMAZON_REDSHIFT": ("com.amazon.redshift.jdbc42.Driver", "aidp-jdbc-custom"),
    "AMAZON_AURORA": ("org.postgresql.Driver", "aidp-postgresql"),
    "HIVE": ("org.apache.hive.jdbc.HiveDriver", "aidp-hive"),
    "INFLUXDB": ("", "aidp-jdbc-custom"),
    "SALESFORCE": ("", "aidp-salesforce"),
    "AZURE_SYNAPSE": ("com.microsoft.sqlserver.jdbc.SQLServerDriver", "aidp-azuresql"),
}


@dataclass
class SourceAccess:
    data_asset: str            # DI data asset name
    asset_type: str            # DI modelType (+ "/" + dataAssetType for generic JDBC)
    kind: str                  # catalog | path | jdbc | none
    parameter: str             # notebook parameter name, e.g. SRC_ADW_SALES
    default: str               # its default value
    action: str                # what must exist on AIDP
    severity: str = "info"     # finding severity for the source
    driver: str = ""

    def to_json(self) -> dict:
        return {"data_asset": self.data_asset, "asset_type": self.asset_type, "access": self.kind,
                "parameter": self.parameter, "default": self.default, "action": self.action}


def param_name(asset_name: str) -> str:
    return "SRC_" + ident(asset_name).upper()


def resolve(asset: dict, overrides: dict) -> SourceAccess:
    """Pick the AIDP access path for one DI data asset."""
    name = asset.get("name") or asset.get("identifier") or asset.get("key") or "asset"
    kind = asset.get("modelType", "")
    sub = (asset.get("dataAssetType") or (asset.get("assetProperties") or {}).get("dataAssetType")
           or "").upper()
    shown = f"{kind}/{sub}" if sub else kind
    pname = param_name(name)
    override = overrides.get(name) or overrides.get(asset.get("key") or "") or {}

    if override:
        access = override.get("access", "catalog")
        value = override.get("value", "")
        action = {"catalog": f"AIDP catalog `{value}` must expose this asset's schemas",
                  "path": f"cluster must be able to read `{value}`",
                  "jdbc": f"AIDP credential `{value}` must hold keys url, user, password"}.get(
                      access, "configured in ocidi-config")
        return SourceAccess(name, shown, access, pname, value, action + " (from ocidi-config)")

    if kind in _CATALOG or (kind == "GENERIC_JDBC_DATA_ASSET" and sub in _JDBC_CATALOG):
        stype = _CATALOG.get(kind) or _JDBC_CATALOG[sub]
        default = ident(name)
        return SourceAccess(name, shown, "catalog", pname, default,
                            f"register an AIDP EXTERNAL catalog `{default}` (sourceType {stype}) "
                            f"for this asset, or map it in ocidi-config `sources`")
    if kind == "ORACLE_OBJECT_STORAGE_DATA_ASSET":
        ns = asset.get("namespace") or "<namespace>"
        severity = "info" if asset.get("namespace") else "review"
        return SourceAccess(name, shown, "path", pname, f"oci://{{bucket}}@{ns}/",
                            "the cluster's principal needs read on the buckets", severity)
    if kind == "AMAZON_S3_DATA_ASSET":
        return SourceAccess(name, shown, "path", pname, "s3a://{bucket}/",
                            "configure S3A credentials on the cluster (aidp-aws-s3 skill)", "review")
    if kind == "HDFS_DATA_ASSET":
        return SourceAccess(name, shown, "path", pname, "hdfs:///{bucket}/",
                            "copy the data to Object Storage, or make the HDFS cluster reachable",
                            "review")
    if kind == "LAKE_DATA_ASSET":
        default = ident(name)
        return SourceAccess(name, shown, "catalog", pname, default,
                            f"expose the OCI Data Lake tables as AIDP catalog `{default}`", "review")
    if kind == "GENERIC_JDBC_DATA_ASSET":
        driver, skill = _JDBC_DRIVERS.get(sub, ("", "aidp-jdbc-custom"))
        cred = ident(name)
        return SourceAccess(name, shown, "jdbc", pname, cred,
                            f"create AIDP credential `{cred}` with keys url, user, password and "
                            f"install the JDBC driver (see the {skill} connector skill)", "review",
                            driver)
    if kind == "FUSION_APP_DATA_ASSET":
        return SourceAccess(name, shown, "none", pname, "",
                            "land the BICC/BIP extracts on AIDP first (aidp-fusion-bicc connector "
                            "or fusion-autopilot), then map the asset to that catalog in "
                            "ocidi-config `sources`", "manual")
    if kind == "REST_DATA_ASSET":
        return SourceAccess(name, shown, "none", pname, "",
                            "REST sources have no catalog form; rebuild with the aidp-rest-generic "
                            "connector", "manual")
    return SourceAccess(name, shown, "none", pname, "",
                        f"no known AIDP access path for {shown}; map it in ocidi-config `sources`",
                        "manual")


def asset_from_ref(snapshot, ref) -> Optional[dict]:
    if ref is None:
        return None
    hit = snapshot.data_asset_for(ref.key) if ref.key else None
    if hit is None and ref.name:
        hit = snapshot.data_asset_for(ref.name)
    return hit or (ref.raw if ref.raw.get("modelType") else None)
