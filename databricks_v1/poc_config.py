"""Shared configuration for the Bosch / AAS Doors PoC.

Imported by the notebooks in this folder. Not a notebook itself.
"""

CATALOG = "bosch_poc"
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
META = "meta"
VOLUME = "landing"

BRONZE_TABLE = f"{CATALOG}.{BRONZE}.aas_doors_raw"
SILVER_TABLE = f"{CATALOG}.{SILVER}.entities"
CATALOG_TABLE = f"{CATALOG}.{META}.column_catalog"
RELEASE_TABLE = f"{CATALOG}.{META}.mapping_release"
REGISTRY_TABLE = f"{CATALOG}.{META}.schema_registry"
STATE_TABLE = f"{CATALOG}.{META}.project_state"

LANDING_ROOT = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}/aas_doors"

# Delivery contract: <project>/<full|delta>/<anything>.csv. The folder is the only signal for
# load mode -- file names carry no timestamp and are treated as opaque.
LOAD_MODES = ("full", "delta")

# Rows a delta file could not parse are captured here rather than dropped; full loads use
# FAILFAST instead, because a half-parsed full load is indistinguishable from a mass deletion.
CORRUPT_COLUMN = "_corrupt_record"

# Sentinel for open-ended validity ranges.
INFINITY = "9999-12-31 00:00:00"

# Generic columns: (name, target_type, is_business_key, promoted_in_release)
# `title` only becomes a real column in release 2 -- used to demonstrate rebuilding
# silver with the mapping that was in force at an earlier point in time.
GENERIC_COLUMNS = [
    ("object_id", "STRING", True, 1),
    ("level", "STRING", False, 1),
    ("status", "STRING", False, 1),
    ("modified_ts", "TIMESTAMP", False, 1),
    ("owner", "STRING", False, 1),
    ("title", "STRING", False, 2),
]

# Each project spells the same concepts differently -- both in column names and in
# the values themselves. Value harmonisation is deliberately out of scope, so the
# differing vocabularies below survive all the way into gold.
PROJECTS = {
    "FERRARI": {
        "generic": {
            "object_id": "Object_ID_Ferrari",
            "level": "Level_Ferrari",
            "status": "Status_Ferrari",
            "modified_ts": "Last_Modified_Ferrari",
            "owner": "Owner_Fer",
            "title": "Object_Text_Fer",
        },
        "unique": ["Fuel_Load_Ferrari", "Brake_Temp_FL_Fer", "Downforce_Idx"],
        "levels": ["System", "Subsystem", "Component"],
        "statuses": ["ACTIVE", "INACTIVE", "PENDING"],
        "ts_format_spark": "dd/MM/yyyy HH:mm:ss",
        "ts_format_py": "%d/%m/%Y %H:%M:%S",
        "owners": ["m.schumacher", "c.leclerc", "j.todt"],
    },
    "MCLAREN": {
        "generic": {
            "object_id": "ObjID_Mc",
            "level": "Lvl_Mc",
            "status": "Status_Mclaren",
            "modified_ts": "Modified_Mc",
            "owner": "Owner_Mc",
            "title": "ObjText_Mc",
        },
        "unique": ["FuelKg_Mc", "TyreComp_Mc"],
        "levels": ["SYS", "SUBSYS", "COMP"],
        "statuses": ["A", "I", "P"],
        "ts_format_spark": "yyyy-MM-dd HH:mm:ss",
        "ts_format_py": "%Y-%m-%d %H:%M:%S",
        "owners": ["l.norris", "z.brown", "a.stella"],
    },
    "ALPINE": {
        "generic": {
            "object_id": "Req_Id_Alp",
            "level": "Hierarchy_Level_Alp",
            "status": "State_Alp",
            "modified_ts": "Changed_On_Alp",
            "owner": "Responsible_Alp",
            "title": "Requirement_Text_Alp",
        },
        "unique": ["Torque_Nm_Alp", "Cooling_Idx_Alp"],
        "levels": ["L1", "L2", "L3"],
        "statuses": ["1", "0", "2"],
        "ts_format_spark": "yyyyMMdd HH:mm",
        "ts_format_py": "%Y%m%d %H:%M",
        "owners": ["e.ocon", "p.gasly", "o.oakes"],
    },
}

# One file per project per delivery, each tagged full or delta. A full load is authoritative
# (absence means deleted); a delta load says nothing about the entities it omits.
EXTRACTS = [
    ("2026-01-05", "full"),
    ("2026-02-05", "delta"),
    ("2026-03-05", "full"),
]
EXTRACT_DATES = [d for d, _ in EXTRACTS]

# Objects dropped from the final full load, per project, to exercise delete detection.
DELETED_OBJECTS = 5

# AAS Doors deltas are not minimal -- they resend rows that did not change (confirmed 2026-08-14).
# The generator mixes this many untouched rows into every delta so the collapse is actually tested.
DELTA_NOOP_RATE = 0.30

DELETED_HASH = "__deleted__"

# Release 3 adds a column to ALPINE only, to exercise schema evolution.
NEW_COLUMN_PROJECT = "ALPINE"
NEW_COLUMN_NAME = "Safety_Class_Alp"


def full_name(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}"


def landing_dir(project_id: str, load_mode: str) -> str:
    return f"{LANDING_ROOT}/{project_id}/{load_mode}"
