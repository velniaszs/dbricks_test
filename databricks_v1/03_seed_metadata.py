# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Seed the mapping metadata
# MAGIC
# MAGIC Adding a project is **config rows, not new notebooks** -- everything downstream is
# MAGIC generated from `meta.column_catalog`.
# MAGIC
# MAGIC Two releases are seeded so the rebuild demo in notebook 06 has something to prove:
# MAGIC
# MAGIC | release | contains |
# MAGIC |---|---|
# MAGIC | 1 | `object_id`, `level`, `status`, `modified_ts`, `owner` |
# MAGIC | 2 | everything in release 1 **plus** `title` promoted from `payload` |
# MAGIC
# MAGIC Rebuilding at release 1 must reproduce the pre-`title` silver exactly.

# COMMAND ----------

import os
import sys

_here = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path[:0] = [_here, os.getcwd()]
import poc_config as cfg

RELEASES = [
    ("2026-08-01 00:00:00", 1, "initial mapping"),
    ("2026-08-10 00:00:00", 2, "promote title"),
]
RECORDED_R1 = "2026-07-01 00:00:00"
RECORDED_R2 = "2026-08-05 00:00:00"

# COMMAND ----------

spark.sql(f"DELETE FROM {cfg.CATALOG_TABLE}")
spark.sql(f"DELETE FROM {cfg.RELEASE_TABLE}")

release_values = ",\n".join(
    f"({ver}, TIMESTAMP'{ts}', '{desc}', {str(ver == RELEASES[-1][1]).lower()})"
    for ts, ver, desc in RELEASES
)
spark.sql(f"INSERT INTO {cfg.RELEASE_TABLE} VALUES\n{release_values}")

display(spark.table(cfg.RELEASE_TABLE))

# COMMAND ----------

types = {name: (target_type, promoted_in) for name, target_type, _, promoted_in in cfg.GENERIC_COLUMNS}
keys = {name for name, _, is_key, _ in cfg.GENERIC_COLUMNS if is_key}

rows = []
for project_id, conf in cfg.PROJECTS.items():
    for generic_column, source_column in conf["generic"].items():
        target_type, promoted_in = types[generic_column]
        parse_format = conf["ts_format_spark"] if target_type == "TIMESTAMP" else None
        rows.append({
            "project_id": project_id,
            "source_column": source_column,
            "generic_column": generic_column,
            "is_promoted": True,
            "is_business_key": generic_column in keys,
            "precedence": 1,
            "target_type": target_type,
            "parse_format": parse_format,
            "description": f"{project_id} {generic_column}",
            "recorded_at": RECORDED_R1 if promoted_in == 1 else RECORDED_R2,
            "superseded_at": "9999-12-31 00:00:00",
        })
    # Project-unique columns are catalogued for documentation but stay in payload.
    for source_column in conf["unique"]:
        rows.append({
            "project_id": project_id,
            "source_column": source_column,
            "generic_column": None,
            "is_promoted": False,
            "is_business_key": False,
            "precedence": 1,
            "target_type": "STRING",
            "parse_format": None,
            "description": f"{project_id} project-specific",
            "recorded_at": RECORDED_R1,
            "superseded_at": "9999-12-31 00:00:00",
        })

catalog_df = (spark.createDataFrame(rows)
              .selectExpr(
                  "project_id", "source_column", "generic_column", "is_promoted",
                  "is_business_key", "precedence", "target_type", "parse_format",
                  "description",
                  "cast(recorded_at as timestamp) as recorded_at",
                  "cast(superseded_at as timestamp) as superseded_at"))

catalog_df.write.mode("append").saveAsTable(cfg.CATALOG_TABLE)

display(spark.sql(f"""
    SELECT generic_column, project_id, source_column, target_type, parse_format, recorded_at
    FROM {cfg.CATALOG_TABLE}
    WHERE is_promoted
    ORDER BY generic_column, project_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC The same generic column, three different source names and three different timestamp
# MAGIC formats. That is the entire mapping layer -- no value translation anywhere.
