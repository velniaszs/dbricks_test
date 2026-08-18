# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze ingest
# MAGIC
# MAGIC **One job, one writer, every pending file.** Projects deliver whenever they like and in any
# MAGIC order, so rather than racing per-project jobs against each other this notebook picks up
# MAGIC everything sitting in the landing volume and processes it in a single deterministic pass.
# MAGIC That is what makes a single globally monotonic `_ingest_seq` safe.
# MAGIC
# MAGIC Key invariants:
# MAGIC
# MAGIC - **append-only**, never updated or deleted from,
# MAGIC - `_row_hash` is a hash of **all** normalised source values -- catalog-free, so no
# MAGIC   modelling decision can ever move an SCD2 version boundary,
# MAGIC - `_load_mode` (`full` / `delta`) comes from the **folder** the file arrived in, and is what
# MAGIC   lets silver treat a missing entity as a deletion in one case and as "unchanged" in the
# MAGIC   other,
# MAGIC - `_ingest_seq` is assigned once at write, ordered by file modification time, and is the
# MAGIC   only ordering key downstream,
# MAGIC - files already ingested are skipped, so re-running is safe.
# MAGIC
# MAGIC Why a global `_ingest_seq` rather than one per project: a per-project counter is not
# MAGIC comparable across projects, so any Parquet file holding rows from two projects would carry
# MAGIC a meaningless min/max range and stop Delta skipping it. A global counter rises with wall
# MAGIC clock, so old files always prune regardless of which projects they contain.

# COMMAND ----------

import hashlib
import os
import sys
import uuid
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F

_here = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path[:0] = [_here, os.getcwd()]
import poc_config as cfg

NULL_TOKENS = ["", "NULL", "N/A", "NA", "-", "#N/A", "(NULL)", "NONE"]
BATCH_ID = str(uuid.uuid4())
CORRUPT = cfg.CORRUPT_COLUMN

# COMMAND ----------


def norm(column):
    """Normalise for hashing only -- the stored payload keeps the raw value."""
    value = F.trim(F.col(f"`{column}`"))
    return F.when(F.upper(value).isin(NULL_TOKENS), F.lit(None)).otherwise(value)


def canon(columns):
    """Sorted keys, so column reordering in the source cannot change the hash."""
    return F.to_json(F.struct(*[
        F.coalesce(norm(c), F.lit("\x00")).alias(c) for c in sorted(columns)
    ]))


def list_landing_files():
    """Every CSV under the landing root, as `<project>/<full|delta>/<file>.csv`.

    The folder is the load-mode contract. A folder that is neither `full` nor `delta` is an
    error rather than a guess: mis-tagging a delta as a full load tombstones every entity the
    delta omits, which is most of them.
    """
    found = []
    for project_dir in dbutils.fs.ls(cfg.LANDING_ROOT):
        project_id = project_dir.name.rstrip("/")
        for mode_dir in dbutils.fs.ls(project_dir.path):
            load_mode = mode_dir.name.rstrip("/")
            if load_mode not in cfg.LOAD_MODES:
                raise ValueError(
                    f"{mode_dir.path}: expected a {cfg.LOAD_MODES} sub-folder, got {load_mode!r}"
                )
            for entry in dbutils.fs.ls(mode_dir.path):
                if entry.name.endswith(".csv"):
                    found.append({
                        "project_id": project_id,
                        "load_mode": load_mode,
                        "path": entry.path,
                        "modified": datetime.fromtimestamp(entry.modificationTime / 1000),
                    })

    # A tie within one project makes the path the de-facto ordering key, which has nothing to do
    # with delivery order -- and `delta/` sorts before `full/`, so the chain would be built
    # backwards and unchanged resends would collapse the wrong way round. Ties across projects
    # are harmless; entities never span projects.
    seen = {}
    for f in found:
        key = (f["project_id"], f["modified"])
        if key in seen:
            raise ValueError(
                f"{f['project_id']}: {seen[key]} and {f['path']} share a modification timestamp "
                f"({f['modified']}). File order is the only ordering signal available."
            )
        seen[key] = f["path"]

    # Arrival order is what _ingest_seq encodes, so it has to be total and reproducible. The
    # path tie-break only ever resolves cross-project ties now, which are arbitrary by nature.
    return sorted(found, key=lambda f: (f["modified"], f["path"]))


def read_one(entry):
    """One file at a time -- headers differ per project and drift over time, and a shared read
    would match later files positionally against the first file's header."""
    header_schema = (spark.read
                     .option("header", "true")
                     .option("inferSchema", "false")   # every source column stays a string
                     .option("encoding", "UTF-8")
                     .csv(entry["path"]).schema)
    source_columns = [field.name for field in header_schema.fields]

    reader = (spark.read
              .option("header", "true")
              .option("encoding", "UTF-8"))

    if entry["load_mode"] == "full":
        # A half-parsed full load is indistinguishable from a mass deletion, so refuse it.
        df = (reader.schema(header_schema)
                    .option("mode", "FAILFAST")
                    .csv(entry["path"])
                    .withColumn(CORRUPT, F.lit(None).cast("string")))
    else:
        # A delta only ever misses updates, so bad rows are quarantined rather than fatal.
        # StructType.add mutates in place, so source_columns must be captured before this.
        df = (reader.schema(header_schema.add(CORRUPT, "string"))
                    .option("mode", "PERMISSIVE")
                    .option("columnNameOfCorruptRecord", CORRUPT)
                    .csv(entry["path"]))

    return df, source_columns


# COMMAND ----------

already_ingested = set()
if spark.catalog.tableExists(cfg.BRONZE_TABLE):
    already_ingested = {
        r._source_file
        for r in spark.table(cfg.BRONZE_TABLE).select("_source_file").distinct().collect()
    }

pending = [f for f in list_landing_files() if f["path"] not in already_ingested]

frames = []
registry_rows = []

for file_order, entry in enumerate(pending):
    raw, source_columns = read_one(entry)
    schema_ver = hashlib.sha256("|".join(sorted(source_columns)).encode("utf-8")).hexdigest()
    registry_rows.append((
        entry["project_id"], schema_ver, sorted(source_columns),
        len(source_columns), entry["modified"], entry["path"],
    ))

    frames.append(
        raw.select(
            F.lit(entry["project_id"]).alias("project_id"),
            F.lit("AAS_DOORS").alias("source_system"),
            F.lit(entry["path"]).alias("_source_file"),
            F.lit(entry["modified"]).cast("timestamp").alias("_file_modified"),
            F.lit(entry["load_mode"]).alias("_load_mode"),
            F.lit(file_order).alias("_file_order"),
            F.parse_json(F.to_json(F.struct(*[F.col(f"`{c}`") for c in source_columns]))).alias("payload"),
            F.sha2(canon(source_columns), 256).alias("_row_hash"),
            F.lit(schema_ver).alias("_schema_ver"),
            F.col(CORRUPT).alias("_corrupt_record"),
        )
    )

for f in pending:
    print(f"{f['project_id']:8} {f['load_mode']:5} {f['modified']:%Y-%m-%d %H:%M:%S}  {f['path']}")

# COMMAND ----------

if not frames:
    print("nothing new -- all files already ingested")
else:
    staged = frames[0]
    for frame in frames[1:]:
        staged = staged.unionByName(frame)

    base_seq = spark.sql(
        f"SELECT coalesce(max(_ingest_seq), 0) AS s FROM {cfg.BRONZE_TABLE}"
    ).collect()[0]["s"]

    # _ingest_seq is assigned once, here, and never recomputed -- that is what makes the
    # downstream SCD2 timeline reproducible on a rebuild years later. A single unpartitioned
    # window is deliberate: it is the only way to get one global order, and at delivery
    # volumes (hundreds of thousands of rows) the sort is cheap.
    in_file = Window.partitionBy("_source_file").orderBy("_row_hash")
    global_order = Window.orderBy("_file_order", "_file_row_num")

    to_write = (staged
                .withColumn("_file_row_num", F.row_number().over(in_file).cast("bigint"))
                .withColumn("_ingest_seq", (F.lit(base_seq) + F.row_number().over(global_order)).cast("bigint"))
                .withColumn("_ingest_ts", F.col("_file_modified"))
                .withColumn("_batch_id", F.lit(BATCH_ID))
                .select("project_id", "source_system", "_ingest_ts", "_ingest_seq", "_batch_id",
                        "_source_file", "_file_modified", "_file_row_num", "_load_mode",
                        "_row_hash", "payload", "_schema_ver", "_corrupt_record"))

    to_write.write.mode("append").saveAsTable(cfg.BRONZE_TABLE)
    print(f"appended batch {BATCH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema registry
# MAGIC
# MAGIC `_schema_ver` on its own is an opaque hash -- it tells you *that* a project's column set
# MAGIC changed, not *what* changed. The registry stores the header behind each hash, so a change
# MAGIC is a diff between two rows.

# COMMAND ----------

if registry_rows:
    incoming = (spark.createDataFrame(
        registry_rows,
        "project_id string, schema_ver string, columns array<string>, "
        "column_count int, seen timestamp, source_file string",
    ).groupBy("project_id", "schema_ver").agg(
        F.first("columns").alias("columns"),
        F.first("column_count").alias("column_count"),
        F.min("seen").alias("first_seen"),
        F.max("seen").alias("last_seen"),
        F.first("source_file").alias("first_file"),
    ))
    incoming.createOrReplaceTempView("_incoming_schema")

    spark.sql(f"""
        MERGE INTO {cfg.REGISTRY_TABLE} t
        USING _incoming_schema s
          ON t.project_id = s.project_id AND t.schema_ver = s.schema_ver
        WHEN MATCHED THEN UPDATE SET t.last_seen = greatest(t.last_seen, s.last_seen)
        WHEN NOT MATCHED THEN INSERT *
    """)

display(spark.sql(f"""
    SELECT project_id, left(schema_ver, 12) AS schema_ver, column_count, first_seen, last_seen
    FROM {cfg.REGISTRY_TABLE}
    ORDER BY project_id, first_seen
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ALPINE has two schema versions -- the last extract gained a column. No `ALTER TABLE` was
# MAGIC needed anywhere, because the payload is self-describing per row. The registry makes the
# MAGIC difference explicit:

# COMMAND ----------

display(spark.sql(f"""
    WITH versions AS (
      SELECT project_id, columns,
             row_number() OVER (PARTITION BY project_id ORDER BY first_seen) AS n
      FROM {cfg.REGISTRY_TABLE} WHERE project_id = '{cfg.NEW_COLUMN_PROJECT}'
    )
    SELECT array_except(b.columns, a.columns) AS added,
           array_except(a.columns, b.columns) AS removed
    FROM versions a JOIN versions b ON b.n = a.n + 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-project cursors
# MAGIC
# MAGIC Ingest advances `last_ingest_seq`; the silver build advances `last_built_seq`. Two
# MAGIC independent cursors, so a failed build never blocks ingest and a project that falls
# MAGIC behind does not hold up the other nineteen.

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {cfg.STATE_TABLE} t
    USING (
      SELECT project_id,
             max(_ingest_seq) AS last_ingest_seq,
             max(_ingest_ts)  AS last_ingest_ts
      FROM {cfg.BRONZE_TABLE}
      WHERE _batch_id = '{BATCH_ID}'
      GROUP BY project_id
    ) s ON t.project_id = s.project_id
    WHEN MATCHED THEN UPDATE SET
      t.last_ingest_seq = s.last_ingest_seq,
      t.last_ingest_ts  = s.last_ingest_ts
    WHEN NOT MATCHED THEN INSERT (project_id, last_ingest_seq, last_built_seq, last_ingest_ts)
      VALUES (s.project_id, s.last_ingest_seq, 0, s.last_ingest_ts)
""")

display(spark.table(cfg.STATE_TABLE).orderBy("project_id"))

# COMMAND ----------

display(spark.sql(f"""
    SELECT project_id, _load_mode, left(_schema_ver, 12) AS schema_ver, count(*) AS rows,
           count(DISTINCT _source_file) AS files,
           count(DISTINCT _row_hash)    AS distinct_rows,
           count_if(_corrupt_record IS NOT NULL) AS unparsed_rows,
           min(_ingest_seq) AS seq_from, max(_ingest_seq) AS seq_to
    FROM {cfg.BRONZE_TABLE}
    GROUP BY ALL
    ORDER BY project_id, seq_from
"""))

