"""Silver build: catalog-driven mapping + SCD2 assignment.

Imported by 04_build_silver and 06_validate_and_rebuild so that the incremental
path and the full-rebuild path run byte-identical code.
"""

import re
import uuid

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

import poc_config as cfg


def load_release(spark: SparkSession, mapping_ver: int) -> list:
    """Catalog rows whose system-time window contains the given release."""
    return spark.sql(f"""
        SELECT c.*
        FROM {cfg.CATALOG_TABLE} c
        JOIN {cfg.RELEASE_TABLE} r ON r.mapping_ver = {int(mapping_ver)}
        WHERE c.recorded_at <= r.released_at
          AND c.superseded_at > r.released_at
    """).collect()


def _variant_path(source_column: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", source_column):
        raise ValueError(
            f"Source column {source_column!r} needs bracket-notation VARIANT paths; "
            "sanitize headers at bronze before mapping it."
        )
    return f"$.{source_column}"


def _source_expr(source_column: str) -> str:
    return f"try_variant_get(payload, '{_variant_path(source_column)}', 'STRING')"


def raw_expr(rows: list) -> str:
    """CASE over project_id -- never a blind coalesce across different projects."""
    by_project: dict[str, list] = {}
    for r in rows:
        by_project.setdefault(r.project_id, []).append(r)

    arms = []
    for project_id in sorted(by_project):
        srcs = [
            _source_expr(r.source_column)
            for r in sorted(by_project[project_id], key=lambda x: x.precedence or 1)
        ]
        # >1 source column within one project = a rename; coalesce resolves each era
        # because payload only ever carries one of the keys per row.
        expr = srcs[0] if len(srcs) == 1 else f"coalesce({', '.join(srcs)})"
        arms.append(f"WHEN '{project_id}' THEN {expr}")
    return f"CASE project_id {' '.join(arms)} END"


def typed_expr(rows: list) -> str:
    raw = raw_expr(rows)
    target_type = (rows[0].target_type or "STRING").upper()
    parse_format = rows[0].parse_format
    if target_type == "STRING":
        return raw
    if target_type == "TIMESTAMP":
        if parse_format:
            # Per-project format, so the CASE has to wrap the parse, not the other way round.
            return _timestamp_case(rows)
        return f"try_cast({raw} AS TIMESTAMP)"
    return f"try_cast({raw} AS {target_type})"


def _timestamp_case(rows: list) -> str:
    by_project: dict[str, list] = {}
    for r in rows:
        by_project.setdefault(r.project_id, []).append(r)

    arms = []
    for project_id in sorted(by_project):
        first = sorted(by_project[project_id], key=lambda x: x.precedence or 1)[0]
        srcs = [
            _source_expr(r.source_column)
            for r in sorted(by_project[project_id], key=lambda x: x.precedence or 1)
        ]
        value = srcs[0] if len(srcs) == 1 else f"coalesce({', '.join(srcs)})"
        arms.append(f"WHEN '{project_id}' THEN try_to_timestamp({value}, '{first.parse_format}')")
    return f"CASE project_id {' '.join(arms)} END"


def build_select(rows: list) -> tuple[list, list]:
    """Returns (select expressions, promoted generic column names)."""
    generic_rows: dict[str, list] = {}
    for r in rows:
        if r.generic_column and r.is_promoted:
            generic_rows.setdefault(r.generic_column, []).append(r)

    ordered = [g for g, _, _, _ in cfg.GENERIC_COLUMNS if g in generic_rows]
    exprs = [f"{typed_expr(generic_rows[g])} AS {g}" for g in ordered]

    key_rows = [r for r in rows if r.is_business_key]
    if not key_rows:
        raise ValueError("No is_business_key rows in the catalog for this release.")
    key_parts = [f"'|' || project_id", f"'|' || {raw_expr(key_rows)}"]
    exprs.append(f"sha2(concat({', '.join(key_parts)}), 256) AS entity_key")
    return exprs, ordered


def deletion_events(staged: DataFrame) -> DataFrame:
    """Synthesise a tombstone per entity that a full load stopped listing.

    A full load is authoritative: absence means deleted. A delta load is not: absence means
    nothing at all. Derived purely from bronze ordering, so a rebuild reproduces it exactly.
    """
    full = staged.where(F.col("_load_mode") == "full")

    # Grain is the file, not the ingest run: one run picks up every pending file, so grouping by
    # _batch_id would fold two full loads into one whose seq_lo predates all history.
    batches = (
        full.groupBy("project_id", "_source_file")
        .agg(
            F.min("_ingest_seq").alias("seq_lo"),
            F.max("_ingest_seq").alias("seq_hi"),
            F.max("_ingest_ts").alias("batch_ts"),
            F.max("_batch_id").alias("del_batch_id"),
        )
        .withColumnRenamed("_source_file", "del_source_file")
    )

    known_before = (
        staged.select("project_id", "entity_key", "_ingest_seq")
        .join(batches, on="project_id")
        .where(F.col("_ingest_seq") < F.col("seq_lo"))
        .groupBy("project_id", "entity_key", "del_source_file", "del_batch_id", "seq_hi", "batch_ts")
        .agg(F.max("_ingest_seq").alias("last_seq"))
    )

    present = full.select(
        "project_id", "entity_key", F.col("_source_file").alias("del_source_file")
    ).distinct()

    missing = known_before.join(
        present, on=["project_id", "entity_key", "del_source_file"], how="left_anti"
    )

    # Carry the last known values forward so the tombstone still shows what was deleted, but
    # take the provenance from the full load that revealed the absence.
    last_row = (staged.withColumnRenamed("_ingest_seq", "last_seq")
                      .drop("_batch_id", "_source_file"))

    return (
        missing.join(last_row, on=["project_id", "entity_key", "last_seq"])
        .withColumn("_ingest_seq", F.col("seq_hi"))
        .withColumn("_ingest_ts", F.col("batch_ts"))
        .withColumn("_row_hash", F.lit(cfg.DELETED_HASH))
        .withColumn("_load_mode", F.lit("full"))
        .withColumnRenamed("del_batch_id", "_batch_id")
        .withColumnRenamed("del_source_file", "_source_file")
        .select(*staged.columns)
    )


def assign_versions(staged: DataFrame) -> DataFrame:
    """Pure window over bronze order -- never reads the existing silver table."""
    w = Window.partitionBy("project_id", "entity_key").orderBy("_ingest_seq")

    events = staged.unionByName(deletion_events(staged))

    # Collapses rows re-delivered unchanged by a full load, and repeated tombstones with them
    # because every tombstone carries the same hash.
    changed = (
        events
        .withColumn("_prev_hash", F.lag("_row_hash").over(w))
        .where(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_row_hash")))
        .drop("_prev_hash")
    )

    deleted = F.col("_row_hash") == F.lit(cfg.DELETED_HASH)

    return (
        changed
        .withColumn("version_no", F.row_number().over(w))
        # A tombstone has no event time of its own, and the carried-forward modified_ts would
        # place it before the version it closes.
        .withColumn(
            "valid_from",
            F.when(deleted, F.col("_ingest_ts")).otherwise(F.coalesce("modified_ts", "_ingest_ts")),
        )
        .withColumn("valid_to", F.coalesce(F.lead("valid_from").over(w), F.lit(cfg.INFINITY).cast("timestamp")))
        # is_current means "latest version AND the entity still exists in the source", so a
        # deleted entity has no current row at all and `WHERE is_current` is the only predicate
        # a consumer needs. The tombstone stays findable via _change_reason.
        .withColumn("is_current", F.lead("_ingest_seq").over(w).isNull() & ~deleted)
        .withColumn(
            "_change_reason",
            F.when(deleted, F.lit("deleted"))
             .when(F.col("version_no") == 1, F.lit("new"))
             .otherwise(F.lit("changed")),
        )
    )


def build_silver(
    spark: SparkSession,
    mapping_ver: int,
    target: str = cfg.SILVER_TABLE,
    from_seq: int = 0,
    key_ver: int = 1,
    build_id: str | None = None,
) -> DataFrame:
    build_id = build_id or str(uuid.uuid4())
    rows = load_release(spark, mapping_ver)
    exprs, generics = build_select(rows)

    src = spark.table(cfg.BRONZE_TABLE).where(F.col("_ingest_seq") > int(from_seq))

    staged = src.selectExpr(
        "project_id", "payload", "_row_hash", "_ingest_seq", "_ingest_ts",
        "_batch_id", "_load_mode", "_source_file", "_schema_ver", "_corrupt_record", *exprs,
    )

    typed = [g for g, t, _, _ in cfg.GENERIC_COLUMNS if g in generics and t != "STRING"]
    cast_failures = F.array_compact(F.array(*[
        F.when(
            F.col(g).isNull() & F.expr(raw_expr([r for r in rows if r.generic_column == g])).isNotNull(),
            F.lit(g),
        )
        for g in typed
    ])) if typed else F.array().cast("array<string>")

    out = (
        assign_versions(staged)
        .withColumn("_cast_failures", cast_failures)
        .withColumn(
            "_dq_status",
            F.when(F.col("_corrupt_record").isNotNull(), F.lit("quarantine"))
             .when(F.col("level").isNull() | F.col("object_id").isNull(), F.lit("quarantine"))
             .when(F.size("_cast_failures") > 0, F.lit("warn"))
             .otherwise(F.lit("ok")),
        )
        .withColumn("_mapping_ver", F.lit(int(mapping_ver)))
        .withColumn("_key_ver", F.lit(int(key_ver)))
        .withColumn("_build_id", F.lit(build_id))
        .withColumn("_committed_at", F.current_timestamp())
    )

    (out.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target))

    spark.sql(f"ALTER TABLE {target} CLUSTER BY (project_id, level, entity_key)")
    spark.sql(f"""
        ALTER TABLE {target} SET TBLPROPERTIES (
          delta.enableChangeDataFeed       = true,
          delta.enableDeletionVectors      = true,
          delta.columnMapping.mode         = 'name',
          delta.dataSkippingNumIndexedCols = 0,
          -- BOOLEAN columns cannot carry skipping stats, so is_current prunes nothing on its own;
          -- pair it with valid_to = '9999-12-31' in queries. _change_reason is the deleted flag.
          delta.dataSkippingStatsColumns   =
            'project_id,level,entity_key,status,valid_from,valid_to,_change_reason,_committed_at,_dq_status'
        )
    """)
    return spark.table(target)
