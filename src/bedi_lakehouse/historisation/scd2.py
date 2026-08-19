"""SCD2 version assignment.

A pure window over bronze order. Nothing here reads the existing silver table, which is what makes
a rebuild reproduce an incremental run exactly. Ported from V1's ``silver_builder.assign_versions``
with ``project_id`` generalised to ``tenant_id``; the version boundaries it produces must stay
byte-identical to V1's, because V1 is the reconciliation baseline.

The same functions version the declarative ``meta`` tables, since those carry the same four
historisation columns.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from bedi_lakehouse.columns import (
    BATCH_ID,
    CHANGE_CHANGED,
    CHANGE_DELETED,
    CHANGE_NEW,
    CHANGE_REASON,
    ENTITY_KEY,
    INFINITY,
    INGEST_SEQ,
    INGEST_TS,
    IS_CURRENT,
    LOAD_MODE,
    ROW_HASH,
    SOURCE_FILE,
    TENANT_ID,
    VALID_FROM,
    VALID_TO,
    VERSION_NO,
)
from bedi_lakehouse.config import LoadMode
from bedi_lakehouse.hashing import DELETED_HASH

_PREV_HASH = "_prev_hash"
_SEQ_LO = "_seq_lo"
_SEQ_HI = "_seq_hi"
_BATCH_TS = "_batch_ts"
_DEL_BATCH_ID = "_del_batch_id"
_DEL_SOURCE_FILE = "_del_source_file"
_LAST_SEQ = "_last_seq"


def _entity_window() -> Window:
    return Window.partitionBy(TENANT_ID, ENTITY_KEY).orderBy(INGEST_SEQ)


def deletion_events(staged: DataFrame) -> DataFrame:
    """Synthesise a tombstone per entity that a full load stopped listing.

    A full load is authoritative: absence means deleted. A delta load is not: absence means nothing
    at all. Derived purely from bronze ordering, so a rebuild reproduces it exactly.

    Args:
        staged: Mapped bronze rows carrying the technical columns and ``entity_key``.

    Returns:
        Tombstone rows with the same columns as ``staged``, hashed as ``DELETED_HASH``.
    """
    full = staged.where(F.col(LOAD_MODE) == F.lit(LoadMode.FULL.value))

    # Grain is the file, not the ingest run: one run picks up every pending file, so grouping by
    # _batch_id would fold two full loads into one whose seq_lo predates all history.
    batches = (
        full.groupBy(TENANT_ID, SOURCE_FILE)
        .agg(
            F.min(INGEST_SEQ).alias(_SEQ_LO),
            F.max(INGEST_SEQ).alias(_SEQ_HI),
            F.max(INGEST_TS).alias(_BATCH_TS),
            F.max(BATCH_ID).alias(_DEL_BATCH_ID),
        )
        .withColumnRenamed(SOURCE_FILE, _DEL_SOURCE_FILE)
    )

    known_before = (
        staged.select(TENANT_ID, ENTITY_KEY, INGEST_SEQ)
        .join(batches, on=TENANT_ID)
        .where(F.col(INGEST_SEQ) < F.col(_SEQ_LO))
        .groupBy(TENANT_ID, ENTITY_KEY, _DEL_SOURCE_FILE, _DEL_BATCH_ID, _SEQ_HI, _BATCH_TS)
        .agg(F.max(INGEST_SEQ).alias(_LAST_SEQ))
    )

    present = full.select(TENANT_ID, ENTITY_KEY, F.col(SOURCE_FILE).alias(_DEL_SOURCE_FILE)).distinct()

    missing = known_before.join(present, on=[TENANT_ID, ENTITY_KEY, _DEL_SOURCE_FILE], how="left_anti")

    # Carry the last known values forward so the tombstone still shows what was deleted, but take
    # the provenance from the full load that revealed the absence.
    last_row = staged.withColumnRenamed(INGEST_SEQ, _LAST_SEQ).drop(BATCH_ID, SOURCE_FILE)

    return (
        missing.join(last_row, on=[TENANT_ID, ENTITY_KEY, _LAST_SEQ])
        .withColumn(INGEST_SEQ, F.col(_SEQ_HI))
        .withColumn(INGEST_TS, F.col(_BATCH_TS))
        .withColumn(ROW_HASH, F.lit(DELETED_HASH))
        .withColumn(LOAD_MODE, F.lit(LoadMode.FULL.value))
        .withColumnRenamed(_DEL_BATCH_ID, BATCH_ID)
        .withColumnRenamed(_DEL_SOURCE_FILE, SOURCE_FILE)
        .select(*staged.columns)
    )


def assign_versions(staged: DataFrame, *, event_time_column: str | None = None) -> DataFrame:
    """Collapse no-op deliveries and mint SCD2 versions over bronze order.

    Args:
        staged: Mapped bronze rows for one stream, including tombstone-eligible full loads.
        event_time_column: Mapped column to use as ``valid_from`` when the source carries a
            modification time. Falls back to ingest time when absent or NULL.

    Returns:
        One row per version with ``version_no``, ``valid_from``, ``valid_to``, ``is_current`` and
        ``_change_reason`` added.
    """
    window = _entity_window()
    events = staged.unionByName(deletion_events(staged))

    # Collapses rows re-delivered unchanged by a full load, and repeated tombstones with them
    # because every tombstone carries the same hash.
    changed = (
        events.withColumn(_PREV_HASH, F.lag(ROW_HASH).over(window))
        .where(F.col(_PREV_HASH).isNull() | (F.col(_PREV_HASH) != F.col(ROW_HASH)))
        .drop(_PREV_HASH)
    )

    deleted = F.col(ROW_HASH) == F.lit(DELETED_HASH)
    observed_at = F.coalesce(F.col(event_time_column), F.col(INGEST_TS)) if event_time_column else F.col(INGEST_TS)

    return (
        changed.withColumn(VERSION_NO, F.row_number().over(window))
        # A tombstone has no event time of its own, and the carried-forward event time would place
        # it before the version it closes.
        .withColumn(VALID_FROM, F.when(deleted, F.col(INGEST_TS)).otherwise(observed_at))
        .withColumn(VALID_TO, F.coalesce(F.lead(VALID_FROM).over(window), F.lit(INFINITY).cast("timestamp")))
        # is_current means "latest version AND still present in source", so a deleted entity has no
        # current row at all and `WHERE is_current` is the only predicate a consumer needs.
        .withColumn(IS_CURRENT, F.lead(INGEST_SEQ).over(window).isNull() & ~deleted)
        .withColumn(
            CHANGE_REASON,
            F.when(deleted, F.lit(CHANGE_DELETED))
            .when(F.col(VERSION_NO) == F.lit(1), F.lit(CHANGE_NEW))
            .otherwise(F.lit(CHANGE_CHANGED)),
        )
    )
