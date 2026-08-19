"""SCD2 semantics, exercised on a real engine.

These are the fixture tests the design calls for: small, hand-built deliveries covering resend,
change, delete and delta-omission. They need a workspace because there is no local Spark, so they
skip when no session is available.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytest

from bedi_lakehouse.columns import (
    CHANGE_CHANGED,
    CHANGE_DELETED,
    CHANGE_NEW,
)
from bedi_lakehouse.hashing import DELETED_HASH
from bedi_lakehouse.historisation.scd2 import assign_versions, deletion_events

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.workspace

STAGED_SCHEMA = (
    "tenant_id STRING, entity_key STRING, _row_hash STRING, _ingest_seq BIGINT, "
    "_ingest_ts TIMESTAMP, _batch_id STRING, _source_file STRING, _load_mode STRING, "
    "status STRING, modified_ts TIMESTAMP"
)

FULL_1 = "full_1.csv"
FULL_2 = "full_2.csv"
DELTA_1 = "delta_1.csv"


def _row(
    entity: str,
    seq: int,
    row_hash: str,
    load_mode: str,
    source_file: str,
    status: str = "ACTIVE",
    modified: datetime | None = None,
) -> tuple[Any, ...]:
    return (
        "FERRARI",
        entity,
        row_hash,
        seq,
        datetime(2026, 1, 1, 12, 0, 0),
        "batch",
        source_file,
        load_mode,
        status,
        modified,
    )


def _stage(spark: SparkSession, rows: list[tuple[Any, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, STAGED_SCHEMA)


def _versions(frame: DataFrame, entity: str) -> list[dict[str, Any]]:
    rows = frame.where(f"entity_key = '{entity}'").orderBy("version_no").collect()
    return [row.asDict() for row in rows]


class TestAssignVersions:
    def test_first_delivery_mints_version_one(self, spark: SparkSession) -> None:
        staged = _stage(spark, [_row("e1", 1, "h1", "full", FULL_1)])
        versions = _versions(assign_versions(staged), "e1")
        assert len(versions) == 1
        assert versions[0]["version_no"] == 1
        assert versions[0]["is_current"] is True
        assert versions[0]["_change_reason"] == CHANGE_NEW

    def test_unchanged_resend_does_not_mint_a_version(self, spark: SparkSession) -> None:
        staged = _stage(
            spark,
            [
                _row("e1", 1, "h1", "full", FULL_1),
                _row("e1", 2, "h1", "full", FULL_2),
            ],
        )
        assert len(_versions(assign_versions(staged), "e1")) == 1

    def test_changed_row_closes_the_previous_version(self, spark: SparkSession) -> None:
        staged = _stage(
            spark,
            [
                _row("e1", 1, "h1", "full", FULL_1),
                _row("e1", 2, "h2", "full", FULL_2, status="INACTIVE"),
            ],
        )
        versions = _versions(assign_versions(staged), "e1")
        assert [v["version_no"] for v in versions] == [1, 2]
        assert [v["is_current"] for v in versions] == [False, True]
        assert versions[0]["valid_to"] == versions[1]["valid_from"]
        assert versions[1]["_change_reason"] == CHANGE_CHANGED

    def test_open_version_runs_to_infinity(self, spark: SparkSession) -> None:
        staged = _stage(spark, [_row("e1", 1, "h1", "full", FULL_1)])
        assert _versions(assign_versions(staged), "e1")[0]["valid_to"] == datetime(9999, 12, 31, 0, 0, 0)

    def test_event_time_becomes_valid_from_when_declared(self, spark: SparkSession) -> None:
        modified = datetime(2025, 6, 1, 8, 30, 0)
        staged = _stage(spark, [_row("e1", 1, "h1", "full", FULL_1, modified=modified)])
        versions = _versions(assign_versions(staged, event_time_column="modified_ts"), "e1")
        assert versions[0]["valid_from"] == modified

    def test_event_time_falls_back_to_ingest_time_when_null(self, spark: SparkSession) -> None:
        staged = _stage(spark, [_row("e1", 1, "h1", "full", FULL_1, modified=None)])
        versions = _versions(assign_versions(staged, event_time_column="modified_ts"), "e1")
        assert versions[0]["valid_from"] == datetime(2026, 1, 1, 12, 0, 0)


class TestDeletion:
    def test_absence_from_a_later_full_load_mints_a_tombstone(self, spark: SparkSession) -> None:
        staged = _stage(
            spark,
            [
                _row("e1", 1, "h1", "full", FULL_1),
                _row("e2", 2, "h2", "full", FULL_1),
                _row("e1", 3, "h1", "full", FULL_2),
            ],
        )
        versions = _versions(assign_versions(staged), "e2")
        assert [v["_change_reason"] for v in versions] == [CHANGE_NEW, CHANGE_DELETED]
        assert versions[-1]["_row_hash"] == DELETED_HASH

    def test_a_deleted_entity_has_no_current_row(self, spark: SparkSession) -> None:
        staged = _stage(
            spark,
            [
                _row("e1", 1, "h1", "full", FULL_1),
                _row("e2", 2, "h2", "full", FULL_1),
                _row("e1", 3, "h1", "full", FULL_2),
            ],
        )
        versions = _versions(assign_versions(staged), "e2")
        assert not any(v["is_current"] for v in versions)

    def test_delta_omission_is_not_a_deletion(self, spark: SparkSession) -> None:
        staged = _stage(
            spark,
            [
                _row("e1", 1, "h1", "full", FULL_1),
                _row("e2", 2, "h2", "full", FULL_1),
                _row("e1", 3, "h1", "delta", DELTA_1),
            ],
        )
        versions = _versions(assign_versions(staged), "e2")
        assert len(versions) == 1
        assert versions[0]["is_current"] is True

    def test_no_tombstone_without_a_full_load(self, spark: SparkSession) -> None:
        staged = _stage(spark, [_row("e1", 1, "h1", "delta", DELTA_1)])
        assert deletion_events(staged).count() == 0

    def test_tombstone_is_not_repeated_by_later_full_loads(self, spark: SparkSession) -> None:
        staged = _stage(
            spark,
            [
                _row("e1", 1, "h1", "full", FULL_1),
                _row("e2", 2, "h2", "full", FULL_1),
                _row("e1", 3, "h1", "full", FULL_2),
                _row("e1", 4, "h1", "full", "full_3.csv"),
            ],
        )
        versions = _versions(assign_versions(staged), "e2")
        assert sum(1 for v in versions if v["_change_reason"] == CHANGE_DELETED) == 1
