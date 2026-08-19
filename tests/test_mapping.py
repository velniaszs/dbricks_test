"""Mapping expression generation, then the same SQL executed on a real engine."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from bedi_lakehouse.config import ColumnMappingRow, ConfigError, Grain
from bedi_lakehouse.mapping import build_select, entity_key_expr, raw_expr, source_expr, typed_expr

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

GRAIN = Grain(business_key=("door_id",), tenant_column="tenant_id")


def _row(
    tenant: str,
    source_column: str,
    generic_column: str,
    target_type: str = "STRING",
    parse_format: str | None = None,
    precedence: int = 1,
    is_business_key: bool = False,
    is_promoted: bool = True,
) -> ColumnMappingRow:
    return ColumnMappingRow(
        tenant=tenant,
        source_column=source_column,
        generic_column=generic_column,
        target_type=target_type,
        parse_format=parse_format,
        precedence=precedence,
        is_business_key=is_business_key,
        is_promoted=is_promoted,
    )


DOOR_ID_ROWS = [
    _row("FERRARI", "DOOR_NO", "door_id", precedence=1, is_business_key=True),
    _row("FERRARI", "DOOR_NUMBER", "door_id", precedence=2, is_business_key=True),
    _row("PORSCHE", "ID", "door_id", precedence=1, is_business_key=True),
]
MODIFIED_ROWS = [
    _row("FERRARI", "CHANGED_ON", "modified_ts", "TIMESTAMP", "dd.MM.yyyy"),
    _row("PORSCHE", "CHANGED_ON", "modified_ts", "TIMESTAMP", "yyyy-MM-dd"),
]


class TestSourceExpr:
    def test_reads_the_payload_by_name(self) -> None:
        assert source_expr("DOOR_NO") == "try_variant_get(payload, '$.DOOR_NO', 'STRING')"

    def test_rejects_a_column_needing_bracket_notation(self) -> None:
        with pytest.raises(ConfigError, match="bracket-notation"):
            source_expr("door no")


class TestRawExpr:
    def test_one_arm_per_tenant(self) -> None:
        expr = raw_expr(DOOR_ID_ROWS)
        assert expr.count("WHEN") == 2
        assert expr.startswith("CASE `tenant_id` ")

    def test_a_rename_becomes_a_coalesce_in_precedence_order(self) -> None:
        expr = raw_expr(DOOR_ID_ROWS)
        assert "coalesce(try_variant_get(payload, '$.DOOR_NO', 'STRING')" in expr
        assert expr.index("$.DOOR_NO") < expr.index("$.DOOR_NUMBER")

    def test_tenants_are_never_coalesced_together(self) -> None:
        expr = raw_expr(DOOR_ID_ROWS)
        assert "coalesce(try_variant_get(payload, '$.ID'" not in expr

    def test_the_tenant_value_is_quoted_not_interpolated(self) -> None:
        expr = raw_expr([_row("O'BRIEN", "ID", "door_id")])
        assert r"WHEN 'O\'BRIEN' THEN" in expr

    def test_rejects_an_empty_row_set(self) -> None:
        with pytest.raises(ConfigError, match="empty mapping"):
            raw_expr([])


class TestTypedExpr:
    def test_string_needs_no_cast(self) -> None:
        assert typed_expr(DOOR_ID_ROWS) == raw_expr(DOOR_ID_ROWS)

    def test_each_tenant_parses_with_its_own_format(self) -> None:
        expr = typed_expr(MODIFIED_ROWS)
        assert "try_to_timestamp(try_variant_get(payload, '$.CHANGED_ON', 'STRING'), 'dd.MM.yyyy')" in expr
        assert "'yyyy-MM-dd'" in expr

    def test_a_timestamp_without_a_format_falls_back_to_cast(self) -> None:
        expr = typed_expr([_row("FERRARI", "CHANGED_ON", "modified_ts", "TIMESTAMP")])
        assert "try_cast(try_variant_get(payload, '$.CHANGED_ON', 'STRING') AS TIMESTAMP)" in expr

    def test_other_types_use_try_cast(self) -> None:
        assert "AS DECIMAL(10, 2))" in typed_expr([_row("FERRARI", "WIDTH", "width", "DECIMAL(10, 2)")])

    def test_rejects_conflicting_types_for_one_generic_column(self) -> None:
        rows = [_row("FERRARI", "W", "width", "INT"), _row("PORSCHE", "W", "width", "DOUBLE")]
        with pytest.raises(ConfigError, match="conflicting types"):
            typed_expr(rows)

    def test_rejects_a_target_type_that_is_not_a_type(self) -> None:
        with pytest.raises(ConfigError, match="Unsupported target_type"):
            typed_expr([_row("FERRARI", "W", "width", "INT; DROP TABLE t")])


class TestEntityKey:
    def test_includes_the_tenant_so_keys_cannot_collide_across_tenants(self) -> None:
        assert "'|' || `tenant_id`" in entity_key_expr(DOOR_ID_ROWS, GRAIN)

    def test_rejects_a_grain_the_mapping_does_not_flag(self) -> None:
        grain = Grain(business_key=("door_id", "revision"), tenant_column="tenant_id")
        with pytest.raises(ConfigError, match="Business key disagrees"):
            entity_key_expr(DOOR_ID_ROWS, grain)

    def test_follows_the_declared_key_order(self) -> None:
        rows = [*DOOR_ID_ROWS, _row("FERRARI", "REV", "revision", is_business_key=True)]
        grain = Grain(business_key=("revision", "door_id"), tenant_column="tenant_id")
        expr = entity_key_expr([r for r in rows if r.tenant == "FERRARI"], grain)
        assert expr.index("$.REV") < expr.index("$.DOOR_NO")


class TestBuildSelect:
    def test_generic_columns_are_alphabetical(self) -> None:
        plan = build_select([*DOOR_ID_ROWS, *MODIFIED_ROWS], GRAIN)
        assert plan.generic_columns == ("door_id", "modified_ts")

    def test_entity_key_is_projected_last(self) -> None:
        plan = build_select([*DOOR_ID_ROWS, *MODIFIED_ROWS], GRAIN)
        assert plan.expressions[-1].endswith("AS `entity_key`")

    def test_unpromoted_columns_are_not_projected(self) -> None:
        rows = [*DOOR_ID_ROWS, _row("FERRARI", "NOTE", "note", is_promoted=False)]
        assert build_select(rows, GRAIN).generic_columns == ("door_id",)


@pytest.mark.workspace
class TestAgainstSpark:
    """The generated SQL has to survive contact with a real VARIANT payload."""

    @pytest.fixture(scope="class")
    def mapped(self, spark: SparkSession) -> list[dict]:
        payloads = [
            ("FERRARI", '{"DOOR_NO":"D1","CHANGED_ON":"05.03.2026"}'),
            ("FERRARI", '{"DOOR_NUMBER":"D2","CHANGED_ON":"06.03.2026"}'),
            ("PORSCHE", '{"ID":"D1","CHANGED_ON":"2026-03-05"}'),
            ("FERRARI", '{"CHANGED_ON":"07.03.2026"}'),
        ]
        bronze = spark.createDataFrame(payloads, "tenant_id STRING, json STRING").selectExpr(
            "tenant_id", "parse_json(json) AS payload"
        )
        plan = build_select([*DOOR_ID_ROWS, *MODIFIED_ROWS], GRAIN)
        rows = bronze.selectExpr("tenant_id", *plan.expressions).collect()
        return [row.asDict() for row in rows]

    def test_a_rename_resolves_across_eras(self, mapped: list[dict]) -> None:
        assert [row["door_id"] for row in mapped] == ["D1", "D2", "D1", None]

    def test_each_tenant_parses_its_own_date_format(self, mapped: list[dict]) -> None:
        assert mapped[0]["modified_ts"].day == 5
        assert mapped[1]["modified_ts"].day == 6
        assert mapped[2]["modified_ts"].day == 5

    def test_the_same_key_in_two_tenants_is_two_entities(self, mapped: list[dict]) -> None:
        assert mapped[0]["entity_key"] != mapped[2]["entity_key"]

    def test_a_missing_business_key_yields_a_null_entity_key(self, mapped: list[dict]) -> None:
        assert mapped[3]["entity_key"] is None

    def test_entity_key_is_stable_for_the_same_input(self, spark: SparkSession, mapped: list[dict]) -> None:
        repeat = (
            spark.createDataFrame([("FERRARI", '{"DOOR_NO":"D1"}')], "tenant_id STRING, json STRING")
            .selectExpr("tenant_id", "parse_json(json) AS payload")
            .selectExpr(entity_key_expr(DOOR_ID_ROWS, GRAIN) + " AS entity_key")
            .collect()
        )
        assert repeat[0]["entity_key"] == mapped[0]["entity_key"]

    def test_a_quoted_tenant_name_survives_the_parser(self, spark: SparkSession) -> None:
        rows = [_row("O'BRIEN", "ID", "door_id", is_business_key=True)]
        out = (
            spark.createDataFrame([("O'BRIEN", '{"ID":"D9"}')], "tenant_id STRING, json STRING")
            .selectExpr("tenant_id", "parse_json(json) AS payload")
            .selectExpr(f"{raw_expr(rows)} AS door_id")
            .collect()
        )
        assert out[0]["door_id"] == "D9"
