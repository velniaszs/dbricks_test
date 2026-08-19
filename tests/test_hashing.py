"""Tests for the fingerprint expressions. Pure string generation, no Spark session."""

import hashlib

import pytest

from bedi_lakehouse.hashing import (
    NULL_SENTINEL_SQL,
    canon_expr,
    norm_expr,
    quote_identifier,
    quote_literal,
    row_hash_expr,
    schema_version,
)


class TestQuoting:
    def test_identifier_is_backticked(self) -> None:
        assert quote_identifier("Object_ID_Ferrari") == "`Object_ID_Ferrari`"

    def test_embedded_backtick_is_doubled(self) -> None:
        assert quote_identifier("we`ird") == "`we``ird`"

    def test_literal_escapes_quote_and_backslash(self) -> None:
        assert quote_literal("O'Brien") == "'O\\'Brien'"
        assert quote_literal("a\\b") == "'a\\\\b'"


class TestNormExpr:
    def test_trims_and_nulls_out_tokens(self) -> None:
        expr = norm_expr("Status_Ferrari")
        assert expr.startswith("CASE WHEN upper(trim(`Status_Ferrari`)) IN (")
        assert "'N/A'" in expr
        assert expr.endswith("THEN NULL ELSE trim(`Status_Ferrari`) END")

    def test_quotes_the_column_rather_than_interpolating_it(self) -> None:
        assert "`x`` FROM t; --`" in norm_expr("x` FROM t; --")


class TestCanonExpr:
    def test_keys_are_sorted_so_column_order_cannot_change_the_hash(self) -> None:
        assert canon_expr(["b", "a"]) == canon_expr(["a", "b"])

    def test_absent_values_fall_back_to_the_sentinel(self) -> None:
        expr = canon_expr(["a"])
        assert expr.startswith("to_json(struct(coalesce(")
        assert NULL_SENTINEL_SQL in expr
        assert expr.endswith("AS `a`))")

    def test_empty_column_set_is_an_error(self) -> None:
        with pytest.raises(ValueError, match="empty column set"):
            canon_expr([])


class TestRowHashExpr:
    def test_is_sha256_over_the_canonical_json(self) -> None:
        expr = row_hash_expr(["a", "b"])
        assert expr == f"sha2({canon_expr(['a', 'b'])}, 256)"

    def test_depends_only_on_source_column_names(self) -> None:
        assert row_hash_expr(["a"]) == row_hash_expr(("a",))


class TestSchemaVersion:
    def test_matches_the_v1_definition(self) -> None:
        columns = ["Level_Ferrari", "Object_ID_Ferrari"]
        expected = hashlib.sha256("|".join(sorted(columns)).encode("utf-8")).hexdigest()
        assert schema_version(columns) == expected

    def test_is_order_insensitive(self) -> None:
        assert schema_version(["b", "a"]) == schema_version(["a", "b"])

    def test_detects_an_added_column(self) -> None:
        assert schema_version(["a"]) != schema_version(["a", "b"])

    def test_empty_header_is_an_error(self) -> None:
        with pytest.raises(ValueError, match="empty header"):
            schema_version([])
