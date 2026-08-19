"""Tests for name construction. No Spark, no workspace."""

import pytest

from bedi_lakehouse.naming import BRONZE, META, Layout, Stream


class TestStream:
    def test_name_round_trips_through_parse(self) -> None:
        stream = Stream("aas_doors", "requirement")
        assert stream.name == "aas_doors__requirement"
        assert Stream.parse(stream.name) == stream

    def test_str_is_the_table_name(self) -> None:
        assert str(Stream("sap", "material")) == "sap__material"

    def test_single_underscores_are_legal_in_either_half(self) -> None:
        stream = Stream.parse("aas_doors__change_request")
        assert stream.source_system == "aas_doors"
        assert stream.entity == "change_request"

    @pytest.mark.parametrize("value", ["requirement", "a__b__c", ""])
    def test_parse_rejects_wrong_separator_count(self, value: str) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            Stream.parse(value)

    @pytest.mark.parametrize("value", ["AAS_Doors", "1sap", "sap_", "aas__doors"])
    def test_rejects_identifiers_that_break_round_tripping(self, value: str) -> None:
        with pytest.raises(ValueError, match="lower snake case"):
            Stream(value, "requirement")


class TestLayout:
    def test_no_suffix_in_production_shape(self) -> None:
        layout = Layout("beg_bedi_prod")
        assert layout.silver(Stream("aas_doors", "requirement")) == "beg_bedi_prod.silver.aas_doors__requirement"

    def test_suffix_isolates_developers_without_changing_table_names(self) -> None:
        shared = Layout("beg_bedi_dev")
        mine = Layout("beg_bedi_dev", "_abaubinas")
        stream = Stream("aas_doors", "requirement")
        assert shared.bronze(stream) == "beg_bedi_dev.bronze.aas_doors__requirement"
        assert mine.bronze(stream) == "beg_bedi_dev.bronze_abaubinas.aas_doors__requirement"
        assert shared.bronze(stream).split(".")[-1] == mine.bronze(stream).split(".")[-1]

    def test_leading_underscore_is_optional(self) -> None:
        assert Layout("beg_bedi_dev", "abaubinas").schema(META) == "meta_abaubinas"
        assert Layout("beg_bedi_dev", "_abaubinas").schema(META) == "meta_abaubinas"

    def test_blank_suffix_is_no_suffix(self) -> None:
        assert Layout("beg_bedi_dev", "   ").schema(BRONZE) == "bronze"

    def test_meta_and_gold_take_plain_table_names(self) -> None:
        layout = Layout("beg_bedi_dev", "_ab")
        assert layout.meta("column_catalog") == "beg_bedi_dev.meta_ab.column_catalog"
        assert layout.gold("requirement_current") == "beg_bedi_dev.gold_ab.requirement_current"

    def test_volume_and_landing_path(self) -> None:
        layout = Layout("beg_bedi_dev", "_ab")
        assert layout.volume("landing") == "/Volumes/beg_bedi_dev/bronze_ab/landing"
        assert layout.landing_path("landing", "/aas_doors/requirement/") == (
            "/Volumes/beg_bedi_dev/bronze_ab/landing/aas_doors/requirement"
        )

    def test_unknown_layer_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unknown layer"):
            Layout("beg_bedi_dev").schema("platinum")

    def test_invalid_suffix_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="schema_suffix"):
            Layout("beg_bedi_dev", "_Bad Suffix")

    def test_invalid_catalog_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="catalog"):
            Layout("BEG_BEDI_DEV")
