"""Tests for declaration loading. Fixtures are written to tmp_path; no workspace involved."""

from pathlib import Path

import pytest

from bedi_lakehouse.config import (
    MAPPING_COLUMNS,
    ConfigError,
    Historisation,
    OnViolation,
    SequencingSource,
    load_column_mapping,
    load_entity_definition,
    load_entity_definitions,
    load_environment,
    parse_entity_definition,
)

ENTITY_YAML = """
source_system: aas_doors
entity: requirement

landing:
  volume: landing
  path: aas_doors/requirement
  layout: "<tenant>/<load_mode>"
  format: csv
  options: { header: true }

grain:
  business_key: [object_id]
  tenant_column: project_id

historisation: scd2

sequencing:
  source: manifest
  manifest_column: extract_seq

schema_evolution:
  new_column: accept
  missing_business_key: fail

quality:
  required: [object_id, level]
  on_violation: quarantine
"""

MAPPING_CSV = """tenant,source_column,generic_column,target_type,parse_format,precedence,is_business_key,is_promoted
FERRARI,Object_ID_Ferrari,object_id,STRING,,1,true,true
FERRARI,Last_Modified_Ferrari,modified_ts,TIMESTAMP,dd/MM/yyyy HH:mm:ss,1,false,true
FERRARI,Fuel_Load_Ferrari,,STRING,,1,no,no
"""


def _write_entity(root: Path, body: str = ENTITY_YAML) -> Path:
    path = root / "sources" / "aas_doors" / "requirement.yml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


class TestEntityDefinition:
    def test_loads_a_complete_declaration(self, tmp_path: Path) -> None:
        definition = load_entity_definition(_write_entity(tmp_path))
        assert definition.stream.name == "aas_doors__requirement"
        assert definition.historisation is Historisation.SCD2
        assert definition.sequencing.source is SequencingSource.MANIFEST
        assert definition.sequencing.manifest_column == "extract_seq"
        assert definition.grain.business_key == ("object_id",)
        assert definition.landing.options == {"header": True}
        assert definition.quality.on_violation is OnViolation.QUARANTINE

    def test_unknown_key_is_rejected_rather_than_ignored(self) -> None:
        raw = {"source_system": "aas_doors", "entity": "requirement", "retention": {"bronze_days": 10}}
        with pytest.raises(ConfigError, match="unknown key"):
            parse_entity_definition(raw, "test")

    def test_missing_key_names_what_is_missing(self) -> None:
        with pytest.raises(ConfigError, match="missing required key"):
            parse_entity_definition({"source_system": "aas_doors", "entity": "requirement"}, "test")

    def test_unsupported_historisation_lists_the_alternatives(self, tmp_path: Path) -> None:
        path = _write_entity(tmp_path, ENTITY_YAML.replace("historisation: scd2", "historisation: scd7"))
        with pytest.raises(ConfigError, match="'scd7' is not one of"):
            load_entity_definition(path)

    def test_manifest_sequencing_requires_a_manifest_column(self, tmp_path: Path) -> None:
        path = _write_entity(tmp_path, ENTITY_YAML.replace("  manifest_column: extract_seq\n", ""))
        with pytest.raises(ConfigError, match="requires manifest_column"):
            load_entity_definition(path)

    def test_empty_business_key_is_rejected(self, tmp_path: Path) -> None:
        path = _write_entity(tmp_path, ENTITY_YAML.replace("business_key: [object_id]", "business_key: []"))
        with pytest.raises(ConfigError, match="at least one column"):
            load_entity_definition(path)

    def test_bad_stream_identifier_is_reported_as_config_error(self) -> None:
        raw = {"source_system": "AAS Doors", "entity": "requirement"}
        with pytest.raises(ConfigError):
            parse_entity_definition(raw, "test")


class TestLoadEntityDefinitions:
    def test_keys_by_stream_name(self, tmp_path: Path) -> None:
        _write_entity(tmp_path)
        definitions = load_entity_definitions(tmp_path)
        assert list(definitions) == ["aas_doors__requirement"]

    def test_duplicate_stream_declaration_is_rejected(self, tmp_path: Path) -> None:
        _write_entity(tmp_path)
        duplicate = tmp_path / "sources" / "sap" / "copy.yml"
        duplicate.parent.mkdir(parents=True, exist_ok=True)
        duplicate.write_text(ENTITY_YAML, encoding="utf-8")
        with pytest.raises(ConfigError, match="already declared"):
            load_entity_definitions(tmp_path)

    def test_missing_sources_directory_is_reported(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="does not exist"):
            load_entity_definitions(tmp_path)


class TestColumnMapping:
    def _write(self, tmp_path: Path, body: str = MAPPING_CSV) -> Path:
        path = tmp_path / "requirement.csv"
        path.write_text(body, encoding="utf-8")
        return path

    def test_parses_types_flags_and_blanks(self, tmp_path: Path) -> None:
        rows = load_column_mapping(self._write(tmp_path))
        assert len(rows) == 3
        assert rows[0].is_business_key is True
        assert rows[1].parse_format == "dd/MM/yyyy HH:mm:ss"
        assert rows[2].generic_column is None
        assert rows[2].is_promoted is False

    def test_header_must_match_exactly(self, tmp_path: Path) -> None:
        path = self._write(tmp_path, "tenant,source_column\nFERRARI,x\n")
        with pytest.raises(ConfigError, match="header must be exactly"):
            load_column_mapping(path)

    def test_duplicate_tenant_column_pair_is_rejected(self, tmp_path: Path) -> None:
        duplicated = MAPPING_CSV + "FERRARI,Object_ID_Ferrari,object_id,STRING,,1,true,true\n"
        with pytest.raises(ConfigError, match="duplicate mapping"):
            load_column_mapping(self._write(tmp_path, duplicated))

    def test_non_numeric_precedence_is_rejected(self, tmp_path: Path) -> None:
        body = ",".join(MAPPING_COLUMNS) + "\nFERRARI,x,object_id,STRING,,first,true,true\n"
        with pytest.raises(ConfigError, match="precedence"):
            load_column_mapping(self._write(tmp_path, body))

    def test_unparseable_boolean_is_rejected(self, tmp_path: Path) -> None:
        body = ",".join(MAPPING_COLUMNS) + "\nFERRARI,x,object_id,STRING,,1,maybe,true\n"
        with pytest.raises(ConfigError, match="is not a boolean"):
            load_column_mapping(self._write(tmp_path, body))

    def test_empty_mapping_is_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="no rows"):
            load_column_mapping(self._write(tmp_path, ",".join(MAPPING_COLUMNS) + "\n"))


class TestEnvironment:
    def _write(self, tmp_path: Path, name: str, body: str) -> None:
        path = tmp_path / "environments" / f"{name}.yml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")

    def test_loads_catalog_and_optional_suffix(self, tmp_path: Path) -> None:
        self._write(tmp_path, "dev", "name: dev\ncatalog: beg_bedi_dev\nschema_suffix: _abaubinas\n")
        environment = load_environment(tmp_path, "dev")
        assert environment.catalog == "beg_bedi_dev"
        assert environment.schema_suffix == "_abaubinas"

    def test_suffix_defaults_to_empty(self, tmp_path: Path) -> None:
        self._write(tmp_path, "prod", "name: prod\ncatalog: beg_bedi_prod\n")
        assert load_environment(tmp_path, "prod").schema_suffix == ""

    def test_name_must_match_the_file(self, tmp_path: Path) -> None:
        self._write(tmp_path, "dev", "name: test\ncatalog: beg_bedi_dev\n")
        with pytest.raises(ConfigError, match="filed as"):
            load_environment(tmp_path, "dev")

    def test_missing_file_is_reported(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="does not exist"):
            load_environment(tmp_path, "nope")
