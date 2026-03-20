from __future__ import annotations

from app.county_adapters.common.config_loader import load_county_adapter_config


def test_load_harris_county_adapter_config() -> None:
    config = load_county_adapter_config("harris")
    assert config.county_id == "harris"
    assert "property_roll" in config.datasets
    assert "tax_rates" in config.datasets
    assert config.dataset_configs["property_roll"].staging_table == "stg_county_property_raw"
    assert config.dataset_configs["tax_rates"].staging_table == "stg_county_tax_rates_raw"
    assert config.dataset_configs["property_roll"].source_name == "Harris CAD Property Roll"
    assert config.dataset_configs["property_roll"].access_method == "fixture_json"
    assert config.field_mappings["property_roll"].sections["parcel"].mode == "object"


def test_load_fort_bend_county_adapter_config() -> None:
    config = load_county_adapter_config("fort_bend")
    assert config.county_id == "fort_bend"
    assert config.parser_module == "app.county_adapters.fort_bend.parse"
    assert config.dataset_configs["property_roll"].ingestion_ready is True
    assert config.dataset_configs["tax_rates"].ingestion_ready is True
    assert config.dataset_configs["property_roll"].source_type == "county_appraisal_roll"
    assert config.dataset_configs["tax_rates"].source_type == "county_tax_rates"
    assert config.dataset_configs["property_roll"].access_method == "fixture_csv"
    assert config.dataset_configs["tax_rates"].access_method == "fixture_csv"
    assert "CSV-backed" in config.dataset_configs["property_roll"].transformation_notes[0]
    assert config.field_mappings["property_roll"].mapping_version == 1
