from __future__ import annotations

from app.county_adapters.common.config_loader import load_county_adapter_config


def test_load_harris_county_adapter_config() -> None:
    config = load_county_adapter_config("harris")
    assert config.county_id == "harris"
    assert "property_roll" in config.datasets


def test_load_fort_bend_county_adapter_config() -> None:
    config = load_county_adapter_config("fort_bend")
    assert config.county_id == "fort_bend"
    assert config.parser_module == "app.county_adapters.fort_bend.parse"

