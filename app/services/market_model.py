from dataclasses import dataclass


def weighted_average(values: list[float], weights: list[float]) -> float:
    return sum(v * w for v, w in zip(values, weights, strict=True)) / sum(weights)

@dataclass
class MarketModelInputs:
    subject_living_area_sf: float
    neighborhood_median_sale_psf_12m: float
    adjusted_comp_value_psf: list[float]
    comp_rank_weights: list[float]

class MarketModelService:
    def run(self, inputs: MarketModelInputs) -> dict:
        market_estimate_nbhd = inputs.subject_living_area_sf * inputs.neighborhood_median_sale_psf_12m
        market_estimate_comp = weighted_average(inputs.adjusted_comp_value_psf, inputs.comp_rank_weights) * inputs.subject_living_area_sf
        market_value_point = 0.40 * market_estimate_nbhd + 0.60 * market_estimate_comp
        return {'market_value_point': market_value_point, 'market_value_low': market_value_point * 0.95, 'market_value_high': market_value_point * 1.05, 'confidence_score': 0.70}
