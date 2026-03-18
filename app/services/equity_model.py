from statistics import median


class EquityModelService:
    def run(self, subject_living_area_sf: float, adjusted_equity_comp_psf: list[float]) -> dict:
        equity_value_point = median(adjusted_equity_comp_psf) * subject_living_area_sf
        return {'equity_value_point': equity_value_point, 'equity_value_low': equity_value_point * 0.95, 'equity_value_high': equity_value_point * 1.05, 'confidence_score': 0.68}
