from statistics import median


class EquityModelService:
    def run(
        self,
        subject_living_area_sf: float,
        adjusted_equity_comp_psf: list[float],
        *,
        low_value_psf: float | None = None,
        high_value_psf: float | None = None,
        confidence_score: float | None = None,
    ) -> dict:
        equity_value_point = median(adjusted_equity_comp_psf) * subject_living_area_sf
        equity_value_low = (
            low_value_psf * subject_living_area_sf
            if low_value_psf is not None
            else equity_value_point * 0.95
        )
        equity_value_high = (
            high_value_psf * subject_living_area_sf
            if high_value_psf is not None
            else equity_value_point * 1.05
        )
        return {
            'equity_value_point': equity_value_point,
            'equity_value_low': equity_value_low,
            'equity_value_high': equity_value_high,
            'confidence_score': confidence_score if confidence_score is not None else 0.68,
        }
