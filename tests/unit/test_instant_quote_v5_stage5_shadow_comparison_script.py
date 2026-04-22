from infra.scripts.report_instant_quote_v5_stage5_shadow_comparison import _comparison_summary


def test_comparison_summary_tracks_display_and_raw_zero_share_separately() -> None:
    rows = [
        {
            "supported": True,
            "current_savings_estimate_display": 0.0,
            "current_savings_estimate_raw": 12.5,
            "shadow_savings_estimate_raw": 12.5,
            "shadow_savings_delta_raw": 0.0,
            "shadow_tax_profile_status": "supported_with_disclosure",
            "shadow_opportunity_vs_savings_state": "standard_quote",
        },
        {
            "supported": True,
            "current_savings_estimate_display": 100.0,
            "current_savings_estimate_raw": 100.0,
            "shadow_savings_estimate_raw": 100.0,
            "shadow_savings_delta_raw": 0.0,
            "shadow_tax_profile_status": "supported_with_disclosure",
            "shadow_opportunity_vs_savings_state": "standard_quote",
        },
    ]

    summary = _comparison_summary(rows)

    assert summary["supported_public_quote_count"] == 2
    assert summary["current_zero_share_supported"] == 0.5
    assert summary["current_zero_share_supported_display"] == 0.5
    assert summary["current_zero_share_supported_raw"] == 0.0
    assert summary["shadow_zero_share_quoteable"] == 0.0
    assert summary["shadow_zero_share_quoteable_raw"] == 0.0


def test_comparison_summary_counts_shadow_quoteability_crossover_rows() -> None:
    rows = [
        {
            "supported": True,
            "current_savings_estimate_display": 250.0,
            "current_savings_estimate_raw": 250.0,
            "shadow_savings_estimate_raw": None,
            "shadow_savings_delta_raw": None,
            "shadow_tax_profile_status": "opportunity_only",
            "shadow_opportunity_vs_savings_state": "opportunity_only_tax_profile_incomplete",
        },
        {
            "supported": False,
            "current_savings_estimate_display": None,
            "current_savings_estimate_raw": None,
            "shadow_savings_estimate_raw": 125.0,
            "shadow_savings_delta_raw": None,
            "shadow_tax_profile_status": "supported_with_disclosure",
            "shadow_opportunity_vs_savings_state": "standard_quote",
        },
    ]

    summary = _comparison_summary(rows)

    assert summary["supported_public_quote_count"] == 1
    assert summary["shadow_quoteable_count"] == 1
    assert summary["public_supported_shadow_unquoteable_count"] == 1
    assert summary["public_unsupported_shadow_quoteable_count"] == 1
    assert summary["opportunity_only_candidate_count"] == 1
    assert summary["high_opportunity_low_cash_count"] == 1
