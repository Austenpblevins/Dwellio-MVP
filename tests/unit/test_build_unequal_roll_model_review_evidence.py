from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from infra.scripts import build_unequal_roll_model_review_evidence as evidence


@pytest.mark.parametrize(
    "year_built, expected", [(1998, 1998), ("2001", 2001), (None, None), ("unknown", None)]
)
def test_subject_context_preserves_optional_year_built(monkeypatch, year_built, expected):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        {
            "account_number": "1001",
            "county_id": "harris",
            "parcel_id": "parcel-1",
            "tax_year": 2025,
            "year_built": year_built,
            "appraised_value": 300000,
        }
    ]
    connect = MagicMock(return_value=connection)
    monkeypatch.setattr("psycopg.connect", connect)

    result = evidence._fetch_subject_context_map(
        database_url="postgresql://mock-only", accounts=["1001"], runtime_current_values={}
    )

    assert result["1001"]["year_built"] == expected
    assert result["1001"]["tax_year"] == 2025
    connect.assert_called_once()
