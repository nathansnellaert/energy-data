"""Tests for Ember European electricity price datasets."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_valid_month, assert_max_length


def test(table: pa.Table, date_col: str) -> None:
    """Validate Ember electricity prices dataset."""
    validate(table, {
        "columns": {
            date_col: "string",
            "country_name": "string",
            "country_code": "string",
            "price_eur_mwh": "double",
        },
        "not_null": [date_col, "country_name", "country_code"],
        "min_rows": 100,
    })

    if date_col == "date":
        assert_valid_date(table, date_col)
    else:
        assert_valid_month(table, date_col)

    assert_max_length(table, "country_code", 3)

    # Validate prices are reasonable (not negative, not astronomically high)
    prices = [p for p in table.column("price_eur_mwh").to_pylist() if p is not None]
    assert all(p >= -500 for p in prices), "Prices should not be extremely negative"
    assert all(p <= 10000 for p in prices), "Prices should not exceed 10000 EUR/MWh"
