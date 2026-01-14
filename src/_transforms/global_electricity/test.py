"""Tests for Ember global electricity datasets."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_valid_month, assert_max_length


def test(table: pa.Table, date_col: str) -> None:
    """Validate Ember electricity dataset."""
    validate(table, {
        "columns": {
            date_col: "string",
            "country_name": "string",
            "country_code": "string",
            "area_type": "string",
            "continent": "string",
            "category": "string",
            "subcategory": "string",
            "variable": "string",
            "unit": "string",
            "value": "double",
            "yoy_change": "double",
            "yoy_change_pct": "double",
        },
        "not_null": [date_col, "country_name", "category", "variable", "unit"],
        "min_rows": 10000,
    })

    if date_col == "year":
        assert_valid_year(table, date_col)
    else:
        assert_valid_month(table, date_col)

    assert_max_length(table, "country_code", 3)

    # Validate expected categories
    categories = set(table.column("category").to_pylist())
    expected = {"Electricity demand", "Electricity generation", "Capacity", "Power sector emissions"}
    assert categories & expected, f"Expected some of {expected}, got {categories}"
