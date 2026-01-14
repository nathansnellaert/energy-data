"""Tests for Ember India electricity datasets."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_valid_month, assert_max_length


def test(table: pa.Table, date_col: str) -> None:
    """Validate Ember India electricity dataset."""
    validate(table, {
        "columns": {
            date_col: "string",
            "country_name": "string",
            "country_code": "string",
            "state": "string",
            "state_code": "string",
            "state_type": "string",
            "category": "string",
            "subcategory": "string",
            "variable": "string",
            "unit": "string",
            "value": "double",
            "yoy_change": "double",
            "yoy_change_pct": "double",
        },
        "not_null": [date_col, "state", "category", "variable", "unit"],
        "min_rows": 1000,
    })

    if date_col == "year":
        assert_valid_year(table, date_col)
    else:
        assert_valid_month(table, date_col)

    assert_max_length(table, "country_code", 3)
    assert_max_length(table, "state_code", 2)

    # Validate all data is for India
    countries = set(table.column("country_code").to_pylist())
    assert countries == {"IND"}, f"Expected only IND, got {countries}"

    # Validate state types
    state_types = set(table.column("state_type").to_pylist())
    expected = {"State", "Union territory", "state", "total"}
    assert state_types <= expected, f"Unexpected state types: {state_types - expected}"
