"""Transform Ember global electricity data (yearly, monthly, Europe monthly)."""

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASETS = {
    "ember_electricity_yearly": {
        "raw_key": "ember_electricity_yearly",
        "date_col": "year",
        "date_source": "Year",
        "title": "Ember Global Electricity (Yearly)",
        "description": "Yearly electricity generation, capacity, emissions and demand for 200+ countries. Frequency is always yearly.",
    },
    "ember_electricity_monthly": {
        "raw_key": "ember_electricity_monthly",
        "date_col": "month",
        "date_source": "Date",
        "title": "Ember Global Electricity (Monthly)",
        "description": "Monthly electricity generation, emissions and demand for 88 countries representing 90%+ of global power demand. Frequency is always monthly.",
    },
    "ember_electricity_europe_monthly": {
        "raw_key": "ember_electricity_europe_monthly",
        "date_col": "month",
        "date_source": "Date",
        "title": "Ember Europe Electricity (Monthly)",
        "description": "Monthly electricity generation, emissions and demand for European countries. Frequency is always monthly.",
    },
}

COLUMN_DESCRIPTIONS = {
    "country_name": "Country or area name",
    "country_code": "ISO 3-letter country code",
    "area_type": "Type of area (Country or economy, Region, etc.)",
    "continent": "Continent name",
    "category": "Data category (Electricity demand, Electricity generation, Capacity, etc.)",
    "subcategory": "Subcategory (Demand, Aggregate fuel, Fuel, etc.)",
    "variable": "Specific variable being measured",
    "unit": "Unit of measurement (TWh, %, GW, etc.)",
    "value": "Measured value",
    "yoy_change": "Year-over-year absolute change",
    "yoy_change_pct": "Year-over-year percentage change",
}


def transform(csv_text: str, date_source: str, date_col: str) -> pa.Table:
    """Transform raw CSV to output schema."""
    table = csv.read_csv(pa.py_buffer(csv_text.encode()))

    # Build output columns
    date_values = table.column(date_source)
    if date_source == "Year":
        # Cast year int to string
        date_values = pc.cast(date_values, pa.string())
    elif date_source == "Date":
        # Convert date to string YYYY-MM-DD then slice to YYYY-MM
        date_values = pc.strftime(date_values, format="%Y-%m")

    columns = {
        date_col: date_values,
        "country_name": table.column("Area"),
        "country_code": table.column("ISO 3 code"),
        "area_type": table.column("Area type"),
        "continent": table.column("Continent"),
        "category": table.column("Category"),
        "subcategory": table.column("Subcategory"),
        "variable": table.column("Variable"),
        "unit": table.column("Unit"),
        "value": pc.cast(table.column("Value"), pa.float64()),
        "yoy_change": pc.cast(table.column("YoY absolute change"), pa.float64()),
        "yoy_change_pct": pc.cast(table.column("YoY % change"), pa.float64()),
    }

    return pa.table(columns)


def run():
    """Transform Ember global electricity datasets."""
    raw_data = load_raw_json("ember_data")

    for dataset_id, cfg in DATASETS.items():
        csv_text = raw_data[cfg["raw_key"]]
        table = transform(csv_text, cfg["date_source"], cfg["date_col"])

        print(f"  {dataset_id}: {table.num_rows:,} rows")

        test(table, cfg["date_col"])

        upload_data(table, dataset_id)

        col_desc = {cfg["date_col"]: f"{cfg['date_col'].title()} of observation"}
        col_desc.update({k: v for k, v in COLUMN_DESCRIPTIONS.items() if k != cfg["date_col"]})

        publish(dataset_id, {
            "id": dataset_id,
            "title": cfg["title"],
            "description": cfg["description"],
            "column_descriptions": col_desc,
        })


if __name__ == "__main__":
    run()
