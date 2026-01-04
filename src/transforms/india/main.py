"""Transform Ember India electricity data (yearly and monthly)."""

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASETS = {
    "ember_electricity_india_yearly": {
        "raw_key": "ember_electricity_india_yearly",
        "date_col": "year",
        "date_source": "Year",
        "title": "Ember India Electricity (Yearly)",
        "description": "Yearly electricity generation, capacity, and emissions for 36 Indian states and union territories. Frequency is always yearly.",
    },
    "ember_electricity_india_monthly": {
        "raw_key": "ember_electricity_india_monthly",
        "date_col": "month",
        "date_source": "Date",
        "title": "Ember India Electricity (Monthly)",
        "description": "Monthly electricity generation, capacity, and emissions for 36 Indian states and union territories. Frequency is always monthly.",
    },
}

COLUMN_DESCRIPTIONS = {
    "country_name": "Country name (always India)",
    "country_code": "ISO 3-letter country code (always IND)",
    "state": "Indian state or union territory name",
    "state_code": "Two-letter state code",
    "state_type": "Type of administrative division (State or Union territory)",
    "category": "Data category (Capacity, Electricity generation, Power sector emissions)",
    "subcategory": "Subcategory (Aggregate fuel, Fuel, etc.)",
    "variable": "Specific variable being measured",
    "unit": "Unit of measurement (MW, GWh, MtCO2, etc.)",
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
        # Convert date to string YYYY-MM
        date_values = pc.strftime(date_values, format="%Y-%m")

    columns = {
        date_col: date_values,
        "country_name": table.column("Country"),
        "country_code": table.column("Country code"),
        "state": table.column("State"),
        "state_code": table.column("State code"),
        "state_type": table.column("State type"),
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
    """Transform Ember India electricity datasets."""
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
