"""Transform energy data into final datasets.

Transforms:
- Ember global electricity (yearly, monthly, Europe monthly)
- Ember India electricity (yearly, monthly)
- Ember European prices (daily, monthly)
"""

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from subsets_utils import load_raw_json, upload_data, validate


# =============================================================================
# Validation
# =============================================================================

def test_ember(table: pa.Table, date_col: str):
    """Validate Ember electricity datasets."""
    validate(table, {
        "columns": {
            date_col: "string",
            "country_name": "string",
            "country_code": "string",
            "category": "string",
            "variable": "string",
            "unit": "string",
            "value": "double",
        },
        "not_null": [date_col, "country_name", "category", "variable"],
        "min_rows": 1000,
    })


def test_india(table: pa.Table, date_col: str):
    """Validate India electricity datasets."""
    validate(table, {
        "columns": {
            date_col: "string",
            "state": "string",
            "category": "string",
            "variable": "string",
            "unit": "string",
            "value": "double",
        },
        "not_null": [date_col, "state", "category", "variable"],
        "min_rows": 100,
    })


def test_prices(table: pa.Table, date_col: str):
    """Validate European prices datasets."""
    validate(table, {
        "columns": {
            date_col: "string",
            "country_name": "string",
            "country_code": "string",
            "price_eur_mwh": "double",
        },
        "not_null": [date_col, "country_name"],
        "min_rows": 100,
    })


# =============================================================================
# Global Electricity Transform
# =============================================================================

GLOBAL_DATASETS = {
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

GLOBAL_COLUMN_DESCRIPTIONS = {
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


def transform_global_electricity():
    """Transform Ember global electricity datasets."""
    print("\n--- Ember Global Electricity ---")
    raw_data = load_raw_json("ember_data")

    for dataset_id, cfg in GLOBAL_DATASETS.items():
        csv_text = raw_data[cfg["raw_key"]]
        table = csv.read_csv(pa.py_buffer(csv_text.encode()))

        # Build output columns
        date_values = table.column(cfg["date_source"])
        if cfg["date_source"] == "Year":
            date_values = pc.cast(date_values, pa.string())
        elif cfg["date_source"] == "Date":
            date_values = pc.strftime(date_values, format="%Y-%m")

        columns = {
            cfg["date_col"]: date_values,
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

        output_table = pa.table(columns)

        print(f"  {dataset_id}: {output_table.num_rows:,} rows")

        test_ember(output_table, cfg["date_col"])

        upload_data(output_table, dataset_id, mode="overwrite")

        col_desc = {cfg["date_col"]: f"{cfg['date_col'].title()} of observation"}
        col_desc.update({k: v for k, v in GLOBAL_COLUMN_DESCRIPTIONS.items() if k != cfg["date_col"]})
# =============================================================================
# India Electricity Transform
# =============================================================================

INDIA_DATASETS = {
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

INDIA_COLUMN_DESCRIPTIONS = {
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


def transform_india_electricity():
    """Transform Ember India electricity datasets."""
    print("\n--- Ember India Electricity ---")
    raw_data = load_raw_json("ember_data")

    for dataset_id, cfg in INDIA_DATASETS.items():
        csv_text = raw_data[cfg["raw_key"]]
        table = csv.read_csv(pa.py_buffer(csv_text.encode()))

        date_values = table.column(cfg["date_source"])
        if cfg["date_source"] == "Year":
            date_values = pc.cast(date_values, pa.string())
        elif cfg["date_source"] == "Date":
            date_values = pc.strftime(date_values, format="%Y-%m")

        columns = {
            cfg["date_col"]: date_values,
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

        output_table = pa.table(columns)

        print(f"  {dataset_id}: {output_table.num_rows:,} rows")

        test_india(output_table, cfg["date_col"])

        upload_data(output_table, dataset_id, mode="overwrite")

        col_desc = {cfg["date_col"]: f"{cfg['date_col'].title()} of observation"}
        col_desc.update({k: v for k, v in INDIA_COLUMN_DESCRIPTIONS.items() if k != cfg["date_col"]})
# =============================================================================
# European Prices Transform
# =============================================================================

PRICES_DATASETS = {
    "ember_electricity_prices_daily": {
        "raw_key": "ember_electricity_prices_daily",
        "date_col": "date",
        "title": "Ember European Electricity Prices (Daily)",
        "description": "Daily wholesale day-ahead electricity prices for European countries. Frequency is always daily. Currency is always EUR.",
    },
    "ember_electricity_prices_monthly": {
        "raw_key": "ember_electricity_prices_monthly",
        "date_col": "month",
        "title": "Ember European Electricity Prices (Monthly)",
        "description": "Monthly average wholesale day-ahead electricity prices for European countries. Frequency is always monthly. Currency is always EUR.",
    },
}

PRICES_COLUMN_DESCRIPTIONS = {
    "date": "Date of observation (YYYY-MM-DD)",
    "month": "Month of observation (YYYY-MM)",
    "country_name": "Country name",
    "country_code": "ISO 3-letter country code",
    "price_eur_mwh": "Wholesale electricity price in EUR per MWh",
}


def transform_european_prices():
    """Transform Ember European electricity price datasets."""
    print("\n--- Ember European Prices ---")
    raw_data = load_raw_json("ember_data")

    for dataset_id, cfg in PRICES_DATASETS.items():
        csv_text = raw_data[cfg["raw_key"]]
        table = csv.read_csv(pa.py_buffer(csv_text.encode()))

        date_values = table.column("Date")
        if cfg["date_col"] == "month":
            date_values = pc.strftime(date_values, format="%Y-%m")
        else:
            date_values = pc.strftime(date_values, format="%Y-%m-%d")

        columns = {
            cfg["date_col"]: date_values,
            "country_name": table.column("Country"),
            "country_code": table.column("ISO3 Code"),
            "price_eur_mwh": pc.cast(table.column("Price (EUR/MWhe)"), pa.float64()),
        }

        output_table = pa.table(columns)

        print(f"  {dataset_id}: {output_table.num_rows:,} rows")

        test_prices(output_table, cfg["date_col"])

        upload_data(output_table, dataset_id, mode="overwrite")

        col_desc = {k: v for k, v in PRICES_COLUMN_DESCRIPTIONS.items() if k in output_table.column_names}
# =============================================================================
# Main
# =============================================================================

def run():
    """Transform all energy datasets."""
    print("Transforming energy datasets...")

    transform_global_electricity()
    transform_india_electricity()
    transform_european_prices()

    print("\nAll transforms complete")


from nodes.ingest import run as ingest_run

NODES = {
    run: [ingest_run],
}


if __name__ == "__main__":
    run()
