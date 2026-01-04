"""Transform Ember European electricity price data (daily and monthly)."""

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASETS = {
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

COLUMN_DESCRIPTIONS = {
    "date": "Date of observation (YYYY-MM-DD)",
    "month": "Month of observation (YYYY-MM)",
    "country_name": "Country name",
    "country_code": "ISO 3-letter country code",
    "price_eur_mwh": "Wholesale electricity price in EUR per MWh",
}


def transform(csv_text: str, date_col: str) -> pa.Table:
    """Transform raw CSV to output schema."""
    table = csv.read_csv(pa.py_buffer(csv_text.encode()))

    # Convert date to appropriate format
    date_values = table.column("Date")
    if date_col == "month":
        # Convert date to YYYY-MM
        date_values = pc.strftime(date_values, format="%Y-%m")
    else:
        # Convert date to YYYY-MM-DD string
        date_values = pc.strftime(date_values, format="%Y-%m-%d")

    columns = {
        date_col: date_values,
        "country_name": table.column("Country"),
        "country_code": table.column("ISO3 Code"),
        "price_eur_mwh": pc.cast(table.column("Price (EUR/MWhe)"), pa.float64()),
    }

    return pa.table(columns)


def run():
    """Transform Ember European electricity price datasets."""
    raw_data = load_raw_json("ember_data")

    for dataset_id, cfg in DATASETS.items():
        csv_text = raw_data[cfg["raw_key"]]
        table = transform(csv_text, cfg["date_col"])

        print(f"  {dataset_id}: {table.num_rows:,} rows")

        test(table, cfg["date_col"])

        upload_data(table, dataset_id)

        col_desc = {k: v for k, v in COLUMN_DESCRIPTIONS.items() if k in table.column_names}

        publish(dataset_id, {
            "id": dataset_id,
            "title": cfg["title"],
            "description": cfg["description"],
            "column_descriptions": col_desc,
        })


if __name__ == "__main__":
    run()
