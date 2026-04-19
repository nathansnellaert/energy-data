"""Ingest energy data from Ember.

Sources:
- Ember: Global electricity generation, capacity, and prices
"""

from subsets_utils import get, save_raw_json


EMBER_SOURCES = {
    "ember_electricity_yearly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/yearly_full_release_long_format.csv",
    "ember_electricity_monthly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/monthly_full_release_long_format.csv",
    "ember_electricity_europe_monthly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/europe_monthly_full_release_long_format.csv",
    "ember_electricity_india_yearly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/india_yearly_full_release_long_format.csv",
    "ember_electricity_india_monthly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/india_monthly_full_release_long_format.csv",
    "ember_electricity_prices_daily": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/price/outputs/european_wholesale_electricity_price_data_daily.csv",
    "ember_electricity_prices_monthly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/price/outputs/european_wholesale_electricity_price_data_monthly.csv",
}


def run():
    """Fetch all Ember electricity data sources."""
    print("Ingesting Ember electricity data...")
    all_data = {}

    for name, url in EMBER_SOURCES.items():
        print(f"  Fetching {name}...")
        response = get(url, timeout=300.0)
        response.raise_for_status()
        all_data[name] = response.text
        print(f"    Downloaded {len(response.text)} bytes")

    save_raw_json(all_data, "ember_data", compress=True)
    print("\nIngestion complete")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
