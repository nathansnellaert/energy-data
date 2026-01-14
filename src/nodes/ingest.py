"""Ingest energy data from multiple sources.

Sources:
- Ember: Global electricity generation, capacity, and prices
- IEA: Monthly electricity statistics
- IRENA: Renewable energy capacity and generation
- JODI: Oil production and consumption statistics
"""

import time
from subsets_utils import get, post, save_raw_json, save_raw_file, load_state, save_state


# =============================================================================
# Ember Data
# =============================================================================

EMBER_SOURCES = {
    # Global electricity
    "ember_electricity_yearly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/yearly_full_release_long_format.csv",
    "ember_electricity_monthly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/monthly_full_release_long_format.csv",
    "ember_electricity_europe_monthly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/europe_monthly_full_release_long_format.csv",
    # India electricity
    "ember_electricity_india_yearly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/india_yearly_full_release_long_format.csv",
    "ember_electricity_india_monthly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/india_monthly_full_release_long_format.csv",
    # European electricity prices
    "ember_electricity_prices_daily": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/price/outputs/european_wholesale_electricity_price_data_daily.csv",
    "ember_electricity_prices_monthly": "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/price/outputs/european_wholesale_electricity_price_data_monthly.csv",
}


def ingest_ember():
    """Fetch all Ember electricity data sources."""
    print("\n--- Ember Electricity ---")
    all_data = {}

    for name, url in EMBER_SOURCES.items():
        print(f"  Fetching {name}...")
        response = get(url, timeout=300.0)
        response.raise_for_status()
        all_data[name] = response.text
        print(f"    Downloaded {len(response.text)} bytes")

    save_raw_json(all_data, "ember_data", compress=True)


# =============================================================================
# IEA Electricity
# =============================================================================

def ingest_iea():
    """Fetch all IEA monthly electricity statistics."""
    print("\n--- IEA Monthly Electricity ---")

    # Get metadata lists
    print("  Fetching metadata...")
    years = get("https://api.iea.org/mes/list/YEAR").json()
    products = get("https://api.iea.org/mes/list/PRODUCT").json()
    countries = get("https://api.iea.org/mes/list/COUNTRY").json()

    save_raw_json({
        "years": years,
        "products": products,
        "countries": countries
    }, "metadata")
    print(f"    Years: {min(years)}-{max(years)}")
    print(f"    Products: {len(products)}")
    print(f"    Countries: {len(countries)}")

    # Get all data from bulk endpoint
    print("  Fetching all monthly data...")
    response = get("https://api.iea.org/mes/", timeout=120)
    data = response.json()

    save_raw_json(data, "monthly_electricity", compress=True)
    print(f"    Records: {len(data):,}")


# =============================================================================
# IRENA Statistics
# =============================================================================

IRENA_BASE_URL = "https://pxweb.irena.org/api/v1/en/IRENASTAT"

IRENA_TABLES = {
    "electricity_by_country": "Power Capacity and Generation/Country_ELECSTAT_2025_H2_PX.px",
    "electricity_by_region": "Power Capacity and Generation/Region_ELECSTAT_2025_H2_PX.px",
    "renewable_share": "Power Capacity and Generation/RE-SHARE_2025_H2_PX.px",
    "public_investments": "Finance/PUBFIN_2025_H2_PX.px",
    "heat_generation": "Heat Generation/HEATGEN_2025_cycle2_PX.px",
}

MAX_CELLS = 100_000


def get_table_metadata(table_path: str) -> dict:
    """Fetch metadata for a PxWeb table."""
    url = f"{IRENA_BASE_URL}/{table_path}"
    response = get(url, timeout=30.0)
    response.raise_for_status()
    return response.json()


def calculate_total_cells(metadata: dict) -> int:
    """Calculate total cells if all data is fetched."""
    total = 1
    for var in metadata["variables"]:
        total *= len(var["values"])
    return total


def fetch_irena_table(name: str, table_path: str):
    """Fetch table data, chunking by year if needed."""
    url = f"{IRENA_BASE_URL}/{table_path}"
    metadata = get_table_metadata(table_path)
    time.sleep(1.1)
    total_cells = calculate_total_cells(metadata)

    if total_cells <= MAX_CELLS:
        query = {"query": [], "response": {"format": "json-stat2"}}
        response = post(url, json=query, timeout=120.0)
        response.raise_for_status()
        data = response.json()
        save_raw_json(data, name, compress=True)
        time.sleep(1.1)
        return len(data.get("value", []))

    # Find the time/year variable to split on
    year_var = None
    year_idx = None
    for idx, var in enumerate(metadata["variables"]):
        if var.get("time", False) or "year" in var["code"].lower():
            year_var = var
            year_idx = idx
            break

    if not year_var:
        raise ValueError(f"Cannot find year variable for {table_path}")

    years = year_var["values"]
    year_code = year_var["code"]
    year_labels = year_var.get("valueTexts", years)

    cells_per_year = total_cells // len(years)
    years_per_chunk = max(1, MAX_CELLS // cells_per_year)

    print(f"    -> {total_cells:,} total cells, fetching in chunks of {years_per_chunk} years")

    all_values = []
    base_response = None

    for i in range(0, len(years), years_per_chunk):
        chunk_years = years[i:i + years_per_chunk]

        query = {
            "query": [{"code": year_code, "selection": {"filter": "item", "values": chunk_years}}],
            "response": {"format": "json-stat2"}
        }

        response = post(url, json=query, timeout=120.0)
        response.raise_for_status()
        data = response.json()

        if base_response is None:
            base_response = data
            all_values = list(data["value"])
        else:
            all_values.extend(data["value"])

        print(f"    -> fetched years {year_labels[i]}-{year_labels[min(i + years_per_chunk - 1, len(years) - 1)]}")
        time.sleep(1.1)

    base_response["value"] = all_values
    base_response["size"][year_idx] = len(years)

    year_dim_name = base_response["id"][year_idx]
    year_dim = base_response["dimension"][year_dim_name]
    year_dim["category"]["index"] = {y: i for i, y in enumerate(years)}
    year_dim["category"]["label"] = {y: label for y, label in zip(years, year_labels)}

    save_raw_json(base_response, name, compress=True)
    return len(all_values)


def ingest_irena():
    """Fetch all IRENA tables."""
    print("\n--- IRENA Renewable Capacity ---")

    state = load_state("irenastat")
    completed = set(state.get("completed", []))

    pending = [(name, path) for name, path in IRENA_TABLES.items() if name not in completed]

    if not pending:
        print("  All tables already fetched")
        return

    print(f"  Fetching {len(pending)} tables...")

    for i, (name, path) in enumerate(pending, 1):
        print(f"  [{i}/{len(pending)}] Fetching {name}...")

        count = fetch_irena_table(name, path)

        completed.add(name)
        save_state("irenastat", {"completed": list(completed)})

        print(f"    -> {count:,} data points")


# =============================================================================
# JODI Oil
# =============================================================================

JODI_PRIMARY_BASE_URL = "https://www.jodidata.org/_resources/files/downloads/oil-data/annual-csv/primary"
JODI_SECONDARY_BASE_URL = "https://www.jodidata.org/_resources/files/downloads/oil-data/annual-csv/secondary"
START_YEAR = 2002
END_YEAR = 2025


def ingest_jodi_primary():
    """Fetch JODI Oil primary CSV data."""
    print("\n--- JODI Oil Primary ---")

    state = load_state("jodi_oil")
    completed = set(state.get("completed", []))

    years = [str(y) for y in range(START_YEAR, END_YEAR + 1)]
    pending = [y for y in years if y not in completed]

    if not pending:
        print("  All years up to date")
        return

    print(f"  Fetching {len(pending)} years...")

    for i, year in enumerate(pending, 1):
        print(f"    [{i}/{len(pending)}] Fetching {year}...")

        url = f"{JODI_PRIMARY_BASE_URL}/{year}.csv"
        response = get(url)
        response.raise_for_status()

        save_raw_file(response.text, f"oil_{year}", extension="csv")

        completed.add(year)
        save_state("jodi_oil", {"completed": list(completed)})


def ingest_jodi_secondary():
    """Fetch JODI Oil secondary CSV data."""
    print("\n--- JODI Oil Secondary ---")

    state = load_state("jodi_oil_secondary")
    completed = set(state.get("completed", []))

    years = [str(y) for y in range(START_YEAR, END_YEAR + 1)]
    pending = [y for y in years if y not in completed]

    if not pending:
        print("  All years up to date")
        return

    print(f"  Fetching {len(pending)} years...")

    for i, year in enumerate(pending, 1):
        print(f"    [{i}/{len(pending)}] Fetching {year}...")

        url = f"{JODI_SECONDARY_BASE_URL}/{year}.csv"
        response = get(url)
        response.raise_for_status()

        save_raw_file(response.text, f"oil_secondary_{year}", extension="csv")

        completed.add(year)
        save_state("jodi_oil_secondary", {"completed": list(completed)})


# =============================================================================
# Main
# =============================================================================

def run():
    """Ingest all energy data sources."""
    print("Ingesting energy data...")

    ingest_ember()
    ingest_iea()
    ingest_irena()
    ingest_jodi_primary()
    ingest_jodi_secondary()

    print("\nIngestion complete")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
