"""Ingest IRENA renewable energy statistics via PxWeb API.

Data source: https://pxweb.irena.org/pxweb/en/IRENASTAT/

API limits: 100,000 cells max per request, 10 calls per 10 seconds.

Tables:
- Power Capacity and Generation:
  - Country_ELECSTAT_2025_H2_PX.px - Electricity stats by country/area
  - Region_ELECSTAT_2025_H2_PX.px - Electricity stats by region
  - RE-SHARE_2025_H2_PX.px - Renewable energy share
- Finance:
  - PUBFIN_2025_H2_PX.px - Public investments
- Heat Generation:
  - HEATGEN_2025_cycle2_PX.px - Heat generation by country
"""

import time
from subsets_utils import get, post, save_raw_json, load_state, save_state


BASE_URL = "https://pxweb.irena.org/api/v1/en/IRENASTAT"

TABLES = {
    "electricity_by_country": "Power Capacity and Generation/Country_ELECSTAT_2025_H2_PX.px",
    "electricity_by_region": "Power Capacity and Generation/Region_ELECSTAT_2025_H2_PX.px",
    "renewable_share": "Power Capacity and Generation/RE-SHARE_2025_H2_PX.px",
    "public_investments": "Finance/PUBFIN_2025_H2_PX.px",
    "heat_generation": "Heat Generation/HEATGEN_2025_cycle2_PX.px",
}

MAX_CELLS = 100_000


def get_table_metadata(table_path: str) -> dict:
    """Fetch metadata for a PxWeb table (variable codes and values)."""
    url = f"{BASE_URL}/{table_path}"
    response = get(url, timeout=30.0)
    response.raise_for_status()
    return response.json()


def calculate_total_cells(metadata: dict) -> int:
    """Calculate total cells if all data is fetched."""
    total = 1
    for var in metadata["variables"]:
        total *= len(var["values"])
    return total


def fetch_and_save_table(name: str, table_path: str):
    """Fetch table data, chunking by year if needed, and save each chunk."""
    url = f"{BASE_URL}/{table_path}"
    metadata = get_table_metadata(table_path)
    time.sleep(1.1)  # Rate limit after metadata request
    total_cells = calculate_total_cells(metadata)

    if total_cells <= MAX_CELLS:
        # Small enough to fetch in one request
        query = {"query": [], "response": {"format": "json-stat2"}}
        response = post(url, json=query, timeout=120.0)
        response.raise_for_status()
        data = response.json()
        save_raw_json(data, name, compress=True)
        time.sleep(1.1)  # Rate limit after data request
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
        raise ValueError(f"Cannot find year variable to split query for {table_path}")

    years = year_var["values"]
    year_code = year_var["code"]
    year_labels = year_var.get("valueTexts", years)

    # Calculate cells per year
    cells_per_year = total_cells // len(years)

    # Determine chunk size (how many years per request)
    years_per_chunk = max(1, MAX_CELLS // cells_per_year)

    print(f"    -> {total_cells:,} total cells, fetching in chunks of {years_per_chunk} years")

    all_values = []
    base_response = None

    for i in range(0, len(years), years_per_chunk):
        chunk_years = years[i:i + years_per_chunk]

        query = {
            "query": [
                {
                    "code": year_code,
                    "selection": {
                        "filter": "item",
                        "values": chunk_years
                    }
                }
            ],
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

        # Rate limit: max 10 calls per 10 seconds
        time.sleep(1.1)

    # Update the base response with combined values and correct metadata
    base_response["value"] = all_values
    base_response["size"][year_idx] = len(years)

    # Update the year dimension with all years
    year_dim_name = base_response["id"][year_idx]
    year_dim = base_response["dimension"][year_dim_name]
    year_dim["category"]["index"] = {y: i for i, y in enumerate(years)}
    year_dim["category"]["label"] = {y: label for y, label in zip(years, year_labels)}

    save_raw_json(base_response, name, compress=True)
    return len(all_values)


def run():
    """Fetch all IRENA tables and save as raw JSON."""
    state = load_state("irenastat")
    completed = set(state.get("completed", []))

    pending = [(name, path) for name, path in TABLES.items() if name not in completed]

    if not pending:
        print("  All tables already fetched")
        return

    print(f"  Fetching {len(pending)} tables...")

    for i, (name, path) in enumerate(pending, 1):
        print(f"  [{i}/{len(pending)}] Fetching {name}...")

        count = fetch_and_save_table(name, path)

        completed.add(name)
        save_state("irenastat", {"completed": list(completed)})

        print(f"    -> {count:,} data points")
        print(f"    -> saved {name}")
