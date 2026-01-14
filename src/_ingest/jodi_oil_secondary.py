"""Ingest JODI Oil Secondary data from jodidata.org.

Secondary oil data covers refined petroleum products (gasoline, diesel, kerosene, etc.)
as opposed to primary data which covers crude oil and NGL.
"""

from subsets_utils import get, save_raw_file, load_state, save_state

BASE_URL = "https://www.jodidata.org/_resources/files/downloads/oil-data/annual-csv/secondary"

# Secondary oil data available from 2002 to 2025 (same as primary)
START_YEAR = 2002
END_YEAR = 2025


def run():
    """Fetch JODI Oil Secondary CSV data for all years."""
    state = load_state("jodi_oil_secondary")
    completed = set(state.get("completed", []))

    years = [str(y) for y in range(START_YEAR, END_YEAR + 1)]
    pending = [y for y in years if y not in completed]

    if not pending:
        print("  JODI Oil Secondary: All years up to date")
        return

    print(f"  JODI Oil Secondary: Fetching {len(pending)} years...")

    for i, year in enumerate(pending, 1):
        print(f"    [{i}/{len(pending)}] Fetching {year}...")

        url = f"{BASE_URL}/{year}.csv"
        response = get(url)
        response.raise_for_status()

        save_raw_file(response.text, f"oil_secondary_{year}", extension="csv")

        completed.add(year)
        save_state("jodi_oil_secondary", {"completed": list(completed)})

        print(f"      -> saved oil_secondary_{year}.csv")

    print(f"  JODI Oil Secondary: Done ({len(years)} years)")
