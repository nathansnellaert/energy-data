"""Ingest JODI Oil data from jodidata.org."""

from subsets_utils import get, save_raw_file, load_state, save_state

BASE_URL = "https://www.jodidata.org/_resources/files/downloads/oil-data/annual-csv/primary"

# Oil data available from 2002 to 2025
START_YEAR = 2002
END_YEAR = 2025


def run():
    """Fetch JODI Oil CSV data for all years."""
    state = load_state("jodi_oil")
    completed = set(state.get("completed", []))

    years = [str(y) for y in range(START_YEAR, END_YEAR + 1)]
    pending = [y for y in years if y not in completed]

    if not pending:
        print("  JODI Oil: All years up to date")
        return

    print(f"  JODI Oil: Fetching {len(pending)} years...")

    for i, year in enumerate(pending, 1):
        print(f"    [{i}/{len(pending)}] Fetching {year}...")

        url = f"{BASE_URL}/{year}.csv"
        response = get(url)
        response.raise_for_status()

        save_raw_file(response.text, f"oil_{year}", extension="csv")

        completed.add(year)
        save_state("jodi_oil", {"completed": list(completed)})

        print(f"      -> saved oil_{year}.csv")

    print(f"  JODI Oil: Done ({len(years)} years)")
