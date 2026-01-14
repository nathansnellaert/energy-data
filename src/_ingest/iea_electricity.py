from subsets_utils import get, save_raw_json


def run():
    """Fetch all IEA monthly electricity statistics."""
    print("Fetching IEA monthly electricity statistics...")

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

    print("Ingestion complete")
