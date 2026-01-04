"""Energy Data Connector

Aggregates energy and electricity data from multiple sources:

- Ember: Global electricity generation, capacity, and prices
- IEA: Monthly electricity statistics
- IRENA: Renewable energy capacity and generation
- JODI: Oil production and consumption statistics
"""

import argparse
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment
from ingest import ember as ingest_ember
from ingest import iea_electricity as ingest_iea
from ingest import irenastat as ingest_irena
from ingest import jodi_oil as ingest_jodi_primary
from ingest import jodi_oil_secondary as ingest_jodi_secondary
from transforms import global_electricity as transform_global
from transforms import india as transform_india
from transforms import prices as transform_prices


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")

        print("\n--- Ember Electricity ---")
        ingest_ember.run()

        print("\n--- IEA Monthly Electricity ---")
        ingest_iea.run()

        print("\n--- IRENA Renewable Capacity ---")
        ingest_irena.run()

        print("\n--- JODI Oil Primary ---")
        ingest_jodi_primary.run()

        print("\n--- JODI Oil Secondary ---")
        ingest_jodi_secondary.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")

        print("\n--- Ember Global Electricity ---")
        transform_global.main.run()

        print("\n--- Ember India Electricity ---")
        transform_india.main.run()

        print("\n--- Ember European Prices ---")
        transform_prices.main.run()


if __name__ == "__main__":
    main()
