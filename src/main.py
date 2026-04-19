"""Energy Data - Ember electricity generation, capacity, emissions and prices."""
from subsets_utils import load_nodes, validate_environment


def main():
    validate_environment()
    workflow = load_nodes()
    workflow.run()


if __name__ == "__main__":
    main()
