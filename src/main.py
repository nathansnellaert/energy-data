"""Energy Data connector - dynamically discovers and runs all nodes."""
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import load_nodes, validate_environment


def main():
    validate_environment()
    workflow = load_nodes()
    workflow.run()


if __name__ == "__main__":
    main()
