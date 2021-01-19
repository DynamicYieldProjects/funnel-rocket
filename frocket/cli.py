import argparse
import logging
import sys
from pathlib import Path
import os
import json
from tabulate import tabulate
from frocket.common.config import config
from frocket.invoker import invoker_api

# TODO convert this into a proper CLI (using the API or not? also, do proper sub-commands)

config.init_logging()
logger = logging.getLogger(__name__)

example_query_path = Path(os.path.dirname(__file__)) / 'resources/example_query.json'

# noinspection PyTypeChecker
parser = argparse.ArgumentParser(description='Funnel Rocket Temporary (yeah yeah...) CLI')
parser.add_argument('--run', nargs='+', metavar=('DATASET', 'QUERY_FILE'),
                    help="Run query over the given dataset. If QUERY_FILE is not specified, "
                         f"the example query {example_query_path} would be used")
parser.add_argument('--list', action='store_true',
                    help='List all registered datasets')
parser.add_argument('--config', action='store_true',
                    help='Show configuration')
args = parser.parse_args()

if args.config:
    logger.info(f"Configuration:\n{json.dumps(config, indent=2)}")

elif args.list:
    datasets = [ds.to_dict() for ds in invoker_api.list_datasets()]

    if len(datasets) == 0:
        logger.info('No datasets registered yet')
    else:
        logger.info("Registered datasets:\n" + tabulate(datasets, headers='keys'))

elif args.run:
    dataset_name = args.run[0]
    if len(args.run) > 2:
        sys.exit("Don't know how to handle multiple queries")
    elif len(args.run) == 2:
        query_path = args.run[1]
    else:
        logger.info('Using example query, note that the given dataset might not have all fields needed to run it')
        query_path = example_query_path

    query = json.load(open(query_path, 'r'))
    dataset = invoker_api.get_dataset(dataset_name)
    if not dataset:
        sys.exit(f"Dataset {dataset_name} not found!")

    run_result = invoker_api.run_query(dataset, query)
    logger.info(f"Result:\n{run_result.to_json(indent=2)}")

else:
    print("Don't know what to do")
    parser.print_help()
    sys.exit(1)
