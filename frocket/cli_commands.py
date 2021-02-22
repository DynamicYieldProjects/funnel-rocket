import argparse
import json
import sys
from json.decoder import JSONDecodeError
from pathlib import Path
from tabulate import tabulate
from frocket.common.config import config
from frocket.common.tasks.base import BaseApiResult
from frocket.common.tasks.registration import DatasetValidationMode, RegisterArgs
from frocket.invoker import invoker_api

DATE_FORMAT = '%Y-%m-%d %H:%M:%S %Z'


def run_command(cmd: str, args: argparse.Namespace):
    mapping = {
        'register': register_dataset_cmd,
        'unregister': unregister_dataset_cmd,
        'list': list_datasets_cmd,
        'run': run_query_cmd,
        'info': dataset_info_cmd,
        'config': show_config_cmd
    }
    mapping[cmd](args)


def fail_missing_dataset(name: str):
    sys.exit(f"Dataset '{name}' not found!")


def trim_column(s: str, args: argparse.Namespace, maxwidth: int) -> str:
    if args.notrim or len(s) <= maxwidth:
        return s
    else:
        return s[:maxwidth - 3] + '...'


def handle_api_result(res: BaseApiResult):
    print("Result:", res.to_json(indent=2))
    if res.success:
        print('Done!')
    else:
        sys.exit('FAILED')


def register_dataset_cmd(args):
    validation_mode = DatasetValidationMode[args.validation.upper()]
    register_args = RegisterArgs(name=args.name,
                                 basepath=args.basepath,
                                 group_id_column=args.group_id_column,
                                 timestamp_column=args.timestamp_column,
                                 pattern=args.pattern,
                                 validation_mode=validation_mode,
                                 validate_uniques=not args.skip_uniques)
    print(register_args.to_json(indent=2))
    res = invoker_api.register_dataset(register_args)
    handle_api_result(res)


def unregister_dataset_cmd(args):
    res = invoker_api.unregister_dataset(args.dataset, force=args.force)
    handle_api_result(res)


def list_datasets_cmd(args):
    datasets = sorted(invoker_api.list_datasets(), key=lambda ds: ds.id.registered_at, reverse=True)
    if len(datasets) == 0:
        print('No datasets registered yet')
    else:
        rows = [{'name': trim_column(ds.id.name, args, maxwidth=30),
                 'registered at': ds.id.registered_at.strftime(DATE_FORMAT),
                 'parts': ds.total_parts,
                 'group id': ds.group_id_column,
                 'timestamp': ds.timestamp_column,
                 'path': trim_column(ds.basepath, args, maxwidth=50)}
                for ds in datasets]
        print(tabulate(rows, headers='keys'))


def json_parse(s: str) -> dict:
    try:
        return json.loads(s)
    except JSONDecodeError as e:
        sys.exit(f'JSON Error: {e}')


def run_query_cmd(args):
    ds_info = invoker_api.get_dataset(args.dataset)
    if not ds_info:
        fail_missing_dataset(args.dataset)
    query = None
    if args.empty:
        query = {}
    elif args.query_string:
        query = json_parse(args.query_string)
    elif args.filename:
        filepath = Path(args.filename)
        if not filepath.exists():
            sys.exit(f'File not found: {args.filename}')
        else:
            query_str = filepath.read_text(encoding='utf-8')
            query = json_parse(query_str)
    else:
        sys.exit('Unknown mode')

    try:
        res = invoker_api.run_query(ds_info, query)
        handle_api_result(res)
    except Exception as e:
        sys.exit(f'Error: {e}')


def dataset_info_cmd(args):
    show_full = args.full
    ds_info = invoker_api.get_dataset(args.dataset)
    if not ds_info:
        fail_missing_dataset(args.dataset)
    parts_info = invoker_api.get_dataset_parts(ds_info)
    schema_info = invoker_api.get_dataset_schema(ds_info, full=show_full)
    print('Basic information:', ds_info.to_json(indent=2))
    print('Parts:', parts_info.to_json(indent=2))
    print(f'Schema (full: {show_full}):', schema_info.to_json(indent=2))


def show_config_cmd(args):
    print('Configuration:')
    print(json.dumps(config, indent=2))
