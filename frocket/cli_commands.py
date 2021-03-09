"""
implementation of CLI commands.
"""
import argparse
import json
import sys
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import Any
from tabulate import tabulate
from frocket.common.config import config
from frocket.common.serializable import SerializableDataClass
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
    if args.notrim or args.nopretty or len(s) <= maxwidth:
        return s
    else:
        return s[:maxwidth - 3] + '...'


def print_json(name: str, o: Any, pretty_print: bool):
    def to_json(o: Any, indent: int = None) -> str:
        return o.to_json(indent=indent) if isinstance(o, SerializableDataClass) else json.dumps(o, indent=indent)

    if pretty_print:
        print(name + ':', to_json(o, indent=2))
    else:
        print(to_json(o))


def handle_api_result(res: BaseApiResult, pretty_print: bool):
    print_json('API Result', res, pretty_print)
    if not res.success:
        sys.exit('FAILED' if pretty_print else 1)


def register_dataset_cmd(args):
    validation_mode = DatasetValidationMode[args.validation.upper()]
    register_args = RegisterArgs(name=args.name,
                                 basepath=args.basepath,
                                 group_id_column=args.group_id_column,
                                 timestamp_column=args.timestamp_column,
                                 pattern=args.pattern,
                                 validation_mode=validation_mode,
                                 validate_uniques=not args.skip_uniques)
    res = invoker_api.register_dataset(register_args)
    handle_api_result(res, pretty_print=not args.nopretty)


def unregister_dataset_cmd(args):
    res = invoker_api.unregister_dataset(args.dataset, force=args.force)
    handle_api_result(res, pretty_print=not args.nopretty)


def list_datasets_cmd(args):
    datasets = sorted(invoker_api.list_datasets(), key=lambda ds: ds.id.registered_at, reverse=True)
    display_datasets = [{'name': trim_column(ds.id.name, args, maxwidth=30),
                         'registered at': ds.id.registered_at.strftime(DATE_FORMAT),
                         'parts': ds.total_parts,
                         'group id': ds.group_id_column,
                         'timestamp': ds.timestamp_column,
                         'path': trim_column(ds.basepath, args, maxwidth=50)}
                        for ds in datasets]
    if args.nopretty:
        print(json.dumps(display_datasets))
    else:
        if len(datasets) == 0:
            print('No datasets registered yet')
        else:
            print(tabulate(display_datasets, headers='keys'))


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
        handle_api_result(res, pretty_print=not args.nopretty)
    except Exception as e:
        sys.exit(f'Error: {e}')


def dataset_info_cmd(args):
    show_full = args.full
    ds_info = invoker_api.get_dataset(args.dataset)
    if not ds_info:
        fail_missing_dataset(args.dataset)
    parts_info = invoker_api.get_dataset_parts(ds_info)
    schema_info = invoker_api.get_dataset_schema(ds_info, full=show_full)
    print_json('Basic information', ds_info, pretty_print=not args.nopretty)
    print_json('Parts', parts_info, pretty_print=not args.nopretty)
    print_json(f'Schema (full: {show_full})', schema_info, pretty_print=not args.nopretty)


def show_config_cmd(args):
    print_json(f'Configuration', config, pretty_print=not args.nopretty)
