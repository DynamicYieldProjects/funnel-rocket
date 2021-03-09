"""
Simple CLI for Funnel Rocket.

This is currently a wrapper over invoker_api directly (meaning that the CLI process is the invoker), rather than
calling an API server - meaning that it does not rely on a running server, but needs the same permissions (listing files
in remote storage, access to Redis as datastore, optionally being able to invoke Lambdas).

This makes the CLI more suitable for onboarding and evaluation, but in production it's preferable to use the API
(for a better permissions model and centralized monitoring/logging, if nothing else).

The CLI does provide a few optional flags which make it also suitable for automating jobs:
* --nopretty returns JSON object/s without any captions
* --notrim and --nocolor prevents data from bein shortened or surrounded by ANSI color codes
* The log level is controllbable, and all log lines have a prefix making them easy to ignore.
"""
import argparse
# TODO backlog don't import any frocket modules but a carefully selected set which does not then import heavy packages
#  or initialize mechanisms. This is only partially done now (see import at end of file).
from frocket.common.config import config
from frocket.common.tasks.registration import DatasetValidationMode, REGISTER_DEFAULT_VALIDATION_MODE, \
    REGISTER_DEFAULT_FILENAME_PATTERN, REGISTER_DEFAULT_VALIDATE_UNIQUES

REGISTER_VALIDATION_MODE_CHOICES = [e.value.lower() for e in DatasetValidationMode]
LOG_LEVEL_CHOICES = ['debug', 'info', 'warning', 'error', 'critical']
LOG_LINE_PREFIX = '[Log '
LOG_FORMAT = LOG_LINE_PREFIX + '%(levelname)s %(name)s] %(message)s'


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Simple CLI for Funnel Rocket',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--notrim', action='store_true', help='Don\'t trim any text')
    parser.add_argument('--nocolor', action='store_true', help='Don\'t trim colorize any text')
    parser.add_argument('--nopretty', action='store_true', help='Don\'t pretty-print the response')
    parser.add_argument('--loglevel', type=str.lower, choices=LOG_LEVEL_CHOICES,
                        help=f'Set log level {LOG_LEVEL_CHOICES}')
    subparsers = parser.add_subparsers(dest='command', title='commands')
    subparsers.required = True

    register_parser = subparsers.add_parser('register', help='Register a dataset',
                                            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    register_parser.add_argument('name', type=str, help='Dataset name')
    register_parser.add_argument('basepath', type=str,
                                 help='The path all files are directly under. Local and s3://... paths supported.')
    register_parser.add_argument('group_id_column', type=str,
                                 help='The column to group rows by, e.g. "userId", "userHash". '
                                      'This column is required and no values can be missing. Each part (file) in the '
                                      'dataset should have a distinct set of values for this column.')
    register_parser.add_argument(
        'timestamp_column', type=str,
        help='The column holding the timestamp of each row, e.g. "timestamp", "ts". '
             'Must be a numeric column with no values missing. Using a unix timestamp is advised - '
             'with or without sub-second resoluton based on your needs, either as int or float.')
    register_parser.add_argument('--pattern', type=str, default=REGISTER_DEFAULT_FILENAME_PATTERN,
                                 help='Filename pattern. Sub-directories are currently not supported.')
    register_parser.add_argument('--validation', type=str.lower,
                                 choices=REGISTER_VALIDATION_MODE_CHOICES,
                                 default=REGISTER_DEFAULT_VALIDATION_MODE.value.lower(),
                                 help=f"Validation mode to use {REGISTER_VALIDATION_MODE_CHOICES}",
                                 metavar='MODE')
    register_parser.add_argument('--skip-uniques', action='store_true',
                                 default=not REGISTER_DEFAULT_VALIDATE_UNIQUES,
                                 help='Skip validation of group_id_column values uniqueness across files '
                                      '(the set of files to test is determined by --validation argument)')

    list_parser = subparsers.add_parser('list', help='List datasets')

    run_query_parser = subparsers.add_parser('run', help='Run query')
    run_query_parser.add_argument('dataset')
    query_sources_group = run_query_parser.add_mutually_exclusive_group(required=True)
    query_sources_group.add_argument('--file', '-f', type=str, help='Run query stored in file', dest='filename')
    query_sources_group.add_argument('--empty', '-e', action='store_true',
                                     help='Run an empty query with no conditions')
    query_sources_group.add_argument('--string' '-s', type=str,
                                     help='Run the following query string', dest='query_string')

    info_parser = subparsers.add_parser('info', help='Show dataset information')
    info_parser.add_argument('dataset', type=str)
    info_parser.add_argument('--full', action='store_true', help='Show full schema')

    unreg_parser = subparsers.add_parser('unregister', help='Unregister a dataset')
    unreg_parser.add_argument('dataset', type=str)
    unreg_parser.add_argument('--force', action='store_true',
                              help='Unregister a dataset even if it\'s currently in use')

    config_parser = subparsers.add_parser('config', help='Show configuration')
    return parser


def run_from_args(args: argparse.Namespace):
    config['log.format'] = LOG_FORMAT if args.nocolor else f"\033[33m{LOG_FORMAT}\033[0m"
    if args.loglevel:
        config['log.level'] = args.loglevel
    config.init_logging(force_console_output=True)

    # invoker_api isn't loaded (or logging implicitly initialized) till arguments are validated and log level is set
    from frocket.cli_commands import run_command
    run_command(args.command, args)


if __name__ == '__main__':
    parser = build_parser()
    args = parser.parse_args()
    run_from_args(args)
