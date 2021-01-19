import logging
import argparse
from frocket.common.tasks import registration
from frocket.common.config import config
from frocket.common.tasks.registration import DatasetValidationMode, RegisterArgs
from frocket.invoker import invoker_api

# TODO Embed this in CLI & API, rather than standalone utility

config.init_logging()
logger = logging.getLogger(__name__)

# noinspection PyTypeChecker
parser = argparse.ArgumentParser(
    description='Register a dataset for querying with Funnel Rocket',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('name', type=str,
                    help='Dataset name')
parser.add_argument('basepath', type=str,
                    help='The path that all files in the dataset are under, local and "s3://..." paths are supported')
parser.add_argument('group_id_column', type=str,
                    help='The column name to group rows by, e.g. "userId", "userHash", etc. '
                         'This column is required and no values can be missing. Each file in the '
                         'dataset should have a distinct set of group_id values.')
parser.add_argument('timestamp_column', type=str,
                    help='The column holding the timestamp of each row, e.g. "timestamp" or "ts"')
parser.add_argument('--pattern', type=str, default=registration.REGISTER_DEFAULT_FILENAME_PATTERN,
                    help='Filename pattern')
validation_mode_choices = [e.value for e in DatasetValidationMode]
parser.add_argument('--validate', type=str,
                    default=registration.REGISTER_DEFAULT_VALIDATION_MODE.value,
                    help=f"Validation to apply {validation_mode_choices}")
parser.add_argument('--skip-uniques', action='store_true', default=not registration.REGISTER_DEFAULT_VALIDATE_UNIQUES,
                    help='Skip validation of group_id values uniqueness across files '
                         '(set of files is determined by --validate argument)')

args = parser.parse_args()
validation_mode = DatasetValidationMode[args.validate.upper()]
register_args = RegisterArgs(name=args.name,
                             basepath=args.basepath,
                             group_id_column=args.group_id_column,
                             timestamp_column=args.timestamp_column,
                             pattern=args.pattern,
                             validation_mode=validation_mode,
                             validate_uniques=not args.skip_uniques)
invoker_api.register_dataset(register_args)
