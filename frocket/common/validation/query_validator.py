import argparse
import json
import logging
import sys
from typing import List, Union
import difflib
import jsonschema
from frocket.common.helpers.utils import terminal_green, terminal_red
from frocket.common.validation.result import QueryValidationResult
from frocket.common.validation.error import ValidationErrorKind, QueryValidationError
from frocket.datastore.registered_datastores import get_datastore
from frocket.common.dataset import DatasetColumnType, DatasetInfo, DatasetShortSchema
from frocket.common.validation.consts import QUERY_SCHEMA, AGGREGATIONS_PATHS, SINGLE_FILTER_PATHS, \
    FILTER_ARRAY_PATHS, VALID_IDENTIFIER_PATTERN, UNIQUE_IDENTIFIER_SCOPES, OPERATORS_BY_COLTYPE, \
    VALUE_TYPES_BY_COLTYPE, NUMERIC_COLTYPES, RELATION_OPS, DEFAULT_RELATION_OP, CONDITION_COLUMN_PREFIX, \
    map_condition_names
from frocket.common.validation.relation_parser import RelationParser
from frocket.common.validation.visitor_functions import _to_verbose_filter, _to_verbose_target, _add_default_target, \
    _validate_aggregation, _expand_aggregations, _validate_or_set_include_zero
from frocket.common.validation.path_visitor import PathVisitor, PathVisitorCallback
from frocket.engine.relation_to_pandas import relation_to_pandas_query

logger = logging.getLogger(__name__)


# TODO doc asserts
class QueryValidator:
    """
    Validate a query, and *expand* as needed:

    The query schema as documented externally allows for various shorthand notations and for some "sensible" defaults
    to be ommitted. However, the query engine itself expects the full notation. To hide this complexity from the engine,
    all transformations are done here, and the 'expanded' query is returned in the result -
    that's the version that should be passed to query tasks rather than the user-supplied one.

    'assert' is used in various places in code where some conditions are expected to be already validated, but if not -
    we want the code to crash.
    """

    def __init__(self, source_query: dict, dataset: DatasetInfo = None, short_schema: DatasetShortSchema = None):
        """Unless calling expand_and_validate() in schema_only=True more, dataset and its schema must be passed."""
        self._source_query = source_query
        self._expanded_query = None
        self._dataset = dataset
        self._short_schema = short_schema
        self._used_columns = None
        self._condition_mapping = None
        self._used_conditions = None
        self._relation_elements = None

    def expand_and_validate(self, schema_only: bool = False) -> QueryValidationResult:
        """
        Validates and applies any transformations.
        In schema_only=True mode, only 'static' validation is done, meaning the existence of columns and their types
        is not checked against the actual dataset the user wishes to query.
        """
        try:
            if not schema_only and not (self._dataset and self._short_schema):
                raise QueryValidationError(
                    message="Provide a dataset and its short schema, or use schema_only=True",
                    kind=ValidationErrorKind.INVALID_ARGUMENTS)

            # First, validate using the JSON schema.
            try:
                jsonschema.validate(instance=self._source_query, schema=QUERY_SCHEMA)
            except jsonschema.exceptions.ValidationError as ve:
                raise QueryValidationError(message=ve.message, kind=ValidationErrorKind.SCHEMA)

            # Second, perform all validations & expansions which do not need the dataset details.
            # Clone before making changes (using serialization, which may also catch unexpected types in the dict)
            self._expanded_query = json.loads(json.dumps(self._source_query))
            self._validate_and_expand_schema()

            # Third, perform validations given the specific dataset and its schema
            if not schema_only:
                self._validate_columns()
                self._validate_timeframe(dataset_mintime=self._short_schema.min_timestamp,
                                         dataset_maxtime=self._short_schema.max_timestamp)

            return QueryValidationResult(success=True,
                                         source_query=self._source_query,
                                         expanded_query=self._expanded_query,
                                         used_columns=self._used_columns,
                                         used_conditions=self._used_conditions,
                                         named_conditions=self._condition_mapping.names,
                                         relation_elements=self._relation_elements,
                                         warnings=None)  # TODO backlog: support warnings/hints to the user
        except Exception as e:
            if not isinstance(e, QueryValidationError):
                logger.exception("Unexpected error")
            return QueryValidationResult.from_exception(e, self._source_query)

    def _validate_and_expand_schema(self):
        # Convert shorthand filter notation (a list) to verbose notation (dict) in single-filter conditions
        # and in sequence steps (both under sequence conditions or funnel).
        # (in filter arrays, filters are defined in schema to always be in verbose notation)
        self._modify(paths=SINGLE_FILTER_PATHS, list_to_items=False, func=_to_verbose_filter)

        # Convert shorthand target notation (a list) to verbose (dict)
        self._modify(paths='query.conditions.target', list_to_items=False, func=_to_verbose_target)

        # Put default target in conditions where it's omitted
        self._visit(paths='query.conditions', func=_add_default_target)

        # Expand 'default' column aggregation (with no declared type) to actual list of aggregations
        self._modify(paths=AGGREGATIONS_PATHS, list_to_items=False, func=_expand_aggregations)

        # Check value of 'includeZero' optional flag, and its applicability. Explicitly set where needed.
        self._visit(paths='query.conditions', func=_validate_or_set_include_zero)

        # Validate that only aggregation requiring otherColumn have them
        self._visit(paths=AGGREGATIONS_PATHS, func=_validate_aggregation)

        # Ensure uniquene names for condition column aggregations
        for scope in UNIQUE_IDENTIFIER_SCOPES:
            self._validate_unique_identifiers(scope)

        # Get total condition count, map named condition to indexes
        self._validate_condition_names()
        # Set a default relation if omitted, expand a shorthand relation,
        # and then fully parse and transform the relation
        self._expand_and_parse_relation()

        # Validate optional timeframe, first without dataset boundaries
        self._validate_timeframe()

        # Self-test that our own modifications did not trash the schema somehow
        jsonschema.validate(instance=self._expanded_query, schema=QUERY_SCHEMA)

    def _visit(self, paths: Union[str, List[str]],
               func: PathVisitorCallback,
               modifiable: bool = False,
               list_to_items: bool = True):
        if type(paths) is str:
            paths = [paths]
        for path in paths:
            p = PathVisitor(root=self._expanded_query, path=path, modifiable=modifiable, list_to_items=list_to_items)
            p.visit(func)

    def _modify(self, paths: Union[str, List[str]], list_to_items: bool, func: PathVisitorCallback) -> None:
        self._visit(paths, modifiable=True, list_to_items=list_to_items, func=func)

    def _collect(self, paths: Union[str, List[str]], list_to_items: bool = True) -> list:
        results = []
        self._visit(paths, modifiable=False, list_to_items=list_to_items, func=lambda v: results.append(v))
        return results

    def _validate_unique_identifiers(self, scope: str) -> None:
        def add_identifier(v):
            if v:
                if v in seen_values:
                    raise QueryValidationError(message=f"Identifier name '{v}' is not unique in scope '{scope}'",
                                               kind=ValidationErrorKind.SCHEMA)
                elif not VALID_IDENTIFIER_PATTERN.match(v):
                    raise QueryValidationError(message=f"Invalid identifier name '{v}'",
                                               kind=ValidationErrorKind.SCHEMA)
                seen_values.add(v)

        seen_values = set()
        self._visit(paths=scope, modifiable=False, list_to_items=True, func=add_identifier)

    def _validate_columns(self):
        # As a baseline, these columns are always needed for the query to work
        used_columns = [self._dataset.group_id_column, self._dataset.timestamp_column]

        # Collect all filter objects and validate:
        # (a) column existence, (b) suitability of operator and type to the column type
        all_filters = self._collect(paths=SINGLE_FILTER_PATHS, list_to_items=False) + \
            self._collect(paths=FILTER_ARRAY_PATHS, list_to_items=True)

        for f in all_filters:
            self._validate_column(name=f['column'], op=f['op'], value=f['value'])
            used_columns.append(f['column'])

        # Collect column names from aggregations
        for name in self._collect(paths=[f"{path}.column" for path in AGGREGATIONS_PATHS]):
            self._validate_column(name)
            used_columns.append(name)

        # Collect column name referenced in (a) targets of type 'sum' and (b) aggregations of type 'sumPerValue',
        # these must be numeric columns.
        sum_by_column_paths = ["query.conditions.target.column"] + \
                              [f"{path}.otherColumn" for path in AGGREGATIONS_PATHS]
        sum_by_columns = self._collect(paths=sum_by_column_paths)
        for name in sum_by_columns:
            self._validate_column(name=name, expected_types=NUMERIC_COLTYPES)
            used_columns.append(name)

        self._used_columns = list(set(used_columns))

    def _validate_column(self,
                         name: str,
                         expected_types: List[DatasetColumnType] = None,
                         op: str = None,
                         value: Union[int, float, str, bool] = None):
        coltype = self._short_schema.columns.get(name, None)
        if not coltype:
            raise QueryValidationError(f"Column '{name}' not in dataset", kind=ValidationErrorKind.DATASET_MISMATCH)

        if expected_types and coltype not in expected_types:
            raise QueryValidationError(f"Column '{name}' is of type {coltype.value}, but should be one of "
                                       f"{[t.value for t in expected_types]}", kind=ValidationErrorKind.TYPE_MISMATCH)

        if op:
            if op not in OPERATORS_BY_COLTYPE[coltype]:
                raise QueryValidationError(f"Operator '{op}' is not applicable for column '{name}' "
                                           f"of type {coltype.value}", kind=ValidationErrorKind.TYPE_MISMATCH)
        if value is not None:
            assert op  # Why was op not passed?
            if type(value) not in VALUE_TYPES_BY_COLTYPE[coltype]:
                value_as_str = (f'"{value}"' if type(value) is str else str(value)) + \
                               f" (type {type(value).__name__.upper()})"
                raise QueryValidationError(f"Value {value_as_str} "
                                           f"is not applicable for column '{name}' of type {coltype.value}",
                                           kind=ValidationErrorKind.TYPE_MISMATCH)

    def _validate_condition_names(self):
        self._condition_mapping = map_condition_names(self._expanded_query)
        for name in self._condition_mapping.names.keys():
            if name in RELATION_OPS:
                raise QueryValidationError(f"'{name}' is not a valid condition name",
                                           kind=ValidationErrorKind.SCHEMA)

    def _expand_and_parse_relation(self):
        relation = None
        simple_op = None
        found_relations = self._collect('query.relation')
        assert len(found_relations) in [0, 1]

        if not found_relations:
            if not self._collect('query.conditions'):
                return
            else:  # There are conditions under 'query', but no relation was defined - set default one
                simple_op = DEFAULT_RELATION_OP
                self._expanded_query['query']['relation'] = simple_op
        else:
            relation = found_relations[0].strip().lower()
            if relation in RELATION_OPS:
                simple_op = relation

        if simple_op:
            all_condition_ids = [f"${i}" for i in range(self._condition_mapping.count)]
            relation = f" {simple_op} ".join(all_condition_ids)
            self._expanded_query['query']['relation'] = relation

        if len(relation) == 0:
            raise QueryValidationError("Relation cannot be an empty string", kind=ValidationErrorKind.RELATION)

        parser = RelationParser(self._expanded_query)
        self._relation_elements = parser.parse()
        # TODO return a warning on unused conditions (when doing warnings mechanism)
        self._used_conditions = parser.used_conditions

    def _validate_timeframe(self, dataset_mintime: float = None, dataset_maxtime: float = None):
        """
        If a timeframe object is included in the query (either from, to, or both), validate that to > from,
        but also that the timestamps seem to be consistent with the dataset's timestamps.
        Currently, Funnel Rocket itself is not opinionated re. the timestamp resolution (seconds? milliseconds? second
        fractions as decimal places after the dot?), and only validates that the no. of digits seems ok.
        TODO backlog (optional?) strict timestamp specification
        TODO backlog when warnings are supported, warn client when query timeframe is fully outside dataset timeframe
        """
        def validate_scale(ts_in_query, ts_in_dataset, dataset_ts_name: str):
            if len(str(int(ts_in_query))) != len(str(int(ts_in_dataset))):
                message = f"Given timestamp {ts_in_query} doesn't appear to be in same scale as " \
                          f"{dataset_ts_name} ({ts_in_dataset}). Query timeframe is: {timeframe}"
                raise QueryValidationError(message, kind=ValidationErrorKind.TYPE_MISMATCH)

        timeframe = self._expanded_query.get('timeframe', None)
        if not timeframe:
            return

        fromtime = timeframe.get('from', None)
        totime = timeframe.get('to', None)

        if fromtime is not None and dataset_mintime is not None:
            validate_scale(fromtime, dataset_mintime, 'minimum timestamp in dataset')
        if totime is not None and dataset_maxtime is not None:
            validate_scale(totime, dataset_maxtime, 'maximum timestamp in dataset')

        if fromtime is not None and totime is not None:
            if totime <= fromtime:
                raise QueryValidationError(f"Value of 'to' (exclusive) should be larger than 'from' (inclusive) "
                                           f"in query timeframe: {timeframe}",
                                           kind=ValidationErrorKind.SCHEMA)

    def _fail_if_not_ran(self):
        if not self._expanded_query:
            raise Exception("Run validation first")

    def diff_from_source(self, colorize: bool = False) -> List[str]:
        """Handy utility for manual tests: show a nice diff between given and expanded queries, just like Git."""
        self._fail_if_not_ran()
        diff = difflib.unified_diff(
            a=json.dumps(self._source_query, indent=2).splitlines(),
            b=json.dumps(self._expanded_query, indent=2).splitlines(),
            fromfile="Source query",
            tofile="Expanded query")

        lines = []
        for curr_line in diff:
            line = curr_line
            if colorize:
                if line.startswith('+ '):
                    line = terminal_green(line)
                elif line.startswith('- '):
                    line = terminal_red(line)
            lines.append(line)
        return lines


# For running validations manually, including detailed diff's (source vs. expanded query)
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Manually run QueryValidator')
    parser.add_argument('dataset', type=str, help='Dataset name')
    parser.add_argument('query_file', type=str, help='Path to file with the query to validate')
    parser.add_argument('--diff', action='store_true', help='Show diff between original and expanded query')

    args = parser.parse_args()
    source_query = json.load(open(args.query_file, 'r'))

    store = get_datastore()
    dataset = store.dataset_info(args.dataset)
    short_schema = store.short_schema(dataset)

    validator = QueryValidator(source_query, dataset, short_schema)
    result = validator.expand_and_validate()
    if not result.success:
        sys.exit(f"{terminal_red('Validation failed!')}\n{result.error_message}")

    print(terminal_green("Validation successful!"))
    if args.diff:
        diff = validator.diff_from_source(colorize=True)
        for line in diff:
            print(line)

    print("===========================================================================================\n"
          f"Used columns: {result.used_columns}")
    print(f"Named conditions: {result.named_conditions}")
    print(f"Used conditions: {result.used_conditions}")
    print(f"Original relation: {PathVisitor(source_query, 'query.relation').list()[0]}")
    pandas_relation = \
        relation_to_pandas_query(elements=result.relation_elements, column_prefix=CONDITION_COLUMN_PREFIX)
    print(f"...........parsed: {pandas_relation}")
