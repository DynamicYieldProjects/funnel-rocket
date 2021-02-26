import os
import json
import time
import pytest
from frocket.common.validation.consts import RELATION_OPS, DEFAULT_AGGREGATIONS
from frocket.common.validation.query_validator import QueryValidator
from frocket.common.dataset import DatasetInfo, DatasetId, DatasetColumnType
from frocket.common.dataset import DatasetShortSchema
from frocket.common.validation.error import ValidationErrorKind
from frocket.engine.relation_to_pandas import relation_to_pandas_query
from tests.utils.base_test_utils import SKIP_SLOW_TESTS

ORIGINAL_QUERY_PATH = os.path.dirname(__file__) + '/utils/base_query_example.json'
ORIGINAL_QUERY_JSON = open(ORIGINAL_QUERY_PATH, 'r').read()
ORIGINAL_QUERY_USED_COLUMNS = ['userId', 'ts', 'goalId', 'goalValue', 'eventId', 'eventValue', 'eventType',
                               'device', 'transactionId']
ORIGINAL_QUERY_NAMED_CONDITIONS = {'made_multiple_purchases': 0, 'made_multiple_purchases2': 1, 'seq': 5}
ORIGINAL_QUERY_USED_CONDITIONS = [1, 2, 5, 0]
PANDAS_CONDITION_COL_PREFIX = '__c'  # If the actually-used value changes in the engine, it won't break the tests
ORIGINAL_QUERY_PARSED_RELATION = '(__c1 & __c2) | __c5 | ((__c0))'

TEST_DATASET = DatasetInfo(id=DatasetId.now("testme"), basepath='/dev/null', total_parts=1,
                           timestamp_column='ts', group_id_column='userId')


def query_clone() -> dict:
    return json.loads(ORIGINAL_QUERY_JSON)


def dict_clone(d: dict) -> dict:
    return json.loads(json.dumps(d))


q = query_clone()
TEST_DS_SCHEMA = DatasetShortSchema(
    columns={'userId': DatasetColumnType.INT, 'ts': DatasetColumnType.INT, 'eventId': DatasetColumnType.INT,
             'rowType': DatasetColumnType.STRING, 'eventType': DatasetColumnType.STRING,
             'eventValue': DatasetColumnType.FLOAT, 'goalValue': DatasetColumnType.FLOAT,
             'isCool': DatasetColumnType.BOOL, 'device': DatasetColumnType.STRING,
             'transactionId': DatasetColumnType.INT, 'goalId': DatasetColumnType.INT},
    min_timestamp=q['timeframe']['from'],
    max_timestamp=q['timeframe']['to']
)


def new_validator(query: dict = None, with_dataset: bool = True):
    query = query or query_clone()
    return QueryValidator(source_query=query,
                          dataset=(TEST_DATASET if with_dataset else None),
                          short_schema=(TEST_DS_SCHEMA if with_dataset else None))


def test_original_without_dataset_ok():
    validator = new_validator(with_dataset=False)
    result = validator.expand_and_validate(schema_only=True)
    print(result)
    assert result.success is True
    assert result.error_message is None
    assert result.error_kind is None
    assert result.named_conditions == ORIGINAL_QUERY_NAMED_CONDITIONS
    assert result.used_conditions == ORIGINAL_QUERY_USED_CONDITIONS
    transformed_relation = relation_to_pandas_query(elements=result.relation_elements,
                                                    column_prefix=PANDAS_CONDITION_COL_PREFIX)
    assert transformed_relation == ORIGINAL_QUERY_PARSED_RELATION


def test_used_columns_na():
    validator = new_validator(with_dataset=False)
    result = validator.expand_and_validate(schema_only=True)
    assert result.used_columns is None


def test_dataset_not_passed():
    validator = new_validator(with_dataset=False)
    result = validator.expand_and_validate()
    assert result.error_kind == ValidationErrorKind.INVALID_ARGUMENTS


def test_original_ok():
    validator = new_validator()
    result = validator.expand_and_validate()
    print(result)
    assert result.success is True
    assert result.error_message is None
    assert result.error_kind is None
    assert sorted(result.used_columns) == sorted(ORIGINAL_QUERY_USED_COLUMNS)
    assert result.named_conditions == ORIGINAL_QUERY_NAMED_CONDITIONS
    assert result.used_conditions == ORIGINAL_QUERY_USED_CONDITIONS
    transformed_relation = relation_to_pandas_query(elements=result.relation_elements,
                                                    column_prefix=PANDAS_CONDITION_COL_PREFIX)
    assert transformed_relation == ORIGINAL_QUERY_PARSED_RELATION


def test_original_not_modified():
    orig_query = query_clone()
    passed_query = query_clone()
    validator = new_validator(passed_query)
    result = validator.expand_and_validate()
    assert validator
    assert passed_query == orig_query
    assert passed_query != result.expanded_query


def test_multipass():
    v1 = new_validator()
    res1 = v1.expand_and_validate()
    assert res1.success
    # Now, ensure that feeding the expanded query back into the validator will result in the same expanded query
    # Create a clone to ensure the expanded query dict is not modified when goes into the second validator as source,
    # thus quietly making the test useless...
    expanded1_clone = dict_clone(res1.expanded_query)

    v2 = new_validator(res1.expanded_query)
    res2 = v2.expand_and_validate()
    assert res2.success

    assert res2.expanded_query == expanded1_clone


@pytest.mark.skipif(SKIP_SLOW_TESTS, reason="Skipping slow tests")
def test_many_runs():
    iterations = 50
    max_avg_iteration_time = 0.03
    max_total_time = max_avg_iteration_time * iterations

    q = query_clone()
    start_time = time.time()
    for i in range(iterations):
        res = new_validator(q).expand_and_validate()
        assert res.success

    total_time = time.time() - start_time
    print(f"Time for {iterations} runs: {total_time} seconds")
    assert total_time <= max_total_time


def test_empty():
    v = new_validator({})
    res = v.expand_and_validate()
    assert res.success


def test_additional_property():
    v = new_validator({'hello': 'world'})
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.SCHEMA

    q1 = query_clone()
    q1['query']['conditions'][0]['hello'] = 'world'
    v = new_validator(q1)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.SCHEMA


def test_no_conditions():
    q = {'query': {}}
    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success and res.used_conditions is None

    q = {'query': {'conditions': []}}
    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success and res.used_conditions is None


def condition(d: dict, idx: int) -> dict:
    return d['query']['conditions'][idx]


def test_shorthand_filter():
    q = {
        'query': {
            'conditions': [
                {'filter': ['eventId', '==', 1]},
                {'filter': {'column': 'eventId', 'op': '==', 'value': 1}}
            ]
        }
    }
    full_filter = condition(q, 1)['filter']
    assert condition(q, 0)['filter'] != full_filter
    res = new_validator(q).expand_and_validate()
    assert res.success
    assert condition(res.expanded_query, 0)['filter'] == full_filter
    assert condition(res.expanded_query, 1)['filter'] == full_filter


def test_shorthand_and_default_target():
    simple_filter = ['eventId', '==', 1]
    q = {
        'query': {
            'conditions': [
                {'filter': simple_filter},  # No target
                {'filter': simple_filter,  # Shorthand count target
                 'target': ['count', '>=', 1]},
                {'filter': simple_filter,  # Full count target
                 'target': {'type': 'count', 'op': '>=', 'value': 1}},
                {'filter': simple_filter,  # Shorthand sum target
                 'target': ['sum', 'eventValue', '>=', 1]},
                {'filter': simple_filter,  # Full sum target
                 'target': {'type': 'sum', 'op': '>=', 'value': 1, 'column': 'eventValue'}}
            ]
        }
    }
    full_count_target = condition(q, 2)['target']
    full_sum_target = condition(q, 4)['target']
    res = new_validator(q).expand_and_validate()
    assert res.success
    assert condition(res.expanded_query, 0)['target'] == full_count_target
    assert condition(res.expanded_query, 3)['target'] == full_sum_target


def test_valid_condition_names():
    simple_filter = ['eventId', '==', 1]
    valid_names = ['a', 'zrubi_1_a_234', 'PORQUE', 'zz12123123']
    valid_conditions = [{'name': n, 'filter': simple_filter} for n in valid_names]
    q = {'query': {'conditions': valid_conditions}}
    res = new_validator(q).expand_and_validate()
    assert res.success

    invalid_names = ['1', '2asd', '$1', '$adsas', 'import crazy', ' a', 'b '] + RELATION_OPS
    for n in invalid_names:
        q = {'query': {'conditions': [{'name': n, 'filter': simple_filter}]}}
        print(f"Testing query: {q}")
        res = new_validator(q).expand_and_validate()
        assert res.success is False and res.error_kind == ValidationErrorKind.SCHEMA


def test_include_zero_validity():
    simple_filter = ['eventId', '==', 1]
    valid_cases = [
        {'target': ['count', '==', 0], 'includeZero': True},
        {'target': ['count', '!=', 1], 'includeZero': True},
        {'target': ['count', '<=', 10], 'includeZero': True},
        {'target': ['count', '<=', 0], 'includeZero': True},
        {'target': ['count', '<', 1], 'includeZero': True},
        {'target': ['count', '>=', 0], 'includeZero': True},
        {'target': ['count', '==', 1], 'includeZero': False},
        {'target': ['count', '!=', 0], 'includeZero': False},
        {'target': ['count', '>', 0], 'includeZero': False},
        {'target': ['count', '>=', 1], 'includeZero': False},
    ]
    invalid_cases = [
        {'target': ['count', '==', 0], 'includeZero': False},
        {'target': ['count', '==', 1], 'includeZero': True},
        {'target': ['count', '!=', 0], 'includeZero': True},
        {'target': ['count', '<', 0], 'includeZero': True},
        {'target': ['count', '>', 0], 'includeZero': True},
        {'target': ['count', '>=', 1], 'includeZero': True},
    ]

    def validate(cases: list, expected_success: bool):
        for case in cases:
            q = {'query': {'conditions': [{**case, 'filter': simple_filter}]}}
            print(f"Testing query: {q}")
            res = new_validator(q).expand_and_validate()
            assert res.success == expected_success
            if not expected_success:
                assert res.error_kind == ValidationErrorKind.TYPE_MISMATCH

    validate(valid_cases, expected_success=True)
    validate(invalid_cases, expected_success=False)

    # Check the one case where 'includeZero' is explicitly expanded if not set, to prevent bugs
    q = {'query': {'conditions': [{'target': ['count', '==', 0],
                                   'filter': simple_filter}]}}
    res = new_validator(q).expand_and_validate()
    assert condition(res.expanded_query, 0)['includeZero']


def test_filters_match_dataset():
    invalid_filters = [
        {'filter': ['eventBlarg', '>', 1]},
        {'filter': ['eventId', 'contains', 'a']},
        {'filter': ['eventId', '>', 'a']},
        {'filter': ['eventId', '==', True]},
        {'filter': ['eventId', '==', False]},
        {'filter': ['rowType', '>', 'a']},
        {'filter': ['rowType', '==', 1]},
        {'filter': ['rowType', '==', True]},
        {'filter': ['isCool', '==', 1]},
        {'filter': ['isCool', '==', 'true']},
        {'filter': ['isCool', 'contains', 'fish']},
        {'filters': [{'column': 'eventId', 'op': '>', 'value': 0},
                     {'column': 'eventBlah', 'op': '>', 'value': 0}]}
    ]

    for case in invalid_filters:
        q = {'query': {'conditions': [case]}}
        print(f"Testing query: {q}")
        res = new_validator(q).expand_and_validate()
        assert not res.success and res.error_kind in [ValidationErrorKind.DATASET_MISMATCH,
                                                      ValidationErrorKind.TYPE_MISMATCH]


def test_targets_match_type_and_dataset():
    simple_filter = ['eventId', '==', 1]
    invalid_targets = [
        ['count', 'contains', 100],
        ['count', '>', True],
        ['count', '>', False],
        ['count', '>', "hello"],
        ['sum', 'jarjar', '>', 100],
        ['sum', 'rowType', '>', 100],
        ['sum', 'eventValue', '>', 'blah'],
        ['sum', 'eventValue', '>', True],
        ['sum', 'eventValue', 'contains', 100],
    ]

    for case in invalid_targets:
        q = {'query': {'conditions': [
            {'filter': simple_filter, 'target': case}
        ]}}
        print(f"Testing query: {q}")
        res = new_validator(q).expand_and_validate()
        assert not res.success and res.error_kind != ValidationErrorKind.UNEXPECTED


def test_name_uniqueness():
    simple_filter = {'column': 'eventId', 'op': '==', 'value': 1}
    q = {
        'query': {
            'conditions': [
                {'name': 'name0', 'filter': simple_filter},
                {'name': 'name1', 'filter': simple_filter},
                {'name': 'name2', 'filter': simple_filter}
            ],
            'aggregations': [
                {"name": "name0", "type": "count", "column": "transactionId"},
                {"name": "name1", "type": "count", "column": "transactionId"},
            ]

        },
        'funnel': {
            'sequence': [{'filter': simple_filter}, {'filter': simple_filter}],
            'stepAggregations': [
                {"name": "name0", "type": "count", "column": "transactionId"},
                {"name": "name1", "type": "count", "column": "transactionId"},
            ],
            'endAggregations': [
                {"name": "name0", "type": "count", "column": "transactionId"},
                {"name": "name1", "type": "count", "column": "transactionId"},
            ]

        }
    }
    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success
    assert res.named_conditions == {'name0': 0, 'name1': 1, 'name2': 2}

    invalid_q = dict_clone(q)
    condition(invalid_q, 2)['name'] = 'name0'
    v = new_validator(invalid_q)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.SCHEMA

    invalid_q = dict_clone(q)
    invalid_q['query']['aggregations'][1]['name'] = 'name0'
    v = new_validator(invalid_q)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.SCHEMA

    invalid_q = dict_clone(q)
    invalid_q['funnel']['stepAggregations'][1]['name'] = 'name0'
    v = new_validator(invalid_q)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.SCHEMA

    invalid_q = dict_clone(q)
    invalid_q['funnel']['endAggregations'][1]['name'] = 'name0'
    v = new_validator(invalid_q)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.SCHEMA


def test_shorthand_relation():
    simple_filter = {'column': 'eventId', 'op': '==', 'value': 1}
    q = {
        'query': {
            'conditions': [
                {'name': 'name0', 'filter': simple_filter},
                {'name': 'name1', 'filter': simple_filter},
                {'name': 'name2', 'filter': simple_filter}
            ]
        }
    }

    # Expanded missing relation
    v = new_validator(q)
    res = v.expand_and_validate()
    print(res.expanded_query)
    assert res.success and res.expanded_query['query']['relation'] == '$0 and $1 and $2'
    transformed_relation = \
        relation_to_pandas_query(elements=res.relation_elements, column_prefix=PANDAS_CONDITION_COL_PREFIX)
    assert transformed_relation == '__c0 & __c1 & __c2'
    assert res.used_conditions == list(range(3))

    # noinspection PyTypeChecker
    q['query']['relation'] = 'OR'
    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success and res.expanded_query['query']['relation'] == '$0 or $1 or $2'
    transformed_relation = \
        relation_to_pandas_query(elements=res.relation_elements, column_prefix=PANDAS_CONDITION_COL_PREFIX)
    assert transformed_relation == '__c0 | __c1 | __c2'
    assert res.used_conditions == list(range(3))

    for relation in ['', '   ', 'mosh']:
        # noinspection PyTypeChecker
        q['query']['relation'] = relation
        v = new_validator(q)
        res = v.expand_and_validate()
        assert res.success is False and res.error_kind == ValidationErrorKind.RELATION


def test_complex_relation():
    simple_filter = {'column': 'eventId', 'op': '==', 'value': 1}
    q = {
        'query': {
            'conditions': [
                {'name': 'name0', 'filter': simple_filter},
                {'name': 'name1', 'filter': simple_filter},
                {'name': 'name2', 'filter': simple_filter}
            ]
        }
    }

    valid_complex_relation = [
        ('$2', '__c2'),

        ('$name2   ', '__c2'),

        ('$0 or $1 and $2 and $name0 or $name2',
         '__c0 | __c1 & __c2 & __c0 | __c2'),

        ('($0 or $1) and  (( $2 and  $name0 ) or (($name2))) ',
         '(__c0 | __c1) & ((__c2 & __c0) | ((__c2)))'),

        (' ( $0  or $1) &&  (( $2 and  $name0)  || ( ($name2 )) ) ',
         '(__c0 | __c1) & ((__c2 & __c0) | ((__c2)))')
    ]

    for source, expected_transformed in valid_complex_relation:
        # noinspection PyTypeChecker
        q['query']['relation'] = source
        print(f"Source relation is '{source}', expected parsed relation: {expected_transformed}")
        v = new_validator(q)
        res = v.expand_and_validate()
        assert res.success
        transformed_source = relation_to_pandas_query(res.relation_elements, column_prefix='__c')
        if transformed_source != expected_transformed:
            raise Exception(f"Expected '{expected_transformed}' but found: '{transformed_source}'")

    invalid_relations = [
        '$1a', '$', '$import() ', '$a-3', '$a a',
        '$3', '$name3', '$97686778343434', '$adlkja(&^%&^$%@#%&^$&*sydfs fs08d6 78s5876596^$%hdjkhasjkdhajkdhjahdjahsj',
        '$1 and', '$1 $2', '$1 and ()', '(($1)))', '()', '$name$1', '$name ($1)', ')', '$1 and(exception)',
        '$1 / $2', '*', '$1 and \'less /etc/gimme', 'sys.exit(1)', '$sys.exit(1)', ';sys.exit(1)', '(sys.exit(1))',
        'raise Exception($1)', '"'
    ]

    for relation in invalid_relations:
        # noinspection PyTypeChecker
        q['query']['relation'] = relation
        print(f"Testing {relation}")
        v = new_validator(q)
        res = v.expand_and_validate()
        assert res.success is False and res.error_kind == ValidationErrorKind.RELATION


def test_aggregation_expansion():
    simple_filter = {'column': 'eventId', 'op': '==', 'value': 1}
    q = {
        'query': {
            'aggregations': [
                {"type": "count", "column": "transactionId"},
                {"type": "sumPerValue", "column": "transactionId", "otherColumn": "eventValue"},
                {"column": "eventId"}
            ]

        },
        'funnel': {
            'sequence': [{'filter': simple_filter}],
            'stepAggregations': [{"column": "eventId"}],
            'endAggregations': [{"column": "eventId"}]

        }
    }

    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success
    original_aggrs = q['query']['aggregations']
    expanded_aggrs = res.expanded_query['query']['aggregations']
    # Aggrs with a type are not changed
    assert expanded_aggrs[0] == original_aggrs[0] and \
           expanded_aggrs[1] == original_aggrs[1]

    # The last aggregation without a type is expanded: types match all DEFAULT_AGGREGATIONS, column name stays
    range_of_expansion = range(len(original_aggrs) - 1, len(expanded_aggrs))
    expanded_types = [expanded_aggrs[i]['type'] for i in range_of_expansion]
    assert sorted(expanded_types) == sorted(DEFAULT_AGGREGATIONS)
    expanded_colnames = [expanded_aggrs[i]['column'] for i in range_of_expansion]
    assert expanded_colnames == [original_aggrs[-1]['column']] * len(DEFAULT_AGGREGATIONS)

    # Expansion happens also in funnel
    assert len(q['funnel']['stepAggregations']) == 1
    assert len(res.expanded_query['funnel']['stepAggregations']) == len(DEFAULT_AGGREGATIONS)
    assert len(q['funnel']['endAggregations']) == 1
    assert len(res.expanded_query['funnel']['endAggregations']) == len(DEFAULT_AGGREGATIONS)


def test_aggregation_other_column():
    simple_filter = {'column': 'eventId', 'op': '==', 'value': 1}
    q = {
        'funnel': {
            'sequence': [{'filter': simple_filter}],
            'endAggregations': [{"column": "eventId", "type": "sumPerValue", "otherColumn": "eventValue"}]

        }
    }

    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success

    qclone = dict_clone(q)
    qclone['funnel']['endAggregations'][0]['otherColumn'] = "what"
    v = new_validator(qclone)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.DATASET_MISMATCH

    qclone['funnel']['endAggregations'][0]['otherColumn'] = "rowType"
    v = new_validator(qclone)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.TYPE_MISMATCH

    qclone['funnel']['endAggregations'][0]['type'] = "countPerValue"
    v = new_validator(qclone)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.SCHEMA

    qclone['funnel']['endAggregations'][0]['type'] = "sumPerValue"
    del qclone['funnel']['endAggregations'][0]['otherColumn']
    v = new_validator(qclone)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.SCHEMA


def test_default_aggr_name():
    simple_filter = {'column': 'eventId', 'op': '==', 'value': 1}
    q = {
        'funnel': {
            'sequence': [{'filter': simple_filter}],
            'stepAggregations': [{"column": "eventId", "type": "count", "name": "aggr1"}, {"column": "eventId"}]

        }
    }

    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success

    # noinspection PyTypeChecker
    q['funnel']['stepAggregations'][1]['name'] = "mename"
    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.SCHEMA


def test_timeframe_scale():
    q = query_clone()
    q['timeframe'] = {}
    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success

    q['timeframe'] = {'from': 123}
    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.TYPE_MISMATCH

    q['timeframe'] = {'to': 123456789012345}
    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.TYPE_MISMATCH

    q['timeframe'] = {'from': 1590918400516, 'to': 1600918400516}
    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success

    q['timeframe'] = {'to': 1590918400516, 'from': 1600918400516}
    v = new_validator(q)
    res = v.expand_and_validate()
    assert res.success is False and res.error_kind == ValidationErrorKind.SCHEMA

# TODO Backlogged tests?
# 1. Sequences: must be filters array, no target, includeZero. Check both for seq conditions and funnel
# 2. Float vs. int columns
# 3. Target 'sum' can accept negative values
