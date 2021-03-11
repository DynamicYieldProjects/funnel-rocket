from pandas import DataFrame
from frocket.common.tasks.query import QueryResult
from frocket.common.validation.query_validator import QueryValidator
from frocket.engine.query_engine import QueryEngine
import pandas as pd

CATEGORIES = ['fishing', 'running', 'climbing', 'snorkeling']
TYPES = ['view', 'click', 'purchase', 'a2c']
BEGIN_TS = 1610555782
BEGIN_PRICE = 100
GROUP_COLUMN = 'id'
TS_COLUMN = 'timestamp'
ALL_USERS = ['a', 'b', 'c', 'd']
KEYWORDS = ['cat|dog', 'mouse|cat|dog', 'cat', 'dog|mouse|cat']

'''
test cases
==========

target types: V
- sum
- count

numeric operators: V
- <
- >
- ==
- !=
- <=
- >=

string operators: V
- ==
- !=
- contains
- not contains

boolean operators: V
- ==
- !=

aggregations:
- "count"
- "countPerValue"
- "groupsPerValue"
- "sumPerValue"
- "meanPerValue"

'''


def test_sum():
    data = []
    for i in range(1, 1001):
        data.append({
            'f': i,
            'id': 'a',
            'timestamp': i
        })
    data.append({
        'f': 10,
        'id': 'b',
        'timestamp': 2
    })

    data = pd.DataFrame(data)
    query = {
        'conditions': [
            {
                "filter": {
                    "column": "f",
                    "op": ">",
                    "value": 0
                },
                "target": {
                    "type": "sum",
                    "op": "==",
                    "column": "f",
                    "value": int((1000 * 1001) / 2)
                }
            }
        ]
    }
    engine_result = expand_and_run_query(df=data, query_part=query)
    assert engine_result.query.matching_group_rows == 1000


def test_boolean_operators():
    data = [
        {
            'bool_field': True,
            'id': 'a',
            'timestamp': 1
        },
        {
            'bool_field': False,
            'id': 'b',
            'timestamp': 2
        },
        {
            'id': 'b',
            'timestamp': 3
        }
    ]
    data = pd.DataFrame(data)
    for (op, value, expectation, target_op) in [('==', True, 1, '=='),
                                                # user a has exactly one "true" event, and exactly one row
                                                ('!=', True, 2, '>'),
                                                # user b has two events with value != "True" (one false and one NA)
                                                ('==', False, 2, '=='),
                                                ('!=', False, 3, '==')  # both users have one event that is not False
                                                ]:
        query = {
            'conditions': [
                {
                    "filter": {
                        "column": "bool_field",
                        "op": op,
                        "value": value
                    },
                    "target": {
                        "type": "count",
                        "op": target_op,
                        "value": 1
                    }
                }
            ]
        }
        engine_result = expand_and_run_query(df=data, query_part=query)
        assert engine_result.query.matching_group_rows == expectation


# all queries filter timestamp to be <= BEGIN_TS + 5, which yields 10 rows
# 4 rows for user "a", 3 for "b", 2 for "c" and 1 for "d"
def test_numeric_operators():
    data = test_create_data()
    for (op, val, expectation) in [('==', 2, 1),  # 1 user with 2 events
                                   ('<', 3, 2),  # 2 users, with 1 and 2 events
                                   ('>', 2, 2),  # etc.
                                   ('<=', 1, 1),
                                   ('>=', 4, 1),
                                   ('!=', 1, 3)]:
        query = {
            'relation': 'and',
            'conditions': [
                {
                    "filter": {
                        "column": "timestamp",
                        "op": "<=",
                        "value": BEGIN_TS + 5
                    },
                    "target": {
                        "type": "count",
                        "op": op,
                        "value": val
                    }
                }
            ]
        }

        engine_result = expand_and_run_query(df=data, query_part=query)
        assert engine_result.query.matching_groups == expectation


def expand_and_run_query(df: DataFrame, query_part: dict = None, funnel_part: dict = None) -> QueryResult:
    full_query = {}
    if query_part:
        full_query['query'] = query_part
    if funnel_part:
        full_query['funnel'] = funnel_part
    validator = QueryValidator(full_query)
    print(f"Query before expansion: {full_query}")
    validation_result = validator.expand_and_validate(schema_only=True)
    expanded_query = validation_result.expanded_query
    print(f"Query after expansion: {expanded_query}")
    print(validation_result.error_message)
    assert validation_result.success
    engine = QueryEngine(group_by_column=GROUP_COLUMN, timestamp_column=TS_COLUMN)
    return engine.run(df, expanded_query)


def test_create_data():
    # 4 users
    dfs = []
    for index, user in enumerate(ALL_USERS):
        index += 2
        number_of_rows_for_user = 2 ** index
        data = {
            'id': [user] * number_of_rows_for_user,
            'timestamp': [BEGIN_TS + index + i for i in range(number_of_rows_for_user)],
            'category': CATEGORIES * int(number_of_rows_for_user / 4),
            'type': TYPES * int(number_of_rows_for_user / 4),
            'price': [BEGIN_PRICE + index + i for i in range(number_of_rows_for_user)],
            'keywords': [f'{w}_{user}' for w in KEYWORDS] * int(number_of_rows_for_user / 4)
        }
        dfs.append(pd.DataFrame.from_dict(data))
    return pd.concat(dfs, axis=0)


def test_empty_query_brings_all_users():
    data = test_create_data()
    query = {
        'conditions': []
    }
    engine_result = expand_and_run_query(df=data, query_part=query)
    assert engine_result.query.matching_groups == len(ALL_USERS)
    assert engine_result.query.matching_group_rows == len(data)


def test_simple_filter():
    data = test_create_data()
    query = {
        'relation': 'and',
        'conditions': [
            {
                "filter": {
                    "column": "price",
                    "op": ">",
                    "value": 133
                },
                "target": {
                    "type": "count",
                    "op": ">",
                    "value": 2
                }
            }
        ]
    }
    engine_result = expand_and_run_query(df=data, query_part=query)
    # this matches only user 'd', that has 2^5 rows
    assert engine_result.query.matching_groups == 1
    assert engine_result.query.matching_group_rows == 2 ** 5


def test_logical_or():
    data = test_create_data()
    query = {
        'relation': 'or',
        'conditions': [
            {
                "filter": {
                    "column": "price",
                    "op": "<=",
                    "value": 104
                },
                "target": {
                    "type": "count",
                    "op": ">",
                    "value": 2
                }
            },
            {
                "filter": {
                    "column": "price",
                    "op": ">",
                    "value": 133
                },
                "target": {
                    "type": "count",
                    "op": ">",
                    "value": 2
                }
            }
        ]
    }
    engine_result = expand_and_run_query(df=data, query_part=query)
    # first condition matches user 'a', that has 2^2 rows,
    # logical or with condition from 'simple' test adds user 'd'
    assert engine_result.query.matching_groups == 1 + 1
    assert engine_result.query.matching_group_rows == (2 ** 2) + (2 ** 5)


def test_column_aggregations():
    data = test_create_data()
    query = {
        'conditions': [],
        'aggregations': [
            {
                'column': 'category'
            }
        ]
    }
    engine_result = expand_and_run_query(df=data, query_part=query)
    aggs = engine_result.query.aggregations
    assert len(aggs) == 3
    aggs_d = {agg.type: agg for agg in aggs}
    assert aggs_d['count'].value == len(data)
    assert aggs_d['countPerValue'].value == {cat: 15 for cat in CATEGORIES}
    assert aggs_d['groupsPerValue'].value == {cat: 4 for cat in CATEGORIES}


def test_other_column_aggregations():
    data = []
    for i in range(10):
        d = {
            'id': 'a',
            'timestamp': 1,
            'category': 'a' if i % 2 == 0 else 'b',
            'price': i
        }
        data.append(d)
    data = pd.DataFrame(data)
    query = {
        'conditions': [],
        'aggregations': [
            {
                'column': 'category',
                'type': 'sumPerValue',
                'otherColumn': 'price'
            }
        ]
    }
    engine_result = expand_and_run_query(df=data, query_part=query)
    aggs = engine_result.query.aggregations
    '''
cat  price
a      0
b      1
a      2
b      3
a      4
b      5
a      6
b      7
a      8
b      9
    '''
    assert aggs[0].value['a'] == 0 + 2 + 4 + 6 + 8
    assert aggs[0].value['b'] == 1 + 3 + 5 + 7 + 9
    query = {
        'conditions': [],
        'aggregations': [
            {
                'column': 'category',
                'type': 'meanPerValue',
                'otherColumn': 'price'
            }
        ]
    }
    engine_result = expand_and_run_query(df=data, query_part=query)
    aggs = engine_result.query.aggregations
    assert aggs[0].value['a'] == (0 + 2 + 4 + 6 + 8) / 5
    assert aggs[0].value['b'] == (1 + 3 + 5 + 7 + 9) / 5


def test_multi_filter_simple():
    data = test_create_data()
    query = {
        'relation': 'or',
        'conditions': [
            {
                "filters": [
                    {
                        "column": "price",
                        "op": "<=",
                        "value": 104
                    },
                    {
                        "column": "category",
                        "op": "==",
                        "value": "running"
                    }
                ],
                "target": {
                    "type": "count",
                    "op": "==",
                    "value": 1
                }
            }
        ]
    }
    engine_result = expand_and_run_query(df=data, query_part=query)
    # this query matches users a and b, which have 2^2 + 2^3 rows together
    assert engine_result.query.matching_groups == 1 + 1
    assert engine_result.query.matching_group_rows == (2 ** 2) + (2 ** 3)


def test_string_operators():
    data = test_create_data()
    print(len(data))
    # all conditions are with > 3 count target
    for (op, value, expectation) in [('contains', 'dog_a', 0),  # a has 3 events containing dog_a
                                     ('contains', 'dog_b', 8),  # b has 8 etc.
                                     ('contains', 'dog_c', 16),
                                     ('==', 'cat_a', 0),  # same as above, only 1 cat_a event
                                     ('==', 'cat_c', 16),
                                     ('!=', 'cat_a', 56),  # all but user a have more than 3 !cat_a events
                                     ('not contains', 'mouse', 56),  # user "a" has only 2 events that doesn't
                                     # contain "mouse", and he has overall 4 events. All other users have more than 3
                                     # events that doesn't contain "mouse"
                                     ]:
        query = {
            'relation': 'or',
            'conditions': [
                {
                    "filter": {
                        "column": "keywords",
                        "op": op,
                        "value": value
                    },
                    "target": {
                        "type": "count",
                        "op": ">",
                        "value": 3
                    }
                }
            ]
        }
        engine_result = expand_and_run_query(df=data, query_part=query)
        assert engine_result.query.matching_group_rows == expectation

def test_sequence_condition():
    data = []
    for i in range(5):
        d = [
            # user 'a' will have this sequence of categories: 0 1 2 3 4
            {
            'id': 'a',
            'timestamp': i,
            'category': i,
            'price': 1
        },
            # user 'b' will have this sequence of categories: 0 1 -1 -1 -1
        {
            'id': 'b',
            'timestamp': i,
            'category': i if i < 2 else -1,
            'price': 2
        },
            # user 'c' will have this sequence of categories: 4 3 2 1 0
        {
            'id': 'c',
            'timestamp': i,
            'category': 4 - i,
            'price': 3
        }
        ]
        data.extend(d)
    data = pd.DataFrame(data)
    query = {
        'conditions': [
            {
                "sequence": [
                    {
                        "filter": ["category", "==", 0]
                    },
                    {
                        "filter": ["category", "<=", 2]
                    },
                    {
                        "filter": ["category", "<", 0]
                    }
                ]
            }
        ],
        'aggregations': [{'column': 'price'}]
    }
    engine_result = expand_and_run_query(df=data, query_part=query)
    # only user b fits the query
    assert engine_result.query.matching_groups == 1
    assert '2' in engine_result.query.aggregations[-1].value
    query = {
        'conditions': [
            {
                "sequence": [
                    {
                        "filter": ["category", ">=", 3]
                    },
                    {
                        "filter": ["category", "<=", 2]
                    },
                    {
                        "filter": ["category", "==", 0]
                    }
                ]
            }
        ],
        'aggregations': [{ 'column': 'price'}]
    }
    engine_result = expand_and_run_query(df=data, query_part=query)
    # this time it's user c
    assert engine_result.query.matching_groups == 1
    assert '3' in engine_result.query.aggregations[-1].value

