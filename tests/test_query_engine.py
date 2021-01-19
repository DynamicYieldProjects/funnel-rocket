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
            'price': [BEGIN_PRICE + index + i for i in range(number_of_rows_for_user)]
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


def test_contains():
    data = test_create_data()
    query = {
        'relation': 'or',
        'conditions': [
            {
                "filter": {
                        "column": "category",
                        "op": "contains",
                        "value": "hing"
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
    # this query gets users with strictly more than 2 fishing events, so only users c and d
    assert engine_result.query.matching_groups == 1 + 1
    assert engine_result.query.matching_group_rows == (2 ** 4) + (2 ** 5)
