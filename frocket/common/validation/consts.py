"""
Consts and types for the query validation package
TODO backlog create a nice enum for all query keywords
"""
#  Copyright 2021 The Funnel Rocket Maintainers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import json
import os
import re
from pathlib import Path
from typing import Dict, NamedTuple
from frocket.common.dataset import DatasetColumnType
from frocket.common.validation.path_visitor import PathVisitor

# JSON Schema file
QUERY_SCHEMA_LOCATION = Path(os.path.dirname(__file__)) / '../../resources/query_schema.json'
QUERY_SCHEMA = json.load(open(QUERY_SCHEMA_LOCATION, 'r'))

TARGET_TYPES_WITH_INCLUDE_ZERO = ['count']
TARGET_OPS_SUPPORTING_INCLUDE_ZERO = ['<', '<=', '==', '!=', '>=']
TARGET_TYPES_WITH_OTHER_COLUMN = ['sum']
AGGR_TYPES_WITH_OTHER_COLUMN = ['sumPerValue', 'meanPerValue']
DEFAULT_TARGET = {'type': 'count', 'op': '>=', 'value': 1}
DEFAULT_AGGREGATIONS = ['count', 'countPerValue', 'groupsPerValue']
AGGREGATIONS_PATHS = ['query.aggregations',
                      'funnel.stepAggregations',
                      'funnel.endAggregations']
SINGLE_FILTER_PATHS = ['query.conditions.filter',
                       'query.conditions.sequence.filter',
                       'funnel.sequence.filter']
FILTER_ARRAY_PATHS = ['query.conditions.filters',
                      'query.conditions.sequence.filters',
                      'funnel.sequence.filters']

VALID_IDENTIFIER_PATTERN = re.compile(r'[A-Z][A-Z_0-9]*$', re.IGNORECASE)
UNIQUE_IDENTIFIER_SCOPES = ['query.conditions.name'] + \
                           [f"{path}.name" for path in AGGREGATIONS_PATHS]

EQUALITY_OPERATORS = ['==', '!=']
NUMERIC_OPERATORS = [*EQUALITY_OPERATORS, '>', '>=', '<', '<=']
STRING_OPERATORS = [*EQUALITY_OPERATORS, 'contains', 'regex']
OPERATORS_BY_COLTYPE = {
    DatasetColumnType.INT: NUMERIC_OPERATORS,
    DatasetColumnType.FLOAT: NUMERIC_OPERATORS,
    DatasetColumnType.BOOL: EQUALITY_OPERATORS,
    DatasetColumnType.STRING: STRING_OPERATORS
}
VALUE_TYPES_BY_COLTYPE = {
    DatasetColumnType.INT: [int],
    DatasetColumnType.FLOAT: [int, float],
    DatasetColumnType.BOOL: [bool],
    DatasetColumnType.STRING: [str]
}
NUMERIC_COLTYPES = [DatasetColumnType.INT, DatasetColumnType.FLOAT]

RELATION_OPS = ['and', 'or', '||', '&&']
DEFAULT_RELATION_OP = 'and'
CONDITION_COLUMN_PREFIX = "__cond_"


class QueryConditionsMap(NamedTuple):
    count: int
    names: Dict[str, int]


def map_condition_names(query: dict) -> QueryConditionsMap:
    """Map named conditions (which is optional) to the condition ID (index in conditions list)."""
    conditions = PathVisitor(query, 'query.conditions').list()
    names = {cond['name'].strip().lower(): i
             for i, cond in enumerate(conditions) if 'name' in cond}
    return QueryConditionsMap(count=len(conditions), names=names)
