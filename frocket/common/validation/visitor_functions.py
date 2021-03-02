"""
A collection of callback functions which the QueryValidator uses to extarct, validate and transform query elements,
with the kind help of PathVisitor class.

Functions which return a value are used to replace the given object with a different one,
which is handled by PathVisitor in its 'modifiable' mode.

Since callbacks are regular functions (not methods), and there's a bunch of them, they're in a separate file from
the QueryValidator class.

asserts are used where processing elements which should be already validated (so failures should be bugs).
"""
from typing import Optional
from frocket.common.validation.consts import DEFAULT_TARGET, AGGR_TYPES_WITH_OTHER_COLUMN, \
    DEFAULT_AGGREGATIONS, TARGET_TYPES_WITH_INCLUDE_ZERO, TARGET_OPS_SUPPORTING_INCLUDE_ZERO
from frocket.common.validation.error import ValidationErrorKind, QueryValidationError


def _to_verbose_filter(fltr) -> Optional[dict]:
    """If a condition filter is in short-hand notation (list), convert to verbose notation."""
    assert type(fltr) in [list, dict]
    if type(fltr) is list:
        assert len(fltr) == 3
        return {'column': fltr[0], 'op': fltr[1], 'value': fltr[2]}


def _to_verbose_target(target) -> Optional[dict]:
    """If a condition target is in short-hand notation (list), convert to verbose notation."""
    assert type(target) in [list, dict]
    if type(target) is list:
        assert len(target) in [3, 4]
        if len(target) == 3:
            return {'type': target[0], 'op': target[1], 'value': target[2]}
        elif len(target) == 4:
            return {'type': target[0], 'column': target[1], 'op': target[2], 'value': target[3]}


def _add_default_target(cond: dict) -> None:
    assert type(cond) is dict
    # (Modification is done on a key under the given object, so no need to return a modified dict)
    if ('filter' in cond or 'filters' in cond) and 'target' not in cond:  # Don't touch sequence conditions
        cond['target'] = DEFAULT_TARGET


def _validate_aggregation(aggr: dict) -> None:
    assert type(aggr) is dict
    aggr_type = aggr.get('type', None)
    other_column_required = aggr_type in AGGR_TYPES_WITH_OTHER_COLUMN
    other_column_found = 'otherColumn' in aggr

    if other_column_required != other_column_found:
        message = f"For aggregation {aggr} with type '{aggr_type}', other column name is "
        if other_column_required:
            message += 'required but was not found'
        else:
            message += 'not relevant but was given'
        raise QueryValidationError(message, kind=ValidationErrorKind.SCHEMA)


def _expand_aggregations(col_aggregations: list) -> Optional[list]:
    assert type(col_aggregations) is list
    result = []
    for aggr in col_aggregations:
        if aggr.get('type', None):
            result.append(aggr)
        else:
            if 'name' in aggr:
                message = f"Aggregation {aggr} expands into multiple default aggregations, " \
                          f"and thus a name attributeis not supported"
                raise QueryValidationError(message, kind=ValidationErrorKind.SCHEMA)
            for added_type in DEFAULT_AGGREGATIONS:
                result.append({**aggr, 'type': added_type})

    return result


def _validate_or_set_include_zero(cond: dict) -> None:
    """
    'includeZero' attribute of conditions may be tricky to get right.
    This function validates that its usage makes sense, and sets the correct default where it's ommitted.
    """
    assert type(cond) is dict
    if not ('filter' in cond or 'filters' in cond):
        return  # Skip sequence condition (and possibly other future types without a target)

    # This should run after _to_verbose_target() and _add_default_target() have already ran, ensuring target exists
    target_type = cond['target']['type']
    target_op = cond['target']['op']
    target_value = cond['target']['value']
    include_zero_value = cond.get('includeZero', None)
    target_as_string = f"{target_type} {target_op} {target_value}"

    if target_type not in TARGET_TYPES_WITH_INCLUDE_ZERO:
        if include_zero_value:  # Exists and set to True
            raise QueryValidationError(
                message=f"'includeZero' is not applicable for target type '{target_type}'. In condition: {cond}",
                kind=ValidationErrorKind.TYPE_MISMATCH)
    else:
        assert type(target_value) is int
        assert target_value >= 0

        if include_zero_value:  # Exists and set to True
            # Operator never relevant for includeZero=True
            if target_op not in TARGET_OPS_SUPPORTING_INCLUDE_ZERO:
                raise QueryValidationError(
                    message=f"For target operator '{target_op}', 'includeZero' cannot be true. In condition: {cond}",
                    kind=ValidationErrorKind.TYPE_MISMATCH)

            # Additional check when an operator is *potentially* relevant for includeZero=True
            if target_op == '<' and target_value == 0:
                raise QueryValidationError(
                    message=f"Target implies a negative value. In condition: {cond}",
                    kind=ValidationErrorKind.TYPE_MISMATCH)

            if (target_op == '!=' and target_value == 0) or \
                    (target_op in ['==', '>='] and target_value != 0):
                message = f"Target {target_as_string} explicitly precludes zero, and thus 'includeZero' " \
                          f"cannot be true. In condition: {cond}"
                raise QueryValidationError(message, kind=ValidationErrorKind.TYPE_MISMATCH)
        else:
            if target_op == '==' and target_value == 0:
                if include_zero_value is None:
                    # Explicitly set includeZero when target is count == 0
                    # Note: modifying a key under the given object, so no need to return a modified dict
                    cond['includeZero'] = True
                elif not include_zero_value:
                    message = f"When using a target of {target_as_string}, 'includeZero' cannot be false. " \
                                    f"Condition: {cond}"
                    raise QueryValidationError(message, kind=ValidationErrorKind.TYPE_MISMATCH)
