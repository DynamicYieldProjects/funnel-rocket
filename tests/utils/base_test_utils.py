import os
import tempfile
from typing import List, Type
from frocket.common.metrics import MetricName, MetricData, MetricLabelEnum

SKIP_SLOW_TESTS = os.environ.get('SKIP_SLOW_TESTS', "False").lower() == 'true'
SKIP_LAMBDA_TESTS = os.environ.get('SKIP_LAMBDA_TESTS', "False").lower() == 'true'
# noinspection PyProtectedMember,PyUnresolvedReferences
TEMP_DIR = tempfile._get_default_tempdir()


# noinspection PyProtectedMember,PyUnresolvedReferences
def temp_filename(suffix='', with_dir: bool = True):
    fname = next(tempfile._get_candidate_names()) + suffix
    return f"{TEMP_DIR}/{fname}" if with_dir else fname


# A mixin to allow defining utility classes named "Test<X>" without pytest trying to collect test cases in them,
# which results in warnings (and without needing a pytest.ini entry). See https://stackoverflow.com/a/46199666
class DisablePyTestCollectionMixin(object):
    __test__ = False


def get_metric_value(metrics: List[MetricData], name: MetricName) -> float:
    assert metrics
    metric = next(filter(lambda metric: metric.name == name, metrics), None)
    assert metric is not None
    return metric.value


def assert_metric_value(metrics: List[MetricData], name: MetricName, value: float):
    assert get_metric_value(metrics, name) == value


def find_first_label_value(metrics: List[MetricData], label_type: Type[MetricLabelEnum]) -> str:
    assert metrics
    found_metric = next(filter(lambda metric: label_type.label_name in metric.labels, metrics), None)
    return found_metric.labels[label_type.label_name]


def assert_label_value_exists(metrics: List[MetricData], label: MetricLabelEnum):
    assert find_first_label_value(metrics, label.__class__) == label.label_value
