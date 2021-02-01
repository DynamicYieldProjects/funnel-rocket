import os
import tempfile
from typing import List
from frocket.common.metrics import MetricName, MetricData, MetricLabelEnum

IN_GITHUB_WORKFLOW = "GITHUB_WORKFLOW" in os.environ
SKIP_SLOW_TESTS = os.environ.get('SKIP_LOCAL_S3_TESTS', False) or IN_GITHUB_WORKFLOW
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


def assert_metric_value(metrics: List[MetricData], name: MetricName, value: float):
    assert metrics
    metric = next(filter(lambda metric: metric.name == name, metrics), None)
    assert metric is not None
    assert metric.value == value


def assert_label_value_exists(metrics: List[MetricData], label: MetricLabelEnum):
    assert metrics
    found_metric = next(filter(lambda metric: label.label_name in metric.labels, metrics), None)
    assert found_metric.labels[label.label_name] == label.label_value
