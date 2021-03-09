"""
While metrics support in Funnel Rocket is built with Prometheus (or more generally OpenMetrics) in mind,
all Prometheus-specific code is in this module.

TODO backlog support help string (documentation) per each member in MetricName enum
"""
from typing import List, Dict, Type
from prometheus_client import Counter, Histogram
from prometheus_client.metrics import MetricWrapperBase
from frocket.common.config import config
from frocket.common.helpers.utils import memoize
from frocket.common.metrics import MetricName, MeasuredUnit, supported_label_names, MetricData, empty_label_names

prom_counters: Dict[MetricName, Counter] = {}
prom_histograms: Dict[MetricName, Histogram] = {}


@memoize
def buckets_by_unit(unit: MeasuredUnit) -> List[float]:
    """Each unit (seconds, bytes, dollars) may have its own buckets configured, or fallback to the default."""
    assert unit is not MeasuredUnit.COUNT  # COUNT should not use a histogram
    buckets_string = config.get_with_fallbacks(f'metrics.buckets.{unit.name.lower()}', 'metrics.buckets.default')
    buckets = [float(b) for b in buckets_string.split(',')]
    return buckets


def unit_to_metric_type(unit: MeasuredUnit) -> Type[MetricWrapperBase]:
    """The type of Prometheus metric is automatically derived from the type of measured unit."""
    if unit is MeasuredUnit.COUNT:
        return Counter
    else:
        return Histogram


def init_prom_metrics():
    """In Prometheus clients, all metrics should be defined only once before use, along with their possible labels.
    This is not a technical limitation of Prometheus itself, but rather enforced by official clients."""
    for e in MetricName:
        base_args = {'name': e.name.lower(),
                     'documentation': e.name,
                     'labelnames': supported_label_names(e)}
        metric_type = unit_to_metric_type(e)
        if metric_type == Counter:
            prom_counters[e] = Counter(**base_args)
        elif metric_type == Histogram:
            prom_histograms[e] = Histogram(**base_args, buckets=buckets_by_unit(e.unit))


def update(metrics: List[MetricData]):
    """Update (increment/observe) new values after a job completes, etc."""
    for md in metrics:
        empty_labels = empty_label_names(md.name)
        all_labels = {**empty_labels, **md.labels}
        metric_type = unit_to_metric_type(md.name.unit)
        if metric_type == Counter:
            prom_counters[md.name].labels(**all_labels).inc(md.value)
        elif metric_type == Histogram:
            prom_histograms[md.name].labels(**all_labels).observe(md.value)
