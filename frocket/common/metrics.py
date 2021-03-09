"""
Base types for metrics support.

This was designed with Prometheus conventions & requirements (which aligns pretty well with OpenMetrics, surprise
surprise), however these classes are generally useful with other systems. This is specifically true, IMHO,
since good Prometheus compatibility means stricter definitions, e.g always use the same label set for a
given metric (rather than free for all whenever you report it), specify the *unit* of the metric as suffix, limit the
cardinality of label values, etc.
"""
import logging
import time
from abc import ABCMeta, abstractmethod
from enum import auto
from typing import Union, Dict, Optional, List, NamedTuple, Type, Set
from dataclasses import dataclass
from inflection import underscore
from frocket.common.serializable import AutoNamedEnum, EnumSerializableByName
from frocket.common.helpers.utils import memoize

logger = logging.getLogger(__name__)


class MetricLabelEnum(AutoNamedEnum):
    """
    Base class for easily setting labels to metrics - the label name being inferred from the enum class name,
    and the value from the member name. Label names and values are transformed to snake-case to match the understated
    Prometheus style.
    """
    # noinspection PyUnusedLocal
    def __init__(self, *args):
        self.__class__._set_label_name()
        self.label_value = self.name.lower()

    @classmethod
    def _set_label_name(cls):
        """Label name is based on the class name, so needs to be done once."""
        if hasattr(cls, 'label_name'):
            return
        key = cls.__name__
        # Remove suffix if exists. TODO backlog when moving to Python 3.9 use removesuffix()
        key = key[:-len('Label')] if key.endswith('Label') else key
        cls.label_name = underscore(key)


class ComponentLabel(MetricLabelEnum):
    """Label for specifying which system component is reporting a metric."""
    INVOKER = auto()
    WORKER = auto()
    APISERVER = auto()


class WorkerStartupLabel(MetricLabelEnum):
    """Serverless worker types have a cold-start/warm-start distinction, which is meaning when monitoring timing."""
    COLD = auto()
    WARM = auto()


class LoadFromLabel(MetricLabelEnum):
    """Whether part files are loaded from origin, or some of the possible cache layers."""
    SOURCE = auto()
    DISK_CACHE = auto()
    MEMORY_CACHE = auto()


class PartSelectMethodLabel(MetricLabelEnum):
    """For jobs supporting part selection by the worker side, we want to monitor whether the worker has successfully
    selected a preferred (locally cached) part, or whether it fell back to choosing a 'random' available part due to
    either having no relevant cached parts, or because they were already taken by another worker."""
    SET_BY_INVOKER = auto()
    SPECIFIC_CANDIDATE = auto()
    RANDOM_NO_CANDIDATES = auto()
    RANDOM_CANDIDATES_TAKEN = auto()


class JobTypeLabel(MetricLabelEnum):
    REGISTER = auto()
    QUERY = auto()


class MeasuredUnit(EnumSerializableByName):
    """Each metric has a unit name as its suffix, which impacts the actual metric type to be created, and make life
    easier for human operators."""
    # noinspection PyUnusedLocal
    def __init__(self, *args):
        super().__init__()
        self.suffix = f"_{self.name}"

    # COUNT is special in that we're measuring occurences of something (a natural number, using a histogram is N/A)
    COUNT = auto()
    SECONDS = auto()
    ROWS = auto()
    GROUPS = auto()
    BYTES = auto()
    DOLLARS = auto()
    # OTHER is assumed to be something that's not a 'count of things'
    OTHER = auto()

    @staticmethod
    def unitof(metric_name: str):
        for e in MeasuredUnit:
            if metric_name.endswith(e.suffix):
                return e
        return MeasuredUnit.OTHER


MetricLabel = Union[MetricLabelEnum, str]

SUCCESS_LABEL = 'success'
DATASET_LABEL = 'dataset'


class MetricName(EnumSerializableByName):
    """All metrics must be defined as members and be associated with a single reporting component
    (to prevent confusion). Any additional metric-specific labels should also be defined at creation."""
    def __init__(self,
                 component: ComponentLabel,
                 unit: MeasuredUnit = None,
                 additional_labels: List[MetricLabel] = None):
        super().__init__()
        self.component = component
        self.unit = unit or MeasuredUnit.unitof(self.name)
        self.additional_labels = additional_labels

    INVOKE_TO_RUN_SECONDS = ComponentLabel.WORKER
    TASK_TOTAL_RUN_SECONDS = ComponentLabel.WORKER
    TASK_TOTAL_LOAD_SECONDS = ComponentLabel.WORKER
    TASK_PREFLIGHT_SLEEP_SECONDS = ComponentLabel.WORKER
    TASK_DOWNLOAD_SECONDS = ComponentLabel.WORKER
    TASK_LOAD_FILE_SECONDS = ComponentLabel.WORKER
    TASK_RUN_QUERY_SECONDS = ComponentLabel.WORKER
    SCANNED_ROWS = ComponentLabel.WORKER
    SCANNED_GROUPS = ComponentLabel.WORKER
    MACHINE_MEMORY_BYTES = ComponentLabel.WORKER
    COST_DOLLARS = ComponentLabel.WORKER
    INVOKER_TOTAL_SECONDS = ComponentLabel.INVOKER
    ASYNC_ENQUEUE_SECONDS = ComponentLabel.INVOKER
    ASYNC_POLL_SECONDS = ComponentLabel.INVOKER


# Main metric for work duration (scope is a job for the invoker, a single task in a job for the worker)
COMPONENT_DURATION_METRIC: Dict[ComponentLabel, MetricName] = {
    ComponentLabel.WORKER: MetricName.TASK_TOTAL_RUN_SECONDS,
    ComponentLabel.INVOKER: MetricName.INVOKER_TOTAL_SECONDS
}

# There's a standard common set of labels for *all* metrics, then another set depending on the component.
# TODO backlog rethink this set: on one hand, having a common set allows filtering by label values across metrics
#  (thus preventing mismatches when looking at graphs). On the other, this inflates the 'cardinality' of metrics by
#  a lot (each extra labels with, say, 3 possible values, means up to x3 the no. of unique label sets that Prometheus
#  would need to track per metrics. Perhaps this should be configurable...
ALL_COMPONENT_COMMON_LABELS: List[MetricLabel] = [ComponentLabel, SUCCESS_LABEL, DATASET_LABEL, JobTypeLabel]

COMPONENT_COMMON_LABELS: Dict[ComponentLabel, List[MetricLabel]] = {
    ComponentLabel.WORKER: [*ALL_COMPONENT_COMMON_LABELS, WorkerStartupLabel, LoadFromLabel, PartSelectMethodLabel],
    ComponentLabel.INVOKER: [*ALL_COMPONENT_COMMON_LABELS]
}

# Label values can be accepted as numbers, but are then stored as text (which is how metric systems get them)
LabelsDict = Dict[str, str]


def label_to_str(label: Union[MetricLabel, Type[MetricLabelEnum]]) -> str:
    """Given either a string, a MetricLabel enum *member or class* - return the label name"""
    if type(label) is str:
        return label
    elif isinstance(label, MetricLabelEnum):
        return label.label_name
    elif isinstance(label, type) and issubclass(label, MetricLabelEnum):
        return label.label_name
    else:
        raise Exception(f"Invalid label: {label} ({type(label)})")


@memoize
def supported_label_names(metric: MetricName) -> List[str]:
    all_labels = COMPONENT_COMMON_LABELS[metric.component] + (metric.additional_labels or [])
    return [label_to_str(label) for label in all_labels]


@memoize
def empty_label_names(metric: MetricName) -> Dict[str, Union[str, None]]:
    return {label: None for label in supported_label_names(metric)}


ALL_LABEL_NAMES: Set[str] = set()
for m in MetricName:
    ALL_LABEL_NAMES.update(supported_label_names(m))


@dataclass(frozen=True)
class MetricData:
    """A instance of a reported metric - name, value and optional metric-specific labels."""
    name: MetricName
    value: float
    labels: Optional[LabelsDict] = None

    def with_added_labels(self, added_labels: LabelsDict):
        """Add "inherited" labels from the component level/all components level."""
        if self.labels:
            for k in added_labels.keys():
                if self.labels.get(k, None) is not None:
                    raise Exception(f"Trying to override existing initialized labels. "
                                    f"Metric: {self}, added labels: {added_labels}")
            merged_labels = {**self.labels, **added_labels}
        else:
            merged_labels = added_labels
        return MetricData(name=self.name, value=self.value, labels=merged_labels)


class SourceAndMetricTuple(NamedTuple):
    """For passing along a metric with the specific source which reported it (e.g. specific task names)."""
    source: str
    metric: MetricData


class EnvironmentMetricsProvider(metaclass=ABCMeta):
    """A base class for providing metrics relevant to the runtime environment (AWS Lambda, generic processes,
    and future runtimes), used by the worker. Not all providers may support all possible metrics."""
    def finalize_metrics(self, run_duration: float = None) -> List[MetricData]:
        metrics = [self._memory_bytes(), self._cost_dollars(run_duration)]
        return [m for m in metrics if m]

    @abstractmethod
    def _memory_bytes(self) -> Optional[MetricData]:
        """Runtime environment-dependent implementation for total physical memory (whether used or free"""
        pass

    @abstractmethod
    def _cost_dollars(self, duration: float = None) -> Optional[MetricData]:
        """environment-specific approximate cost calculation - if possible to get."""
        pass


class MetricsBag:
    """A MetricsBag object accompanies a job (at the invoker level) or a task (at the worker level) along its lifecycle,
    being propagated to all relevant modules through the process. Metrics & common labels are reported through it."""
    def __init__(self, component: ComponentLabel, env_metrics_provider: EnvironmentMetricsProvider = None):
        self._component: ComponentLabel = component
        self._metrics: Dict[MetricName, MetricData] = {}
        self._env_metrics_provider = env_metrics_provider
        self._finalized = False
        self._labels: LabelsDict = {}  # Labels (value->name) common to all metrics in this bag, rather than one metric
        self.set_label_enum(component)

    def set_label(self, name: str, value: Union[str, int, float, bool]):
        self._do_set_label(name, value)

    def set_label_enum(self, e: MetricLabelEnum):
        """Easier, less error-prone method for setting labels - just pass the appropriate enum member."""
        self._do_set_label(e, e.label_value)

    def _do_set_label(self, label: MetricLabel, value):
        self.fail_if_finalized()
        string_name = label_to_str(label)
        lookup = label if type(label) is str else type(label)
        if lookup not in COMPONENT_COMMON_LABELS[self._component]:
            raise Exception(f"Label {label} ({type(label)}) not supported for {self._component}")

        if string_name in self._labels:
            raise Exception(f"Label '{string_name}' already set for {self._component}")
        self._labels[string_name] = str(value)

    def set_metric(self, metric: MetricName, value: float,
                   additional_labels: Dict[MetricLabel, str] = None):
        self.fail_if_finalized()
        if metric.component != self._component:
            raise Exception(f"Metric {metric} not applicable for {self._component}")
        elif metric in self._metrics:
            raise Exception(f"Metric {metric} already set")
        elif additional_labels:
            for k in additional_labels.keys():
                if k not in metric.additional_labels:
                    raise Exception(f"Label {k} not applicable for {metric}")

        data = MetricData(metric, value, additional_labels)
        self._metrics[metric] = data

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Set {data}")

    def measure(self, name: MetricName):
        """Start a 'context manager' for measuring a block of code to the given metric. When the code block ends,
        its duration will be automatically added to this bag."""
        return MeasuredBlock(self, name)

    @property
    def success(self) -> Optional[bool]:
        return self._labels.get(SUCCESS_LABEL, None)

    def finalize(self, success: bool, added_labels: LabelsDict = None) -> List[MetricData]:
        """Prepare metrics for shipping (from worker back to invoker, through the data store). No further metrics
        can be collected on this bag, for consistency."""
        self.fail_if_finalized()
        self.set_label(SUCCESS_LABEL, success)
        self._add_env_metrics()
        self._finalized = True

        # The returned metrics get all inherited labels from this bag
        common_labels = {**(added_labels or {}),
                         **self._labels}
        result = [m.with_added_labels(common_labels)
                  for m in self._metrics.values()]
        return result

    def _add_env_metrics(self):
        if not self._env_metrics_provider:
            return

        duration_metric = self._metrics.get(COMPONENT_DURATION_METRIC[self._component], None)
        run_duration = duration_metric.value if duration_metric else None
        env_metrics = self._env_metrics_provider.finalize_metrics(run_duration)
        self._metrics.update({m.name: m for m in env_metrics})

    def fail_if_finalized(self):
        if self._finalized:
            raise Exception("Already finalized")

    def __getitem__(self, metric: MetricName) -> Optional[float]:
        if metric.name in self._metrics:
            return self._metrics[metric].value
        else:
            return None

    def label_value(self, label: Union[MetricLabel, Type[MetricLabelEnum]]):
        return self._labels.get(label_to_str(label), None)


class MeasuredBlock:
    """
    See MetricsBag.measure() method above: measure a code block and report its duration back to the parent bag,
    as the given metric's value.
    TODO backlog consider capturing any exceptions in the code block before re-raising them,
     so a success=False label can be applied to this metric.
    """
    def __init__(self, parent: MetricsBag, name: MetricName):
        self._parent = parent
        self._name = name
        self._start_time = time.time()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        total_time = time.time() - self._start_time
        self._parent.set_metric(self._name, total_time)
