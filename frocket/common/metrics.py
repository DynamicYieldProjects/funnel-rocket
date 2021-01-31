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


# The purpose of MetricLabelEnum is to easily set a label to one of its allowed (enum) values.
# The label name is inferred from the class name (minus the 'Label' suffix).
# Label names and values are transformed to snake-case to match the understated Prometheus style.
class MetricLabelEnum(AutoNamedEnum):
    # noinspection PyUnusedLocal
    def __init__(self, *args):
        self.__class__._set_label_name()
        self.label_value = self.name.lower()

    @classmethod
    def _set_label_name(cls):
        if hasattr(cls, 'label_name'):
            return
        key = cls.__name__
        # Remove suffix if exists. TODO When moving to Python 3.9 use removesuffix()
        key = key[:-len('Label')] if key.endswith('Label') else key
        cls.label_name = underscore(key)


class ComponentLabel(MetricLabelEnum):
    INVOKER = auto()
    WORKER = auto()
    APISERVER = auto()


class WorkerStartupLabel(MetricLabelEnum):
    COLD = auto()
    WARM = auto()


class LoadFromLabel(MetricLabelEnum):
    SOURCE = auto()
    DISK_CACHE = auto()
    MEMORY_CACHE = auto()


class PartSelectMethodLabel(MetricLabelEnum):
    SET_BY_INVOKER = auto()
    SPECIFIC_CANDIDATE = auto()
    RANDOM_NO_CANDIDATES = auto()
    RANDOM_CANDIDATES_TAKEN = auto()


class JobTypeLabel(MetricLabelEnum):
    REGISTER = auto()
    QUERY = auto()


class MeasuredUnit(EnumSerializableByName):
    # noinspection PyUnusedLocal
    def __init__(self, *args):
        super().__init__()
        self.suffix = f"_{self.name}"

    COUNT = auto()
    SECONDS = auto()
    ROWS = auto()
    GROUPS = auto()
    BYTES = auto()
    DOLLARS = auto()
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


COMPONENT_DURATION_METRIC: Dict[ComponentLabel, MetricName] = {
    ComponentLabel.WORKER: MetricName.TASK_TOTAL_RUN_SECONDS,
    ComponentLabel.INVOKER: MetricName.INVOKER_TOTAL_SECONDS
}

ALL_COMPONENT_COMMON_LABELS: List[MetricLabel] = [ComponentLabel, SUCCESS_LABEL, DATASET_LABEL, JobTypeLabel]

COMPONENT_COMMON_LABELS: Dict[ComponentLabel, List[MetricLabel]] = {
    ComponentLabel.WORKER: [*ALL_COMPONENT_COMMON_LABELS, WorkerStartupLabel, LoadFromLabel, PartSelectMethodLabel],
    ComponentLabel.INVOKER: [*ALL_COMPONENT_COMMON_LABELS]
}

LabelsDict = Dict[str, str]


def label_to_str(label: Union[MetricLabel, Type[MetricLabelEnum]]) -> str:
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
    name: MetricName
    value: float
    labels: Optional[LabelsDict] = None

    def with_added_labels(self, added_labels: LabelsDict):
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
    source: str
    metric: MetricData


class EnvironmentMetricsProvider(metaclass=ABCMeta):
    def finalize_metrics(self, run_duration: float = None) -> List[MetricData]:
        metrics = [self._memory_bytes(), self._cost_dollars(run_duration)]
        return [m for m in metrics if m]

    @abstractmethod
    def _memory_bytes(self) -> Optional[MetricData]:
        pass

    @abstractmethod
    def _cost_dollars(self, duration: float = None) -> Optional[MetricData]:
        pass


class MetricsBag:
    def __init__(self, component: ComponentLabel, env_metrics_provider: EnvironmentMetricsProvider = None):
        self._component: ComponentLabel = component
        self._metrics: Dict[MetricName, MetricData] = {}
        self._env_metrics_provider = env_metrics_provider
        self._finalized = False
        self._labels: LabelsDict = {}
        self.set_label_enum(component)

    def set_label(self, name: str, value: Union[str, int, float, bool]):
        self._do_set_label(name, value)

    def set_label_enum(self, e: MetricLabelEnum):
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
        return MeasuredBlock(self, name)

    @property
    def success(self) -> Optional[bool]:
        return self._labels.get(SUCCESS_LABEL, None)

    def finalize(self, success: bool, added_labels: LabelsDict = None) -> List[MetricData]:
        self.fail_if_finalized()
        self.set_label(SUCCESS_LABEL, success)
        self._add_env_metrics()
        self._finalized = True

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


# TODO set additional labels etc.? + DOC how to use + LATER consider automatic exception capture into label
class MeasuredBlock:
    def __init__(self, parent: MetricsBag, name: MetricName):
        self._parent = parent
        self._name = name
        self._start_time = time.time()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        total_time = time.time() - self._start_time
        self._parent.set_metric(self._name, total_time)
