import logging
from typing import Optional, List, Dict, Union
import pandas as pd
from frocket.common.config import config
from frocket.common.metrics import MetricName, SourceAndMetricTuple, ALL_LABEL_NAMES
from frocket.invoker import prom_adapter

logger = logging.getLogger(__name__)

PANDAS_FLOAT_FORMAT = '{:.5f}'
pd.options.display.float_format = PANDAS_FLOAT_FORMAT.format

EXPORT_LASTRUN_FILE = config.get('metrics.export.lastrun', None)
EXPORT_TO_PROMETHEUS = config.bool('metrics.export.prometheus')

if EXPORT_TO_PROMETHEUS:
    prom_adapter.init_prom_metrics()


class MetricsFrame:
    def __init__(self, source_and_metrics: List[SourceAndMetricTuple]):
        self._sources = [ms.source for ms in source_and_metrics]
        self._metrics = [ms.metric for ms in source_and_metrics]
        self._df = None

    def _build_df(self):
        if self._df is not None:
            return

        metric_source_column = self._sources
        metric_name_column = [m.name.name for m in self._metrics]
        metric_value_column = [m.value for m in self._metrics]

        label_columns: Dict[str, List[Union[str, None]]] = {}
        for label_name in ALL_LABEL_NAMES:
            label_columns[label_name] = [None] * len(self._metrics)

        for i, metric in enumerate(self._metrics):
            for label_name, label_value in metric.labels.items():
                label_columns[label_name][i] = label_value

        df_columns = {'source': metric_source_column,
                      'metric': metric_name_column,
                      'value': metric_value_column,
                      **label_columns}
        self._df = pd.DataFrame(data=df_columns)

    def export(self) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            self._build_df()
            logger.debug(f"Types: {self._df.dtypes.index.tolist()}, data:\n{self._df}")

        if EXPORT_LASTRUN_FILE:
            self._to_lastrun_file(EXPORT_LASTRUN_FILE)
        if EXPORT_TO_PROMETHEUS:
            self._to_prometheus()

    def _to_prometheus(self) -> None:
        prom_adapter.update(self._metrics)

    def _to_lastrun_file(self, filename: str) -> None:
        self._build_df()
        if filename.lower().endswith('.parquet'):
            self._df.to_parquet(filename)
        else:
            self._df.to_csv(filename, float_format=PANDAS_FLOAT_FORMAT, index=False)

    # TODO check if all actual expected cost values are found?
    def total_cost(self) -> Optional[float]:
        cost_reports = [m.value for m in self._metrics if m.name == MetricName.COST_DOLLARS]
        num_reports = len(cost_reports)
        if num_reports == 0:
            logger.debug(f"Total cost: no metrics found")
            return None
        else:
            total_cost = sum(cost_reports)
            logger.debug(f"Total cost: ${total_cost:.6f} (sum of {num_reports} metric reports)")
            return total_cost
