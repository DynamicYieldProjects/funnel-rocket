import logging
from typing import List, Dict, Union
import pandas as pd
from pandas import DataFrame
from frocket.common.config import config
from frocket.common.metrics import SourceAndMetricTuple, ALL_LABEL_NAMES
from frocket.invoker import prom_adapter

logger = logging.getLogger(__name__)

METRIC_SOURCE_COLUMN = 'source'
METRIC_NAME_COLUMN = 'metric'
METRIC_VALUE_COLUMN = 'value'

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

        df_columns = {METRIC_SOURCE_COLUMN: metric_source_column,
                      METRIC_NAME_COLUMN: metric_name_column,
                      METRIC_VALUE_COLUMN: metric_value_column,
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
            self._df.to_parquet(filename, index=False)
        else:
            self._df.to_csv(filename, float_format=PANDAS_FLOAT_FORMAT, index=False)

    @property
    def dataframe(self) -> DataFrame:
        return self._df
