"""
Transform a given list of metrics from multiple sources (invoker, workers) into one DataFrame, for easy analysis.
Export the data into file and/or Prometheus, by configuration.
"""
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

PANDAS_FLOAT_FORMAT = '{:.5f}'  # No pesky scientific notation ;-)
pd.options.display.float_format = PANDAS_FLOAT_FORMAT.format

# 'Last run' file, if defines, stores the most recent job's metrics as a file in CSV or Parquet format (by extension)
EXPORT_LASTRUN_FILE = config.get('metrics.export.lastrun', None)
EXPORT_TO_PROMETHEUS = config.bool('metrics.export.prometheus')

if EXPORT_TO_PROMETHEUS:
    prom_adapter.init_prom_metrics()


class MetricsFrame:
    def __init__(self, source_and_metrics: List[SourceAndMetricTuple]):
        self._sources = [ms.source for ms in source_and_metrics]
        self._metrics = [ms.metric for ms in source_and_metrics]
        self._build_df()

    def _build_df(self):
        """
        Build the DataFrame: each row is one reported metric, but the DF is created with columns. Hence, we're creating
        columns here rather than rows.
        """
        metric_source_column = self._sources
        metric_name_column = [m.name.name for m in self._metrics]  # Metric names column
        metric_value_column = [m.value for m in self._metrics]     # Metric values column

        # Init empty columns for all possible label names.
        # Cells not not filled (see below) will remain empty (and possibly even entire columns)
        label_columns: Dict[str, List[Union[str, None]]] = {}
        for label_name in ALL_LABEL_NAMES:
            label_columns[label_name] = [None] * len(self._metrics)

        # Fill labels columns with what labels are actually set per metric
        for i, metric in enumerate(self._metrics):
            for label_name, label_value in metric.labels.items():
                label_columns[label_name][i] = label_value

        df_columns = {METRIC_SOURCE_COLUMN: metric_source_column,
                      METRIC_NAME_COLUMN: metric_name_column,
                      METRIC_VALUE_COLUMN: metric_value_column,
                      **label_columns}
        self._df = pd.DataFrame(data=df_columns)
        # logger.debug(f"Types: {self._df.dtypes.index.tolist()}, data:\n{self._df}")  # If needed

    def export(self) -> None:
        if EXPORT_LASTRUN_FILE:
            self._to_lastrun_file(EXPORT_LASTRUN_FILE)
        if EXPORT_TO_PROMETHEUS:
            self._to_prometheus()

    def _to_prometheus(self) -> None:
        prom_adapter.update(self._metrics)

    def _to_lastrun_file(self, filename: str) -> None:
        if filename.lower().endswith('.parquet'):
            self._df.to_parquet(filename, index=False)
        else:
            self._df.to_csv(filename, float_format=PANDAS_FLOAT_FORMAT, index=False)

    @property
    def dataframe(self) -> DataFrame:
        return self._df
