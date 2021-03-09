"""
Build JobStats (returned to the client after job completion) - based mostly on the DataFrame of collected metrics from
the invoker and all workers.
"""
import logging
import sys
from typing import Optional, Union, List, Dict
import pandas
import numpy as np
from pandas import DataFrame
from frocket.common.config import config
from frocket.common.dataset import DatasetPartsInfo, PartNamingMethod
from frocket.common.tasks.base import JobStats, JobDatasetStats, JobInvokerStats, TimingStats, JobWorkerStats
from frocket.invoker.metrics_frame import MetricsFrame, METRIC_NAME_COLUMN, METRIC_VALUE_COLUMN, METRIC_SOURCE_COLUMN
from frocket.common.metrics import MetricName, ComponentLabel, SUCCESS_LABEL, MetricLabelEnum, \
    WorkerStartupLabel, LoadFromLabel

logger = logging.getLogger(__name__)

TASK_COMPLETION_GRANULARITY_SECONDS = 0.25  # Data series of task success over time is measured in this resolution
TIMING_PERCENTILES = [float(pct) for pct in config.get('stats.timing.percentiles').split(',')]
MIN_METRICS_FOR_PERCENTILES = 20  # Below this sample count, don't return percentiles
MIN_METRICS_FOR_99_PERCENTILE = 100  # Below this count, don't return 99th percentile
# List of keys to pull from Pandas' describe()
TIMING_DESCRIBE_KEYS = ['min', 'mean', 'max'] + [f"{int(pct*100)}%" for pct in TIMING_PERCENTILES]


def build_stats(frame: MetricsFrame, parts_info: DatasetPartsInfo = None) -> JobStats:
    df = frame.dataframe
    if df is None:  # In job failure cases
        return JobStats()

    if parts_info:
        ds_stats = JobDatasetStats(total_size=parts_info.total_size, parts=parts_info.total_parts)
    else:
        ds_stats = None

    # Invoker stats
    all_task_rows_df = _filter_by_label(df, ComponentLabel.WORKER)
    successful_task_rows_df = _filter_by_success(all_task_rows_df)
    total_tasks = _count_tasks(all_task_rows_df)
    failed_tasks = total_tasks - _count_tasks(successful_task_rows_df)

    invoker_stats = JobInvokerStats(
        enqueue_time=_sum_value(df, MetricName.ASYNC_ENQUEUE_SECONDS, single_value=True),
        poll_time=_sum_value(df, MetricName.ASYNC_POLL_SECONDS, single_value=True),
        total_tasks=total_tasks,
        failed_tasks=failed_tasks,
        task_success_over_time=_task_success_over_time(successful_task_rows_df)
        # TODO backlog add: lost_task_retries as counted by the invoker; support sync. invokers?
    )

    # Worker stats
    worker_stats = JobWorkerStats(
        cold_tasks=_count_tasks(_filter_by_label(successful_task_rows_df, WorkerStartupLabel.COLD)),
        warm_tasks=_count_tasks(_filter_by_label(successful_task_rows_df, WorkerStartupLabel.WARM)),
        scanned_rows=_sum_value(successful_task_rows_df, MetricName.SCANNED_ROWS, as_int=True),
        scanned_groups=_sum_value(successful_task_rows_df, MetricName.SCANNED_GROUPS, as_int=True),
        cache=_cache_performance(successful_task_rows_df),
        invoke_latency=_timing_stats(successful_task_rows_df, MetricName.INVOKE_TO_RUN_SECONDS),
        load_time=_timing_stats(successful_task_rows_df, MetricName.TASK_TOTAL_LOAD_SECONDS),
        total_time=_timing_stats(successful_task_rows_df, MetricName.TASK_TOTAL_RUN_SECONDS)
        # TODO backlog add: loaded_column_types - mapping of column type to count, which affects load time
    )

    job_stats = JobStats(
        total_time=_sum_value(df, MetricName.INVOKER_TOTAL_SECONDS, single_value=True),
        cost=_total_cost(df),
        dataset=ds_stats,
        invoker=invoker_stats,
        worker=worker_stats)
    return job_stats


def _task_success_over_time(task_rows_df: DataFrame) -> Dict[float, int]:
    """Return a sparse series of data points - for each time slot (e.g. 0.25 secs) since the job started, return how
    many tasks completed successfully in that slot. Non-cumulative, does not include zeros."""
    task_duration_rows = _filter_by_metrics(
        task_rows_df, metrics=[MetricName.INVOKE_TO_RUN_SECONDS, MetricName.TASK_TOTAL_RUN_SECONDS])
    task_durations = task_duration_rows.groupby(METRIC_SOURCE_COLUMN)[METRIC_VALUE_COLUMN].sum()
    quantized_task_durations = \
        np.ceil(task_durations / TASK_COMPLETION_GRANULARITY_SECONDS) * TASK_COMPLETION_GRANULARITY_SECONDS
    return quantized_task_durations.value_counts().sort_index().to_dict()


def _cache_performance(task_rows_df: DataFrame) -> Dict[str, int]:
    return {
        # Note the 'source' is always the case for locally-loaded files, in which case caching is N/A.
        'source': _count_tasks(_filter_by_label(task_rows_df, LoadFromLabel.SOURCE)),
        'diskCache': _count_tasks(_filter_by_label(task_rows_df, LoadFromLabel.DISK_CACHE))
    }


def _sum_value(df: DataFrame, metric: MetricName,
               single_value: bool = False,
               as_int: bool = False) -> Union[float, int, None]:
    df = _filter_by_metrics(df, metric)
    if single_value:
        assert len(df) <= 1
    if df.empty:
        return None
    else:
        values_sum = df[METRIC_VALUE_COLUMN].sum()
        return int(values_sum) if as_int else float(values_sum)


def _count(df: DataFrame, metric: MetricName) -> int:
    return _filter_by_metrics(df, metric)[METRIC_VALUE_COLUMN].count()


def _timing_stats(task_rows_df: DataFrame, metric: MetricName) -> TimingStats:
    values_df = _filter_by_metrics(task_rows_df, metric)[METRIC_VALUE_COLUMN]
    if len(values_df) < MIN_METRICS_FOR_PERCENTILES:
        percentiles = [0.5]
    else:
        percentiles = TIMING_PERCENTILES
        if len(values_df) < MIN_METRICS_FOR_99_PERCENTILE:
            percentiles = [pct for pct in percentiles if pct < 0.99]

    raw_stats = values_df.describe(percentiles=percentiles).to_dict()
    return {k: v for k, v in raw_stats.items()
            if k in TIMING_DESCRIBE_KEYS and not np.isnan(v)}


def _filter_by_metrics(df: DataFrame, metrics: Union[MetricName, List[MetricName]]) -> DataFrame:
    if type(metrics) is MetricName:
        return df[df[METRIC_NAME_COLUMN] == metrics.name]
    else:
        return df[df[METRIC_NAME_COLUMN].isin([m.name for m in metrics])]


def _filter_by_label(df: DataFrame, label: MetricLabelEnum) -> DataFrame:
    return df[df[label.label_name] == label.label_value.lower()]


def _filter_by_success(df: DataFrame, value: bool = True) -> DataFrame:
    return df[df[SUCCESS_LABEL] == str(value)]


def _count_tasks(task_rows_df: DataFrame) -> int:
    """Each task attempt (e.g. task index 117, attempt 2) has a unique name in the source column, which ofc appears in
    multiple rows. This count the unique count of task attempt IDs in the given DF."""
    return task_rows_df[METRIC_SOURCE_COLUMN].nunique()


def _total_cost(df: DataFrame) -> Optional[float]:
    cost_reports_df = _filter_by_metrics(df, MetricName.COST_DOLLARS)
    num_reports = len(cost_reports_df)
    if num_reports == 0:
        logger.debug(f"Total cost: no metrics found")
        return None
    else:
        total_cost = float(cost_reports_df[METRIC_VALUE_COLUMN].sum())
        logger.debug(f"Total cost: ${total_cost:.6f} (sum of {num_reports} metric reports)")
        return total_cost


# Stand-alone testing
if __name__ == "__main__":
    config.init_logging(force_level=logging.DEBUG, force_console_output=True)
    filename = config.get('metrics.export.lastrun', None)
    if not filename:
        sys.exit('No lastrun file defined')

    df = pandas.read_parquet(filename)
    dummy_frame = MetricsFrame([])
    dummy_frame._df = df
    dummy_parts_info = DatasetPartsInfo(naming_method=PartNamingMethod.LIST, total_parts=4, total_size=1024)
    build_stats(dummy_frame, dummy_parts_info)
