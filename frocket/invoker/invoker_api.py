"""
This is the "Funnel Rocket" frontend API - wrappedby the CLI & API server, and may be embeddable in other apps.
Clients are not expected to bypass this API (call the datastore directly, initialize an invoker, etc.)
"""
import concurrent.futures
import logging
import time
from typing import List, Optional, cast, Union
from frocket.common.config import config
from frocket.common.dataset import DatasetInfo, DatasetShortSchema, DatasetSchema, DatasetPartsInfo
from frocket.common.tasks.registration import RegistrationJobResult, RegisterArgs, UnregisterApiResult
from frocket.common.tasks.query import QueryJobResult
from frocket.common.tasks.async_tracker import AsyncJobTracker, AsyncJobStatusUpdater
from frocket.common.validation.query_validator import QueryValidator
from frocket.common.validation.result import QueryValidationResult
from frocket.datastore.registered_datastores import get_datastore
from frocket.invoker.jobs.query_job import QueryJob
from frocket.invoker.jobs.registration_job import RegistrationJob
from frocket.invoker.impl.registered_invokers import new_invoker

logger = logging.getLogger(__name__)
executor = concurrent.futures.ThreadPoolExecutor()

# TODO backlog allow configurable timeout per job type (async or not)
ASYNC_MAX_WAIT = config.int("invoker.run.timeout") * 1.1  # Adding a bit of grace around the invoker


def _unregister_safety_interval() -> int:
    """How long after a dataset was last used to block unregister (can be set to zero, or overidden with force=True)."""
    interval = config.get('unregister.last.used.interval', None)
    if not interval:  # Not defined, or empty string (explicit '0' is truthy)
        interval = config.int('invoker.run.timeout') * 2
    else:
        interval = int(interval)
    return interval


def register_dataset(args: RegisterArgs) -> RegistrationJobResult:
    request_builder = RegistrationJob(args)
    invoker = new_invoker(request_builder)
    result = cast(RegistrationJobResult, invoker.run())
    logger.info(f"Registration {'successful' if result.success else f'failed! {result.error_message}'}")
    return result


def register_dataset_async(args: RegisterArgs, set_max_wait: bool = True) -> AsyncJobTracker:
    """The async version starts the invoker in a separate thread and then returns, handing back
    an AsyncJobTracker to poll for progress/completion."""
    def worker(register_args, async_status):
        invoker = new_invoker(RegistrationJob(register_args))
        return invoker.run(async_status)

    async_status = AsyncJobStatusUpdater(max_wait=(ASYNC_MAX_WAIT if set_max_wait else None))
    executor.submit(worker, args, async_status)
    logger.info(f"Submitted async registration for dataset named {args.name} in basepath {args.basepath}")
    return async_status


def get_dataset(name: str, throw_if_missing: bool = False) -> Optional[DatasetInfo]:
    dataset = get_datastore().dataset_info(name)
    if not dataset and throw_if_missing:
        raise Exception(f"Dataset '{name}' not found")
    return dataset


def get_dataset_schema(dataset: DatasetInfo, full: bool = False) -> Union[DatasetSchema, DatasetShortSchema]:
    return get_datastore().schema(dataset) if full else get_datastore().short_schema(dataset)


def get_dataset_parts(dataset: DatasetInfo) -> DatasetPartsInfo:
    return get_datastore().dataset_parts_info(dataset)


def unregister_dataset(name: str, force: bool = False) -> UnregisterApiResult:
    dataset = get_dataset(name=name)
    if not dataset:
        return UnregisterApiResult(success=True, error_message=None,
                                   dataset_found=False, dataset_last_used=None)

    datastore = get_datastore()
    last_used = datastore.last_used(dataset)
    if last_used:
        time_since_used = int(time.time() - last_used)
        safety_interval = _unregister_safety_interval()
        message = f"Dataset was last used {time_since_used} seconds ago, which is less than safety interval " \
                  f"{safety_interval}. Use the 'force' parameter to unregister anyway."
        if safety_interval > time_since_used and not force:
            return UnregisterApiResult(success=False, error_message=message,
                                       dataset_found=True, dataset_last_used=last_used)

    get_datastore().remove_dataset_info(name)
    return UnregisterApiResult(success=True, error_message=None,
                               dataset_found=True, dataset_last_used=last_used)


def expand_and_validate_query(dataset: DatasetInfo, query: dict) -> QueryValidationResult:
    short_schema = get_dataset_schema(dataset)
    return QueryValidator(query, dataset, short_schema).expand_and_validate()


def _build_query_job(dataset: DatasetInfo,
                     query: dict,
                     validation_result: QueryValidationResult) -> QueryJob:
    """If the query was already validated, skip re-validating."""
    if validation_result:
        assert validation_result.success
        assert query in [validation_result.source_query, validation_result.expanded_query]
    else:
        validation_result = expand_and_validate_query(dataset, query)
        if not validation_result.success:
            raise Exception(f"Query validation failed: {validation_result.error_message}")

    get_datastore().mark_used(dataset)
    dataset_parts = get_datastore().dataset_parts_info(dataset)
    short_schema = get_datastore().short_schema(dataset)
    return QueryJob(dataset, dataset_parts, short_schema,
                    validation_result.expanded_query, validation_result.used_columns)


def run_query(dataset: DatasetInfo,
              query: dict,
              validation_result: QueryValidationResult = None) -> QueryJobResult:
    job_builder = _build_query_job(dataset, query, validation_result)
    invoker = new_invoker(job_builder)
    result = cast(QueryJobResult, invoker.run())
    if result.success:
        logger.info("Query completed successfully")
    else:
        logger.error(f"Query failed with message: {result.error_message}")
    return result


def run_query_async(dataset: DatasetInfo,
                    query: dict,
                    set_max_wait: bool = True,
                    validation_result: QueryValidationResult = None) -> AsyncJobTracker:
    """The async version starts the invoker in a separate thread and then returns, handing back
    an AsyncJobTracker to poll for progress/completion."""
    def worker(job_builder, async_status):
        invoker = new_invoker(job_builder)
        return invoker.run(async_status)

    job_builder = _build_query_job(dataset, query, validation_result)
    async_status = AsyncJobStatusUpdater(max_wait=(ASYNC_MAX_WAIT if set_max_wait else None))
    executor.submit(worker, job_builder, async_status)
    logger.info(f"Submitted async query for dataset '{dataset.id.name}'")
    return async_status


def list_datasets() -> List[DatasetInfo]:
    datasets = get_datastore().datasets()
    return datasets
