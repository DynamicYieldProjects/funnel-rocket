"""
A worker that gets its tasks by a blocking dequeue from the datastore. Doesn't get any simpler -
but is easily scalable, and requires no load balancer or orchestrator (except for the queue's atomic guarantees).

TODO backlog having a cache-friendly task assignment would require more work, if it makes sense to do.
"""
import logging
from frocket.common.metrics import MetricsBag, WorkerStartupLabel, ComponentLabel
from frocket.common.tasks.base import BaseTaskRequest
from frocket.datastore.registered_datastores import get_datastore
from frocket.worker.impl.generic_env_metrics import GenericEnvMetricsProvider
from frocket.worker.runners.base_task_runner import BaseTaskRunner, TaskRunnerContext
from frocket.common.config import config
from frocket.worker.runners.registered_runners import REGISTERED_RUNNERS

config.init_logging()
logger = logging.getLogger(__name__)
datastore = get_datastore()


def handle(req: BaseTaskRequest) -> None:
    metrics = MetricsBag(component=ComponentLabel.WORKER,
                         env_metrics_provider=GenericEnvMetricsProvider())
    metrics.set_label_enum(WorkerStartupLabel.WARM)  # Always warm this worker is, uhmmhmmhmmhmm

    runner_class = REGISTERED_RUNNERS[type(req)]
    runner = runner_class(req, TaskRunnerContext(metrics))
    result = runner.run()
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(result.to_json())


def main_loop():
    # TODO backlog currently workers that encounter an unexpected data format will crash rather than continuing to
    #  consume and (probably) fail. This has a pro (outdated worker versions fail fast), but of course also cons -
    #  consider the desired/configurable behavior (e.g. crash after N unexpected errors?)
    try:
        while True:
            logger.info('Waiting for work...')
            req: BaseTaskRequest = datastore.dequeue()
            if req:
                logger.info(f"Got request: {req}")

                should_run, reject_reason = BaseTaskRunner.should_run(req)
                if should_run:
                    handle(req)
                else:
                    logger.warning(f"Request rejected: {reject_reason}")
    except KeyboardInterrupt:
        logger.info('Bye')


main_loop()
