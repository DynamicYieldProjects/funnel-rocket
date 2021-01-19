import logging
from typing import cast
from frocket.common.serializable import Envelope
from frocket.common.tasks.base import BaseTaskRequest
from frocket.common.metrics import MetricsBag, WorkerStartupLabel, ComponentLabel
from frocket.worker.impl.aws_lambda_metrics import AwsLambdaMetricsProvider
from frocket.worker.runners.base_task_runner import BaseTaskRunner, TaskRunnerContext
from frocket.common.config import config
from frocket.worker.runners.registered_runners import REGISTERED_RUNNERS

config.init_logging()
logger = logging.getLogger(__name__)
# Only set for newly-run Lambda instances (warm ones go straight to the handler function)
cold_start_flag = True


def is_cold_start():
    global cold_start_flag
    if cold_start_flag:
        cold_start_flag = False  # For next invocation
        return True
    else:
        return False


def init_task_metrics(lambda_context) -> MetricsBag:
    metrics = MetricsBag(component=ComponentLabel.WORKER,
                         env_metrics_provider=AwsLambdaMetricsProvider(lambda_context))
    if is_cold_start():
        metrics.set_label_enum(WorkerStartupLabel.COLD)
    else:
        metrics.set_label_enum(WorkerStartupLabel.WARM)
    return metrics


def lambda_handler(event, context):
    metrics = init_task_metrics(context)

    envelope = Envelope.from_dict(event)
    req = cast(BaseTaskRequest, envelope.open(expected_superclass=BaseTaskRequest))
    logger.info(f"Got request: {req}")

    result = None
    should_run, reject_reason = BaseTaskRunner.should_run(req)
    if should_run:
        runner_class = REGISTERED_RUNNERS[type(req)]
        runner = runner_class(req, TaskRunnerContext(metrics))
        result = runner.run()

    lambda_response = {
        'statusCode': 200,  # Retry mechanism relies on status written to datastore, not this status code
    }

    # The task result is written to datastore. This allows running test invocations of this Lambda
    if logger.isEnabledFor(logging.DEBUG):
        if result:
            lambda_response['result'] = result.to_json()
        else:
            lambda_response['reject_reason'] = reject_reason
    return lambda_response
