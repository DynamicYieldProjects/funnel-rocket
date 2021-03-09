"""
lambda_handler() in this module is the AWS Lambda's defined entrypoint.
There's minimal code here that's Lambda-specific (== a good thing).
"""
import logging
from typing import cast
from frocket.common.serializable import Envelope
from frocket.common.tasks.base import BaseTaskRequest
from frocket.common.metrics import MetricsBag, WorkerStartupLabel, ComponentLabel
from frocket.worker.impl.aws_lambda_metrics import AwsLambdaMetricsProvider
from frocket.worker.runners.base_task_runner import BaseTaskRunner, TaskRunnerContext
from frocket.common.config import config
from frocket.worker.runners.registered_runners import REGISTERED_RUNNERS

config.init_lambda_logging()  # Adapted to the logger being already-inited by the Lambda runtime
logger = logging.getLogger(__name__)

# This flag only set when a new Lambda instance is cold-started. Warm lambdas would go straight to the handler function.
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
    # The event JSON was already parsed to dict by the Lambda runtime -
    # now read from that dict that actual task request object
    envelope = Envelope.from_dict(event)
    req = cast(BaseTaskRequest, envelope.open(expected_superclass=BaseTaskRequest))
    logger.info(f"Got request: {req}")

    result = None
    should_run, reject_reason = BaseTaskRunner.should_run(req)
    if should_run:
        runner_class = REGISTERED_RUNNERS[type(req)]
        runner = runner_class(req, TaskRunnerContext(metrics))
        result = runner.run()

    """
    A note about the Lambda response: unlike most request/response Lambdas, Funnel Rocket's invoker does not rely on the
    function's result coming from the Lambda directly (as it's invoked async.) but rather always through the datastore.
    The retry mechanism is also based on polling the tasks' status and result payload in the datastore, hence the 
    Lambda itself should not normally return a non-200 status (unless it crashed unexpectedly), and the Lambda should
    be configured to have no retries at the AWS level.
    """

    lambda_response = {
        'statusCode': 200,
    }

    # Getting the result object in the Lambda response is still useful for manual testing
    if logger.isEnabledFor(logging.DEBUG):
        if result:
            lambda_response['result'] = result.to_json()
        else:
            lambda_response['reject_reason'] = reject_reason
    return lambda_response
