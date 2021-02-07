import logging
from enum import Enum, auto
from frocket.common.config import config
from frocket.invoker.base_invoker import BaseInvoker
from frocket.invoker.jobs.job import Job
from frocket.invoker.impl.aws_lambda_invoker import AwsLambdaInvoker
from frocket.invoker.impl.work_queue_invoker import WorkQueueInvoker

logger = logging.getLogger(__name__)


class InvocationType(Enum):
    WORK_QUEUE = auto()
    AWS_LAMBDA = auto()


INVOKER_CLASSES = {
    InvocationType.WORK_QUEUE: WorkQueueInvoker,
    InvocationType.AWS_LAMBDA: AwsLambdaInvoker
}


def new_invoker(request_builder: Job) -> BaseInvoker:
    invoker_type = InvocationType[config.get("invoker").upper()]
    invoker_class = INVOKER_CLASSES[invoker_type]
    logger.info(f"Creating invoker type: {invoker_class.__name__}, for request builder type: {type(request_builder)}")
    return invoker_class(request_builder)
