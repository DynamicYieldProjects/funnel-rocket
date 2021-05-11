#  Copyright 2021 The Funnel Rocket Maintainers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
