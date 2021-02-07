from typing import Dict, Type
from frocket.common.tasks.base import BaseTaskRequest
from frocket.common.tasks.registration import RegistrationTaskRequest
from frocket.common.tasks.query import QueryTaskRequest
from frocket.worker.runners.base_task_runner import BaseTaskRunner
from frocket.worker.runners.query_task_runner import QueryTaskRunner
from frocket.worker.runners.registration_task_runner import RegistrationTaskRunner

REGISTERED_RUNNERS: Dict[Type[BaseTaskRequest], Type[BaseTaskRunner]] = {
    QueryTaskRequest: QueryTaskRunner,
    RegistrationTaskRequest: RegistrationTaskRunner
}
