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
