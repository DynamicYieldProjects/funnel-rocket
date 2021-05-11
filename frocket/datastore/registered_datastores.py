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
from frocket.common.config import config
from frocket.common.helpers.utils import memoize
from frocket.datastore.datastore import Datastore
from frocket.datastore.blobstore import Blobstore
from frocket.datastore.redis_store import RedisStore

logger = logging.getLogger(__name__)

DATASTORE_CLASSES = {
    "redis": RedisStore,
}

BLOBSTORE_CLASSES = {
    "redis": RedisStore,
}


# TODO backlog consider thread-safety here: while RedisStore is thread-safe and having more than one is ok, future
#  implementations may not be? (or should be required to)
def _get_store(store_kind: str, store_mapping: dict):
    store_class = store_mapping[config.get(store_kind).lower()]
    store = store_class(role=store_kind)
    logger.info(f"Initialized {store}")
    return store


@memoize
def get_datastore() -> Datastore:
    return _get_store("datastore", DATASTORE_CLASSES)


@memoize
def get_blobstore() -> Blobstore:
    return _get_store("blobstore", BLOBSTORE_CLASSES)
