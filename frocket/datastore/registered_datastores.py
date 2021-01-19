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


# TODO Consider thread-safe needs - in the store itself actually? (double creation is less an issue)
#  RedisStore itself is thanks to its underlying connection (and no other state), but there may be others
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
