import os
import pytest
from frocket.common.config import config
from frocket.datastore.registered_datastores import get_datastore, get_blobstore

TEST_REDIS_HOST = os.environ.get('TEST_REDIS_HOST', 'localhost')
TEST_REDIS_PORT = os.environ.get('TEST_REDIS_PORT', '6379')
TEST_REDIS_DB = os.environ.get('TEST_REDIS_DB', '0')


@pytest.fixture(scope="session", autouse=True)
def init_redis():
    config['redis.host'] = TEST_REDIS_HOST
    config['redis.port'] = TEST_REDIS_PORT
    config['redis.db'] = TEST_REDIS_DB
    config['datastore.redis.prefix'] = "frocket:tests:"
    print(get_datastore(), get_blobstore())  # Fail on no connection, print connection details
