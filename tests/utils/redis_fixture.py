import os
import pytest
from frocket.common.config import config, ConfigDict
from frocket.datastore.registered_datastores import get_datastore, get_blobstore

TEST_REDIS_HOST = os.environ.get('TEST_REDIS_HOST', 'localhost')
TEST_REDIS_PORT = os.environ.get('TEST_REDIS_PORT', '6379')
TEST_REDIS_DB = os.environ.get('TEST_REDIS_DB', '0')


@pytest.fixture(scope="session", autouse=True)
def init_test_redis_settings():
    config['redis.host'] = TEST_REDIS_HOST
    config['redis.port'] = TEST_REDIS_PORT
    config['redis.db'] = TEST_REDIS_DB
    print(get_datastore(), get_blobstore())  # Fail on no connection, print connection details


def get_test_redis_env_variables():
    return {
        ConfigDict.to_env_variable(key): config.get(key)
        for key in ['redis.host', 'redis.port', 'redis.db', 'datastore.redis.prefix']
    }
