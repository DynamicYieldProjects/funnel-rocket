import os
import pytest
from frocket.common.config import config, ConfigDict
from frocket.datastore.registered_datastores import get_datastore, get_blobstore


@pytest.fixture(scope="session", autouse=True)
def init_test_redis_settings():
    config['redis.host'] = os.environ.get('TEST_REDIS_HOST', config['redis.host'])
    config['redis.port'] = os.environ.get('TEST_REDIS_PORT', config['redis.port'])
    config['redis.db'] = os.environ.get('TEST_REDIS_DB', config['redis.db'])
    print(get_datastore(), get_blobstore())  # Fail on no connection, print connection details


def get_test_redis_env_variables():
    return {
        ConfigDict.to_env_variable(key): config.get(key)
        for key in ['redis.host', 'redis.port', 'redis.db', 'datastore.redis.prefix']
    }
