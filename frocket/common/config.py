"""
A simple config class based on a dict filled with default values, overridable by env. variables.

There is no support for a config file here, as my personal experience shows managing such files for different
environments tends to be clunky, while env variables are supported by pretty much everything (but YMMV).
Specifically for Funnel Rocket, the env is easily set for AWS Lambdas decoupled from the packaged code.

To override the default value of 'redis.port', set the env variable FROCKET_REDIS_PORT.
See the reference documentation for details of all settings.

TODO backlog switch to a "real" config package OR polish the API and None/empty values handling based on actual usage.
"""
import os
import logging
import sys
from typing import Dict
import boto3
import botocore

ENV_CONFIG_PREFIX = "FROCKET"
# All default values are specified as strings, just as they may be specified in env variables.
DEFAULTS = {
    "datastore": "redis",
    "blobstore": "redis",
    "redis.host": "localhost",
    "redis.port": "6379",
    "redis.db": "0",
    "datastore.redis.prefix": "frocket:",
    "blobstore.default.ttl": "600",
    "blobstore.max.ttl": "86400",
    "dataset.max.parts": "1000",
    "dataset.categorical.ratio": "0.1",
    "dataset.categorical.top.count": "20",
    "dataset.categorical.top.minpct": "0.01",
    "dataset.categorical.potential.use": "true",
    "part.selection.preflight.ms": "200",
    "invoker": "work_queue",
    "invoker.run.timeout": "60",
    "invoker.async.poll.interval.ms": "25",
    "invoker.async.log.interval.ms": "1000",
    "invoker.lambda.name": "frocket",
    "invoker.lambda.threads": "20",
    "invoker.lambda.debug.payload": "false",
    "invoker.lambda.legacy.async": "true",
    "invoker.retry.max.attempts": "3",
    "invoker.retry.failed.interval": "3",
    "invoker.retry.lost.interval": "20",
    "worker.self.select.enabled": "true",
    "worker.reject.age": "60",
    "metrics.export.prometheus": "true",
    "metrics.export.lastrun": "",
    "metrics.buckets.default": "0.1, 0.5, 1, 5, 25, 100, 1000",
    "metrics.buckets.seconds": "0.05, 0.1, 0.5, 1, 2, 5, 10, 15",
    "metrics.buckets.dollars": "0.01, 0.05, 0.1, 0.5, 1, 2",
    "metrics.buckets.bytes": "1048576, 16777216, 67108864, 134217728, 268435456",
    "metrics.buckets.groups": "100, 1000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 500_000_000",
    "metrics.buckets.rows":   "100, 1000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 500_000_000",
    "worker.disk.cache.size.mb": "256",
    "stats.timing.percentiles": "0.25, 0.5, 0.75, 0.95, 0.99",
    "log.file": "",
    "log.level": "info",
    "log.format": "%(asctime)s %(name)s %(levelname)s %(message)s",
    "apiserver.admin.actions": "true",
    "apiserver.error.details": "true",
    "apiserver.prettyprint": "true",
    "validation.sample.max": "10",
    "validation.sample.ratio": "0.1",
    "unregister.last.used.interval": "30",
    "aggregations.top.default.count": "10",
    "aggregations.top.grace.factor": "2.0",
    "aws.endpoint.url": "",
    "aws.access.key.id": "",
    "aws_secret_access_key": "",
    "aws.no.signature": "false",
    "lambda.aws.max.pool.connections": "50",
    "lambda.aws.connect.timeout": "3",
    "lambda.aws.read.timeout": "3",
    "lambda.aws.retries.max.attempts": "3"
}


class ConfigDict(Dict[str, str]):
    def __init__(self, seq=None, **kwargs):
        super().__init__(seq, **kwargs)
        self._log_initialized = False

    def add_env_config(self, prefix: str) -> None:
        """Convert environment variable such as "FROCKET_REDIS_HOST" to key "redis.host"."""
        for key, value in os.environ.items():
            value = value.strip()
            if key.startswith(prefix + '_') and len(value) > 0:
                clean_key = key.replace(prefix + '_', '').replace('_', '.').lower()
                self[clean_key] = value

    def _value_is_true(self, v: str = None) -> bool:
        return v and v.strip().lower() == 'true'

    def bool(self, key: str) -> bool:
        return self._value_is_true(self.get(key))

    def int(self, key) -> int:
        return int(self.get(key))

    def float(self, key) -> int:
        return float(self.get(key))

    def get_with_fallbacks(self, key: str, fallback_key: str, default: str = None) -> str:
        if key in self:
            return self[key]
        else:
            return self.get(fallback_key, default)

    def _get_for_service(self, key: str, service: str = None) -> str:
        if service:
            value = self.get_with_fallbacks(f"{service}.{key}", key, None)
        else:
            value = self.get(key, None)
        return value or None

    def aws_client_settings(self, service: str = None) -> dict:
        """
        Get the basic arguments for boto3.client(), to be unpacked into arguments by **aws_client_settings([service]).

        If service argument (e.g. 's3', 'lambda') is given, try first to read a value specific to that service,
        then fallback to reading a non service-specific value. If that one does not exist, boto would use its default
        mechanism of looking for env variables and configuration files.

        Having per-service settings if useful for (a) mocking the different services by separate tools (== endpoints),
        and (b) if using access keys rather than IAM roles, specifying service-specific keys.
        """
        aws_to_config_keys = {
            'region_name': 'aws.region',
            'endpoint_url': 'aws.endpoint.url',
            'aws_access_key_id': 'aws.access.key.id',
            'aws_secret_access_key': 'aws.secret.access.key'}
        settings = {aws_key: self._get_for_service(config_key, service)
                    for aws_key, config_key in aws_to_config_keys.items()}
        return settings

    def aws_config_dict(self, service: str = None) -> dict:
        """
        Returns values for initializing a boto Config object, to be unpacked by **aws_config_dict([service]).
        This is similar to the above method, except that these are values to init the optional config object with.
        """
        max_pool_connections = self._get_for_service('aws.max.pool.connections', service) or None
        connect_timeout = self._get_for_service('aws.connect.timeout', service) or None
        read_timeout = self._get_for_service('aws.read.timeout', service) or None
        max_retries = self._get_for_service('aws.retries.max.attempts', service) or None
        # For mocking purposes only!
        no_sign_requests = self._value_is_true(self._get_for_service('aws.no.signature', service))

        config = {
            'max_pool_connections': int(max_pool_connections) if max_pool_connections else None,
            'connect_timeout': int(connect_timeout) if connect_timeout else None,
            'read_timeout': int(read_timeout) if read_timeout else None,
            'retries': {'max_attempts': int(max_retries)} if max_retries else None,
            'signature_version': botocore.UNSIGNED if no_sign_requests else None
        }
        config = {k: v for k, v in config.items() if v is not None}
        return config

    @staticmethod
    def to_env_variable(key: str) -> str:
        """A helper for tests - enabling passing env variables to a sub-process with the current config value."""
        return ENV_CONFIG_PREFIX + '_' + key.replace('.', '_').upper()

    @property
    def loglevel(self) -> int:
        configured_level_name = self.get('log.level').upper()
        level = logging.getLevelName(configured_level_name)
        if not level:
            raise Exception(f"Uknown log level: {configured_level_name}")
        return level

    def _quiet_boto_logging(self):
        """botocore/boto3 can get pretty chatty, so only report warnings (e.g. connection pool pressure) and errors."""
        for package_prefix in ['botocore', 'boto3', 'urllib3']:
            boto3.set_stream_logger(name=package_prefix, level=logging.WARNING)

    def init_logging(self,
                     force_level: int = None,
                     force_console_output: bool = False) -> None:
        if not self._log_initialized:
            use_level = force_level or self.loglevel
            use_filename = None
            if not force_console_output:
                configured_filename = self.get('log.file')
                if configured_filename and len(configured_filename) > 0:
                    use_filename = configured_filename
            base_args = {
                'level': use_level,
                'format': self.get('log.format')
            }
            if use_filename:
                print(f"Logging to file {use_filename}")
                logging.basicConfig(filename=use_filename, **base_args)
            else:
                logging.basicConfig(stream=sys.stdout, **base_args)  # Please don't all go to stderr...

            self._quiet_boto_logging()
            self._log_initialized = True

    def init_lambda_logging(self):
        """
        Lambda Python runtime already init'ed a logger, so using basicConfig() as above would have no effect,
        and our precious log lines would have gone into /dev/oblivion. See https://stackoverflow.com/a/56579088
        """
        if not self._log_initialized:
            logging.getLogger().setLevel(self.loglevel)
            self._quiet_boto_logging()
            self._log_initialized = True


config = ConfigDict(DEFAULTS)
config.add_env_config(ENV_CONFIG_PREFIX)
