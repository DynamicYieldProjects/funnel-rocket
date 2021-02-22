import os
import logging
import sys
from typing import Dict
import botocore

# TODO Switch to a "real" config package, standardize behavior on none/empty values

ENV_CONFIG_PREFIX = "FROCKET"
DEFAULTS = {
    "datastore": "redis",
    "blobstore": "redis",  # TODO doc optional separate redis config of any property
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
    "part.selection.preflight.ms": "200",  # TODO document: 0 means no preflight
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
    "invoker.retry.lost.interval": "20",  # TODO document: depends on normal performance bounds
    "worker.self.select.enabled": "true",
    "worker.reject.age": "60",
    "metrics.export.lastrun": "",  # TODO change and doc
    "metrics.export.prometheus": "true",
    "metrics.buckets.default": "0.1, 0.5, 1, 5, 25, 100, 1000",
    "metrics.buckets.seconds": "0.05, 0.1, 0.5, 1, 2, 5, 10, 15",
    "metrics.buckets.dollars": "0.01, 0.05, 0.1, 0.5, 1, 2",
    "metrics.buckets.bytes": "1048576, 16777216, 67108864, 134217728, 268435456",
    "metrics.buckets.groups": "100, 1000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 500_000_000",
    "metrics.buckets.rows":   "100, 1000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 500_000_000",
    "worker.disk.cache.size.mb": "256",  # TODO document: 0 means no caching
    "stats.timing.percentiles": "0.25, 0.5, 0.75, 0.95, 0.99",
    "log.file": "",
    "log.level": "info",
    "log.format": "%(asctime)s %(name)s %(levelname)s %(message)s",
    "apiserver.prettyprint": "true",
    "apiserver.stream.poll.interval.ms": "10",
    "apiserver.stream.write.interval.ms": "250",
    "validation.sample.max": "10",
    "validation.sample.ratio": "0.1",
    "unregister.last.used.interval": "30",  # TODO comment out & doc - default is invoker.run.timeout * 2
    "aggregations.top.default.count": "10",
    "aggregations.top.grace.factor": "2.0",
    "aws.endpoint.url": "",
    "aws.access.key.id": "",
    "aws_secret_access_key": "",
    "aws.nosign": False
}


class ConfigDict(Dict[str, str]):
    def __init__(self, seq=None, **kwargs):
        super().__init__(seq, **kwargs)
        self._log_initialized = False

    # Convert environment variable such as "FROCKET_REDIS_HOST" to key "redis.host"
    def add_env_config(self, prefix: str) -> None:
        for key, value in os.environ.items():
            value = value.strip()
            if key.startswith(prefix + '_') and len(value) > 0:
                clean_key = key.replace(prefix + '_', '').replace('_', '.').lower()
                self[clean_key] = value

    def bool(self, key: str) -> bool:
        return self.get(key).strip().lower() == 'true'

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
        aws_to_config_keys = {
            'region_name': 'aws.region',
            'endpoint_url': 'aws.endpoint.url',
            'aws_access_key_id': 'aws.access.key.id',
            'aws_secret_access_key': 'aws.secret.access.key'}
        settings = {aws_key: self._get_for_service(config_key, service)
                    for aws_key, config_key in aws_to_config_keys.items()}
        return settings

    def aws_config_kws(self, service: str = None) -> dict:
        val = self._get_for_service('aws.nosign', service)
        no_sign_requests = val and val.lower() == 'true'
        config = {}
        if no_sign_requests:
            config['signature_version'] = botocore.UNSIGNED
        return config

    @property
    def loglevel(self) -> int:
        configured_level_name = self.get('log.level').upper()
        level = logging.getLevelName(configured_level_name)
        if not level:
            raise Exception(f"Uknown log level: {configured_level_name}")
        return level

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
                logging.basicConfig(stream=sys.stdout, **base_args)
            self._log_initialized = True


config = ConfigDict(DEFAULTS)
config.add_env_config(ENV_CONFIG_PREFIX)
