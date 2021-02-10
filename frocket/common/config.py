import os
import logging
import sys
from typing import Dict

#
# TODO use a "real" config package? (but Config42 was too much trouble...)
#  None behavior?
#

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
    "invoker.retry.max.attempts": "3",
    "invoker.retry.failed.interval": "3",
    "invoker.retry.lost.interval": "20",  # TODO document: depends on normal performance bounds
    "worker.self.select.enabled": "true",
    "worker.reject.age": "60",
    "metrics.export.lastrun": "lastrun.parquet",  # TODO change and doc
    "metrics.export.prometheus": "true",
    "metrics.buckets.default": "0.1, 0.5, 1, 5, 25, 100, 1000",
    "metrics.buckets.seconds": "0.05, 0.1, 0.5, 1, 2, 5, 10, 15",
    "metrics.buckets.dollars": "0.01, 0.05, 0.1, 0.5, 1, 2",
    "metrics.buckets.bytes": "1048576, 16777216, 67108864, 134217728, 268435456",
    "metrics.buckets.groups": "100, 1000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 500_000_000",
    "metrics.buckets.rows":   "100, 1000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 500_000_000",
    "worker.disk.cache.size.mb": "256",  # TODO document: 0 means no caching
    "log.file": "",
    "log.level": "info",
    "log.format": "%(asctime)s %(name)s %(levelname)s %(message)s",
    "apiserver.prettyprint": "true",
    "apiserver.stream.poll.interval.ms": "10",
    "apiserver.stream.write.interval.ms": "250",
    "validation.sample.max": "10",
    "validation.sample.ratio": "0.1",
    "unregister.last.used.interval": "10",  # TODO comment out & doc - default is invoker.run.timeout * 2
    "aggregations.top.default.count": "10",
    "aggregations.top.grace.factor": "2.0",
    "aws.endpoint.url": "",
    "aws.access.key.id": "",
    "aws_secret_access_key": ""
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

    def aws_access_settings(self) -> dict:
        return {
            'endpoint_url':  self.get('aws.endpoint.url', None) or None,
            'aws_access_key_id':  self.get('aws.access.key.id', None) or None,
            'aws_secret_access_key': self.get('aws.secret.access.key', None) or None
        }

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
