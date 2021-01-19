import logging
import os
from frocket.common.metrics import EnvironmentMetricsProvider, MetricData, MetricName

logger = logging.getLogger(__name__)


class GenericEnvMetricsProvider(EnvironmentMetricsProvider):
    def _memory_bytes(self):
        # Tested on Linux and OS X
        try:
            mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
        except ValueError:
            # Fallback to sysctl in case that os.sysconf('SC_PHYS_PAGES') fails on OS X
            # noinspection PyBroadException
            try:
                stream = os.popen('sysctl hw.memsize')
                mem_bytes = int(stream.read().split(' ')[1])
            except Exception as e:
                logger.warning(f"Can't detect machine memory: {e}")
                return None

        return MetricData(MetricName.MACHINE_MEMORY_BYTES, mem_bytes)

    def _cost_dollars(self, duration=None):
        return None
