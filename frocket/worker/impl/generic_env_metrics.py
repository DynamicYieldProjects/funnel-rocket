"""
This is the most generic implementation for getting runtime-environment metrics:
it does not assume we know the cost of the host machine for the request duration,
and getting physical memory size should generally work on Linux variants and OS X versions.
"""
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
            # Fallback to sysctl in case that os.sysconf('SC_PHYS_PAGES') fails on OS X (seems version specific)
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
