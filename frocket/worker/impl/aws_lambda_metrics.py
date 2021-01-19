import logging
import math
from frocket.common.metrics import MetricName, EnvironmentMetricsProvider, MetricData

logger = logging.getLogger(__name__)

# Assume the actual run takes this amount of seconds more than what's been measured
LAMBDA_TIME_OVERHEAD = 0.008  # 8ms, safe choice based on a few observations


class AwsLambdaMetricsProvider(EnvironmentMetricsProvider):
    def __init__(self, lambda_context):
        # See https://docs.aws.amazon.com/lambda/latest/dg/python-context.html
        assert lambda_context.__class__.__name__ == 'LambdaContext'
        self._lambda_context = lambda_context

    def _memory_bytes(self):
        mem_bytes = int(self._lambda_context.memory_limit_in_mb) * (1024 ** 2)
        return MetricData(MetricName.MACHINE_MEMORY_BYTES, mem_bytes)

    def _cost_dollars(self, duration=None):
        if not duration:
            return None
        # noinspection PyBroadException
        try:
            memory_gb = self._memory_bytes().value / (1024 ** 3)
            # Lambdas are currently billed in 1ms granularity, so rounding up
            rounded_duration = duration + LAMBDA_TIME_OVERHEAD
            rounded_duration = math.ceil(rounded_duration * 1000) / 1000

            gb_second_units = rounded_duration * memory_gb
            cost_per_unit = self._cost_per_gb_second()
            cost = gb_second_units * cost_per_unit
            logger.info(
                f"Cost: original duration: {duration: .4f} sec, rounded duration: {rounded_duration:.3f}, memory: "
                f"{memory_gb}GB, GB/second units: {gb_second_units}, unit cost: ${cost_per_unit:.10f} => "
                f"total run cost is ${cost:.6f}")
            return MetricData(MetricName.COST_DOLLARS, cost)
        except Exception:
            logger.exception("Failed calculating cost")
            return None

    # noinspection PyMethodMayBeStatic
    def _cost_per_gb_second(self) -> float:
        # TODO pricing by region - region is in self._lambda_context, within the function ARN,
        #  currently using the price in US & some other major regions (e.g. Frankfurt)
        return 0.0000166667
