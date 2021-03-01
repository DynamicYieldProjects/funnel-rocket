import logging
import math
import re
from frocket.common.metrics import MetricName, EnvironmentMetricsProvider, MetricData

logger = logging.getLogger(__name__)

DEFAULT_PRICE_GB_SEC = 0.0000166667
REGION_PRICING = {
    "eu-south-1": 0.0000195172,  # Milan
    "me-south-1": 0.0000206667,  # Bahrain
    "ap-northeast-3": 0.00002153,  # Osaka
    "af-south-1": 0.0000221,  # Capetown
    "ap-east-1": 0.00002292  # Hong-kong
}
# Assume the actual run takes this amount of seconds more than what's been measured
LAMBDA_TIME_OVERHEAD = 0.008  # 8ms, a conservative value based on a few observations


class AwsLambdaMetricsProvider(EnvironmentMetricsProvider):
    def __init__(self, lambda_context):
        # See https://docs.aws.amazon.com/lambda/latest/dg/python-context.html
        assert lambda_context.__class__.__name__ == 'LambdaContext'
        self._lambda_context = lambda_context

        # ARN example: arn:aws:lambda:us-west-2:123456789012:function:my-function
        arn_parts = lambda_context.invoked_function_arn.split(':')
        region = arn_parts[3]
        if re.match(r'\w+-\w+-\d+', region):
            self._region = region
        else:
            self._region = None
            logger.warning(f"Seems like an invalid region: '{region}' in ARN: {lambda_context.invoked_function_arn}, "
                           f"not calculating cost")

    def _memory_bytes(self):
        mem_bytes = int(self._lambda_context.memory_limit_in_mb) * (1024 ** 2)
        return MetricData(MetricName.MACHINE_MEMORY_BYTES, mem_bytes)

    def _cost_dollars(self, duration=None):
        if not duration or not self._region:
            return None

        # noinspection PyBroadException
        try:
            memory_gb = self._memory_bytes().value / (1024 ** 3)
            # Lambdas are currently billed in 1ms granularity, so rounding up
            rounded_duration = duration + LAMBDA_TIME_OVERHEAD
            rounded_duration = math.ceil(rounded_duration * 1000) / 1000

            gb_second_units = rounded_duration * memory_gb
            cost_per_unit = REGION_PRICING.get(self._region, DEFAULT_PRICE_GB_SEC)
            cost = gb_second_units * cost_per_unit
            message = \
                f"Cost: original duration: {duration: .4f} sec, rounded duration: {rounded_duration:.3f}, memory: " \
                f"{memory_gb}GB, GB/second units: {gb_second_units}, unit cost for region {self._region}: " \
                f"${cost_per_unit:.10f} => total run cost is ${cost:.10f}"
            logger.debug(message)
            return MetricData(MetricName.COST_DOLLARS, cost)
        except Exception:
            logger.exception("Failed calculating cost")
            return None
