import concurrent.futures
import logging
import time
import boto3
from botocore.client import BaseClient
from botocore.config import Config
from frocket.common.serializable import Envelope
from frocket.common.tasks.base import BaseTaskRequest
from frocket.invoker.impl.async_invoker import AsyncInvoker
from frocket.common.config import config

# TODO Later: also inject actual invoke time of this task (rather than when invocation of all job's tasks started)
# TODO handle throttling, consider response codes

config.init_logging()
logger = logging.getLogger(__name__)
DEBUG_PRINT_PAYLOADS = config.bool("invoker.lambda.debug.payload")
LEGACY_INVOKE_ASYNC = config.bool("invoker.lambda.legacy.async")


def _worker_task(req: BaseTaskRequest, client: BaseClient, lambda_name: str):
    json_payload = Envelope.seal_to_json(req)
    if DEBUG_PRINT_PAYLOADS:
        logger.debug(json_payload)
    # While invoke_async is deprecated, it has actually shown better performance at scale - TODO re-test and tweak
    if LEGACY_INVOKE_ASYNC:
        response = client.invoke_async(FunctionName=lambda_name, InvokeArgs=json_payload)
    else:
        response = client.invoke(FunctionName=lambda_name, InvocationType='Event', Payload=json_payload)
    if response['Status'] != 202:
        raise Exception("Bad response: {response}")
    return response['Status']


class AwsLambdaInvoker(AsyncInvoker):
    def _enqueue(self, requests) -> None:
        lambda_name = config.get("invoker.lambda.name")
        num_threads = int(config.get("invoker.lambda.threads"))

        # TODO allow configuration of boto debug levels. Currently, we only wants warnings
        #  e.g. connection pool pressure
        for package_prefix in ['botocore', 'boto3', 'urllib3']:
            boto3.set_stream_logger(name=package_prefix, level=logging.WARNING)

        # TODO move rest of settings to aws_config_kws
        boto_config = Config(**config.aws_config_kws(service='lambda'),
                             max_pool_connections=50, connect_timeout=3,
                             read_timeout=3, retries={'max_attempts': 2})
        client = boto3.client('lambda',
                              **config.aws_client_settings(service='lambda'),
                              config=boto_config)
        logger.debug(f"Invoking lambdas, name: {lambda_name}, no. of invocations: {len(requests)}"
                     f", no. of invoker threads: {num_threads}")
        futures = []
        start_invoke_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            for req in requests:
                futures.append(executor.submit(_worker_task, req, client, lambda_name))
            concurrent.futures.as_completed(futures)
            executor.shutdown()
        logger.info(f"Async invocation done in {time.time() - start_invoke_time:.3f}")
