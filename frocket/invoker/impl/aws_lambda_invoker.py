import logging
import time
import concurrent.futures
from typing import cast
import boto3
from botocore.client import BaseClient
from botocore.config import Config
from frocket.common.serializable import Envelope
from frocket.common.tasks.base import BaseTaskRequest, BaseApiResult
from frocket.invoker.impl.async_invoker import AsyncInvoker
from frocket.common.config import config

# TODO Later: handle Lambda service throttling
# TODO Later: add actual invoke time of each Lambda (rather than when invocation of all tasks started)

logger = logging.getLogger(__name__)

DEBUG_PRINT_PAYLOADS = config.bool("invoker.lambda.debug.payload")
# While invoke_async is deprecated, it has actually shown better performance at scale - TODO re-test and tweak
LEGACY_INVOKE_ASYNC = config.bool("invoker.lambda.legacy.async")
LAMBDA_ASYNC_OK_STATUS = 202
LAMBDA_STATUS_FIELD = 'Status' if LEGACY_INVOKE_ASYNC else 'StatusCode'


def _worker_task(req: BaseTaskRequest, client: BaseClient, lambda_name: str) -> BaseApiResult:
    # noinspection PyBroadException
    try:
        result = None
        json_payload = Envelope.seal_to_json(req)
        if DEBUG_PRINT_PAYLOADS:
            logger.debug(json_payload)

        if LEGACY_INVOKE_ASYNC:
            response = client.invoke_async(FunctionName=lambda_name, InvokeArgs=json_payload)
        else:
            response = client.invoke(FunctionName=lambda_name, InvocationType='Event', Payload=json_payload)

        if response[LAMBDA_STATUS_FIELD] == LAMBDA_ASYNC_OK_STATUS:
            result = BaseApiResult(success=True, error_message=None)
        else:
            message = f"Response status differs from expected ({LAMBDA_ASYNC_OK_STATUS}): {response}"
            result = BaseApiResult(success=False, error_message=message)
    except Exception as e:
        result = BaseApiResult(success=False, error_message=f"Failed to invoke lambda function '{lambda_name}': {e}")
    return result


class AwsLambdaInvoker(AsyncInvoker):
    def _enqueue(self, requests) -> None:
        lambda_name = config.get('invoker.lambda.name')
        num_threads = config.int('invoker.lambda.threads')
        boto_config = Config(**config.aws_config_dict(service='lambda'))
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
            futures = concurrent.futures.as_completed(futures)
            executor.shutdown()

        error_message = None
        for f in futures:
            assert f.done()
            if f.cancelled():
                error_message = "Lambda invocation interrupted"
            elif f.exception():
                error_message = f"Invocation failed with error: {f.exception()}"
            else:
                result = f.result()
                if not result or type(result) is not BaseApiResult:
                    error_message = f"Invocation returned with response: {result}"
                result = cast(BaseApiResult, result)
                if not result.success:
                    error_message = result.error_message
            if error_message:
                break

        if error_message:
            logger.error(error_message)
            raise Exception(error_message)
        else:
            logger.info(f"Async invocation done in {time.time() - start_invoke_time:.3f}")
