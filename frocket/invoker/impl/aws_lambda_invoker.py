"""
Invoke tasks by invoking an AWS Lambda function asynchronously.

This is a great feature of Lamdba, which implictly manages a queue of invocation for you with configurable retention
(probably based on SQS). As long as the concurrent invocations limit in your account/burst limit of the AWS region are
not reached, AWS will launch Lambdas for queued invocation immediately, with no meaningful delay. This also prevents
getting rate-limited on momentary invocation spikes.

A few important notes:

1. As noted in the setup guide, the retry count for the Lambda function *should be set to zero*, as it's the invoker's
job to launch retries with slightly different arguments, based on its own configuration, with logic that is agnostic
to whether the actual invoker is using Lambdas or anything else (which does not have its optional retry feature).

2. Unfortunately, there's no API for batch Lambda invocation, so we're invoking one by one with multiple threads -
and still the time to invoke all tasks can add up to 1-2 seconds or more.
TODO backlog optimize! this also hurts caching as not all tasks get their fair chance to pick a locally cached part.

3. The InvokeAsync() Lambda API is considered deprecated and replaced by the 'InvocationType' parameter in Invoke().
However, the InvokeAsync API currently seems to take about half the time to return! Which one to use is configurable.

TODO backlog stress-test queue limits till reaching rate limiting (status 429).
TODO backlog for each invocation, add its actual invoke time as parameter
 (now we only measure time since invocation of all tasks started)
"""
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

logger = logging.getLogger(__name__)

DEBUG_PRINT_PAYLOADS = config.bool("invoker.lambda.debug.payload")
LEGACY_INVOKE_ASYNC = config.bool("invoker.lambda.legacy.async")  # See notes at top of module
LAMBDA_ASYNC_OK_STATUS = 202
LAMBDA_STATUS_FIELD = 'Status' if LEGACY_INVOKE_ASYNC else 'StatusCode'


def _worker_task(req: BaseTaskRequest, client: BaseClient, lambda_name: str) -> BaseApiResult:
    """Run by the thread pool below."""
    # noinspection PyBroadException
    try:
        result = None
        json_payload = Envelope.seal_to_json(req)  # Encodes the actual object and its type, for correct decoding later.
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
        # TODO backlog consider lifecycle of the thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            for req in requests:
                futures.append(executor.submit(_worker_task, req, client, lambda_name))
            futures = concurrent.futures.as_completed(futures)  # Wait till all complete!
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
