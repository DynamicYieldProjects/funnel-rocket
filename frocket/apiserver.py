"""
HTTP API server for Funnel Rocket - manage datasets, get metadata, run queries.
For long-running operations, the server supports streaming updates with HTTP chunked encoding.
"""
import logging
from typing import Type, Callable, cast
import flask
from flask import Flask, request, jsonify, Response, stream_with_context, json, make_response
from prometheus_client.exposition import make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from frocket.common.config import config
from frocket.common.dataset import DatasetInfo
from frocket.common.serializable import SerializableDataClass
from frocket.common.tasks.registration import RegisterArgs, BaseApiResult
from frocket.common.tasks.async_tracker import AsyncJobTracker
from frocket.common.validation.error import ValidationErrorKind
from frocket.invoker import invoker_api

config.init_logging()
logger = logging.getLogger(__name__)

ALLOW_ADMIN_ACTIONS = config.bool("apiserver.admin.actions")
RETURN_ERROR_DETAILS = config.bool("apiserver.error.details")
PRETTY_PRINT = config.bool("apiserver.prettyprint")
EXPORT_TO_PROMETHEUS = config.bool('metrics.export.prometheus')
DEFAULT_ERROR = 'Error'

app = Flask(__name__)
if PRETTY_PRINT:
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

if EXPORT_TO_PROMETHEUS:
    # Add prometheus WSGI middleware to route /metrics requests
    app.wsgi_app = DispatcherMiddleware(app=app.wsgi_app, mounts={'/metrics': make_wsgi_app()})


class ClientFacingError(Exception):
    """
    For purposefully throwing errors back to client.
    'public' errors are those known not to leak any sensitive information. Other errors are supressed from response
    if 'apiserver.error.details' config attribute is False.
    """
    def __init__(self, message: str, public_error: bool = False):
        self.message = message
        self.public_error = public_error

    @staticmethod
    def error_message_to_client(error, public_error=False) -> str:
        if public_error or RETURN_ERROR_DETAILS:
            return f"Error: {error}"
        else:
            return DEFAULT_ERROR

    def __str__(self):
        return self.error_message_to_client(self.message, public_error=self.public_error)


@app.errorhandler(ClientFacingError)
def user_facing_error_handler(error: ClientFacingError):
    logger.exception(error)
    return str(error), 500


@app.errorhandler(500)
def error500_handler(error):
    message = DEFAULT_ERROR
    # noinspection PyBroadException
    try:
        message = ClientFacingError.error_message_to_client(error, public_error=False)  # Assuming non-public error
    except Exception:
        logger.exception('Error while formatting full error response')
    logger.exception(error)
    return message, 500


def get_dataset_or_fail(dataset_name: str) -> DatasetInfo:
    dataset = invoker_api.get_dataset(dataset_name, throw_if_missing=False)
    if dataset:
        return dataset
    else:
        raise ClientFacingError(message=f"Dataset {dataset_name} not found", public_error=True)


def ensure_admin_enabled() -> None:
    if not ALLOW_ADMIN_ACTIONS:
        raise ClientFacingError(message="Not supported", public_error=True)


def dataclass_from_body(request, cls: Type[SerializableDataClass]) -> SerializableDataClass:
    try:
        return cls.from_json(request.data)
    except KeyError as ke:
        raise ClientFacingError(f"{ke} missing or invalid in body", public_error=True)
    except ValueError as ve:
        raise ClientFacingError(f"{ve}", public_error=True)


def make_api_response(result: BaseApiResult) -> flask.Response:
    status_code = 200 if result.success else 500
    return make_response(jsonify(result.to_api_response_dict()), status_code)


def bool_request_arg(name: str, default: bool = False) -> bool:
    v = request.args.get(name, None)
    if v is None:
        return default
    else:
        return v.strip().lower() == "true"


def run_streamable(sync_func: Callable[[], BaseApiResult] = None,
                   async_func: Callable[[], AsyncJobTracker] = None,
                   should_stream: bool = False) -> flask.Response:
    """
    Run functions that are either synchronous (normal HTTP response, full response returned when handling ends) or
    async. (status updates are returned, then the final response, via HTTP chunked encoding). For async functions,
    the argument is an already initialized AsyncJobTracker that can be polled to completion.
    """
    assert (should_stream and async_func) or \
           (not should_stream and sync_func)

    try:
        @stream_with_context
        def generate_stream(tracker):
            # TODO backlog use the newer AsyncJobStatus.generator()
            while True:
                update_available = tracker.wait()
                if not tracker.wait_time_remaining:
                    api_result = BaseApiResult(success=False, error_message="Request timed out")
                    break

                status = tracker.status
                if status.result:
                    api_result = status.result
                    break

                if update_available:
                    if status.task_counters:
                        simple_status = {
                            'stage': status.stage.value,
                            'message': status.message,
                            'tasks': {k.name: v for k, v in status.task_counters.items()},
                        }
                        yield json.dumps(simple_status) + '\n'  # Chunks are expected to be separated by blank lines
                        logger.debug(f"Streaming status: {simple_status}")

            yield json.dumps(api_result.to_api_response_dict()) + '\n'

        if not should_stream:
            job_result = sync_func()
            return make_api_response(job_result)
        else:
            # TODO backlog test handling exceptions in async run
            tracker = async_func()
            return Response(generate_stream(tracker), mimetype='application/json')

    except Exception as e:
        raise ClientFacingError(str(e), public_error=False)


@app.route('/datasets/register', methods=['POST'])
def register_dataset():
    ensure_admin_enabled()
    register_args = cast(RegisterArgs, dataclass_from_body(request, RegisterArgs))
    should_stream = bool_request_arg('stream')
    if should_stream:
        return run_streamable(should_stream=True, async_func=lambda: invoker_api.register_dataset_async(register_args))
    else:
        return run_streamable(should_stream=False, sync_func=lambda: invoker_api.register_dataset(register_args))


@app.route('/datasets/<dataset_name>/unregister', methods=['POST'])
def unregister_dataset(dataset_name: str):
    ensure_admin_enabled()
    should_force = bool_request_arg('force')
    result = invoker_api.unregister_dataset(dataset_name, should_force)
    return make_api_response(result)


@app.route('/datasets')
def list_datasets():
    datasets = [ds.to_api_response_dict() for ds in invoker_api.list_datasets()]
    return jsonify(datasets)


@app.route('/datasets/<dataset_name>/schema')
def get_dataset_schema(dataset_name: str):
    dataset = get_dataset_or_fail(dataset_name)
    want_full_schema = bool_request_arg('full')
    schema = invoker_api.get_dataset_schema(dataset, full=want_full_schema)
    return jsonify(schema.to_api_response_dict())


@app.route('/datasets/<dataset_name>/parts')
def get_dataset_parts(dataset_name: str):
    ensure_admin_enabled()
    dataset = get_dataset_or_fail(dataset_name)
    parts = invoker_api.get_dataset_parts(dataset)
    return jsonify(parts.to_api_response_dict())


def do_run_query(dataset_name: str, query: dict, should_stream: bool = False) -> Response:
    dataset = get_dataset_or_fail(dataset_name)
    validation_result = invoker_api.expand_and_validate_query(dataset, query)
    if not validation_result.success:
        is_public_error = validation_result.error_kind != ValidationErrorKind.UNEXPECTED
        raise ClientFacingError(message=f"Query validation failed: {validation_result.error_message}",
                                public_error=is_public_error)

    query = validation_result.expanded_query
    if should_stream:
        return run_streamable(should_stream=True,
                              async_func=lambda:  invoker_api.run_query_async(dataset, query,
                                                                              validation_result=validation_result))
    else:
        return run_streamable(should_stream=False,
                              sync_func=lambda: invoker_api.run_query(dataset, query,
                                                                      validation_result=validation_result))


@app.route('/datasets/<dataset_name>/query', methods=['POST'])
def run_query(dataset_name: str):
    query = request.json
    should_stream = bool_request_arg('stream')
    return do_run_query(dataset_name, query, should_stream)


@app.route('/datasets/<dataset_name>/empty-query')
def run_empty_query(dataset_name: str):
    """Returns basic stats (group and row count, etc.) over the dataset. A GET request since no query is passed."""
    query = {}
    should_stream = bool_request_arg('stream')
    return do_run_query(dataset_name, query, should_stream)
