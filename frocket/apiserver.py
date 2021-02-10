import logging
import os
from pathlib import Path
from typing import Type, Callable
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

STREAM_POLL_INTERVAL = config.int("apiserver.stream.poll.interval.ms") / 1000
STREAM_WRITE_INTERVAL = config.int("apiserver.stream.write.interval.ms") / 1000
# TODO Disabling "public mode" (which exposes only specific endpoints and response attributes)
#  till there's proper test coverage. This server should not be directly internet-facing.
PUBLIC_MODE = False
PRETTY_PRINT = config.bool("apiserver.prettyprint")
EXPORT_TO_PROMETHEUS = config.bool('metrics.export.prometheus')
DEFAULT_ERROR = 'Error'

app = Flask(__name__)
if PRETTY_PRINT:
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
if EXPORT_TO_PROMETHEUS:
    if PUBLIC_MODE:
        logger.warning('Server is in public mode but metrics export to Prometheus via /metrics is enabled. '
                       'Is it advised to disallow client access to that endpoint in your production setup.')
    # Add prometheus WSGI middleware to route /metrics requests
    app.wsgi_app = DispatcherMiddleware(app=app.wsgi_app, mounts={'/metrics': make_wsgi_app()})


class ClientFacingError(Exception):
    def __init__(self, message: str, public_error: bool = False):
        self.message = message
        self.public_error = public_error

    @staticmethod
    def error_message_to_client(error, public_error=False) -> str:
        if public_error or not PUBLIC_MODE:
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
        message = ClientFacingError.error_message_to_client(error, public_error=False)
    except Exception:
        logger.exception('Error while formatting full error response')
        pass
    logger.exception(error)
    return message, 500


def get_dataset_or_fail(dataset_name: str) -> DatasetInfo:
    dataset = invoker_api.get_dataset(dataset_name, throw_if_missing=False)
    if dataset:
        return dataset
    else:
        raise ClientFacingError(message=f"Dataset {dataset_name} not found", public_error=True)


def fail_if_public() -> None:
    if PUBLIC_MODE:
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
    return make_response(jsonify(result.to_api_response_dict(PUBLIC_MODE)), status_code)


@app.route('/datasets')
def list_datasets():
    datasets = [ds.to_api_response_dict(public_fields_only=PUBLIC_MODE)
                for ds in invoker_api.list_datasets()]
    return jsonify(datasets)


@app.route('/datasets/<dataset_name>/schema')
def get_dataset_schema(dataset_name: str):
    dataset = get_dataset_or_fail(dataset_name)
    want_full_schema = bool_request_arg('full')
    schema = invoker_api.get_dataset_schema(dataset, full=want_full_schema)
    return jsonify(schema.to_api_response_dict(PUBLIC_MODE))


@app.route('/datasets/<dataset_name>/parts')
def get_dataset_parts(dataset_name: str):
    fail_if_public()
    dataset = get_dataset_or_fail(dataset_name)
    parts = invoker_api.get_dataset_parts(dataset)
    return jsonify(parts.to_api_response_dict(PUBLIC_MODE))


def bool_request_arg(name: str, default: bool = False) -> bool:
    v = request.args.get(name, None)
    if v is None:
        return default
    else:
        return v.strip().lower() == "true"


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
                              async_func=lambda: invoker_api.run_query_async(dataset, query,
                                                                             validation_result=validation_result))
    else:
        return run_streamable(should_stream=False,
                              sync_func=lambda: invoker_api.run_query(dataset, query,
                                                                      validation_result=validation_result))


# TODO move to helper module with rest of friends...
def run_streamable(sync_func: Callable[[], BaseApiResult] = None,
                   async_func: Callable[[], AsyncJobTracker] = None,
                   should_stream: bool = False) -> flask.Response:
    assert (should_stream and async_func) or \
           (not should_stream and sync_func)

    try:
        @stream_with_context
        def generate_stream(tracker):
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
                            'message': status.message if not PUBLIC_MODE else None,
                            'tasks': {k.name: v for k, v in status.task_counters.items()},
                        }
                        yield json.dumps(simple_status) + '\n'
                        logger.debug(f"Streaming status: {simple_status}")

            yield json.dumps(api_result.to_api_response_dict(PUBLIC_MODE)) + '\n'

        if not should_stream:
            job_result = sync_func()
            return make_api_response(job_result)
        else:
            # TODO IMPORTANT catch exceptions in async run, stop polling and return the error
            tracker = async_func()
            return Response(generate_stream(tracker), mimetype='application/json')

    except Exception as e:
        raise ClientFacingError(str(e), public_error=False)


@app.route('/datasets/<dataset_name>/query', methods=['POST'])
def run_query(dataset_name: str):
    query = request.json
    should_stream = bool_request_arg('stream')
    return do_run_query(dataset_name, query, should_stream)


# TODO transform to empty query when engine supports it fully
@app.route('/datasets/<dataset_name>/example_query')
def run_example_query(dataset_name: str):
    query = json.load(open(Path(os.path.dirname(__file__)) / 'resources/example_query.json', 'r'))
    should_stream = bool_request_arg('stream')
    return do_run_query(dataset_name, query, should_stream)


def do_register(register_args, should_stream):
    if should_stream:
        return run_streamable(should_stream=True,
                              async_func=lambda: invoker_api.register_dataset_async(register_args))

    else:
        return run_streamable(should_stream=False,
                              sync_func=lambda: invoker_api.register_dataset(register_args))


@app.route('/datasets/register', methods=['POST'])
def register_dataset():
    fail_if_public()
    register_args = dataclass_from_body(request, RegisterArgs)
    should_stream = bool_request_arg('stream')
    return do_register(register_args, should_stream)


@app.route('/datasets/<dataset_name>/unregister', methods=['POST'])
def unregister_dataset(dataset_name: str):
    fail_if_public()
    should_force = bool_request_arg('force')
    result = invoker_api.unregister_dataset(dataset_name, should_force)
    return make_api_response(result)
