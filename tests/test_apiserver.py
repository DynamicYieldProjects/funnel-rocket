import json
import os
import time
from dataclasses import dataclass
from enum import auto
from typing import Optional, Union
import pytest
import requests
from frocket.common.helpers.utils import timestamped_uuid
from frocket.common.serializable import AutoNamedEnum
from tests.utils.dataset_utils import new_test_dataset, DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN, TIME_SHIFT, \
    BASE_TIME, TestColumn, BASE_USER_ID, DEFAULT_GROUP_COUNT, datafile_schema

# TODO as part of automated failure/retry tests: test query timeout
from tests.utils.mock_s3_utils import SKIP_S3_TESTS

TEST_API_SERVER_URL = os.environ.get('TEST_API_SERVER_URL', 'http://127.0.0.1:5000/')
if not TEST_API_SERVER_URL.endswith('/'):
    TEST_API_SERVER_URL += '/'
ERROR_STRING_PREFIX = 'Error: '
LIST_DATASETS_EXPECTED_KEYS = sorted(['basepath', 'groupIdColumn', 'id', 'timestampColumn', 'totalParts'])
STREAM_STATUS_EXPECTES_KEYS = ['message', 'stage', 'tasks']


class UrlMethod(AutoNamedEnum):
    GET = auto()
    POST = auto()


@dataclass
class UrlInfo:
    url: str
    method: UrlMethod
    returns_success_key: bool = True


@dataclass
class ResponseInfo:
    text: str
    json: Optional[Union[dict, list]]


class URLs:
    @staticmethod
    def register_url(run_async: bool = False) -> UrlInfo:
        return UrlInfo(
            url=f"{TEST_API_SERVER_URL}datasets/register?stream={str(run_async).lower()}",
            method=UrlMethod.POST)

    @staticmethod
    def unregister_url(ds_name: str, force: bool = False) -> UrlInfo:
        return UrlInfo(
            url=f"{TEST_API_SERVER_URL}datasets/{ds_name}/unregister?force={str(force).lower()}",
            method=UrlMethod.POST)

    @staticmethod
    def list_datasets_url() -> UrlInfo:
        return UrlInfo(url=f"{TEST_API_SERVER_URL}datasets", method=UrlMethod.GET,
                       returns_success_key=False)

    @staticmethod
    def dataset_parts_url(ds_name: str) -> UrlInfo:
        return UrlInfo(url=f"{TEST_API_SERVER_URL}datasets/{ds_name}/parts", method=UrlMethod.GET,
                       returns_success_key=False)

    @staticmethod
    def dataset_schema_url(ds_name: str, full: bool = True) -> UrlInfo:
        return UrlInfo(
            url=f"{TEST_API_SERVER_URL}datasets/{ds_name}/schema?full={str(full).lower()}",
            method=UrlMethod.GET, returns_success_key=False)

    @staticmethod
    def query_url(ds_name: str, run_async: bool = False) -> UrlInfo:
        return UrlInfo(
            url=f"{TEST_API_SERVER_URL}datasets/{ds_name}/query?stream={str(run_async).lower()}",
            method=UrlMethod.POST)

    @staticmethod
    def run(url_info: UrlInfo,
            json_body: dict = None,
            expected_success: bool = True,
            expect_stream: bool = False,
            errors_are_json: bool = True,
            expected_dict: dict = None,
            stream_expected_task_count: int = None) -> ResponseInfo:
        # print(f"UrlInfo: {url_info}, json_body: {json_body}, expected success: {expected_success}, "
        #       f"expected dict: {expected_dict}")
        if url_info.method == UrlMethod.POST:
            response = requests.post(url_info.url, json=json_body, stream=expect_stream)
        elif url_info.method == UrlMethod.GET:
            response = requests.get(url_info.url, stream=expect_stream)
        else:
            raise Exception('Uknown method!')

        if expect_stream:
            lines = [line.decode() for line in response.iter_lines() if line]
            if len(lines) > 1:
                for status_line in lines[:-1]:
                    status = json.loads(status_line)
                    assert sorted(status.keys()) == STREAM_STATUS_EXPECTES_KEYS
                    if stream_expected_task_count and status['tasks']:
                        total_tasks = sum(status['tasks'].values())
                        assert total_tasks == stream_expected_task_count
            response_text = lines[-1]
        else:
            response_text = response.text

        if expected_success or errors_are_json:
            response_json = json.loads(response_text)
        else:
            response_json = None

        # print(f"Response as text: {response_text}")
        if expected_success:
            assert response.status_code == 200
            assert type(response_json) in [dict, list]
            if url_info.returns_success_key:
                assert response_json['success'] is True
                assert not response_json.get('errorMessage', None)
        else:
            print(f"Response as text: {response_text}")
            assert response.status_code == 500 if not expect_stream else 200
            if errors_are_json:
                if url_info.returns_success_key:
                    assert response_json['success'] is False
                    assert response_json['errorMessage'] is not None
            else:
                assert response_text.startswith(ERROR_STRING_PREFIX)

        if expected_dict:
            for k, v in expected_dict.items():
                assert response_json[k] == v

        return ResponseInfo(response_text, response_json)


def build_register_args(basepath: str, ds_name: str = None):
    ds_name = ds_name or timestamped_uuid(prefix="ds-")
    return {
        "name": ds_name,
        "basepath": basepath,
        "group_id_column": DEFAULT_GROUP_COLUMN,
        "timestamp_column": DEFAULT_TIMESTAMP_COLUMN,
        "validation_mode": "FIRST_LAST",
        "validate_uniques": True
    }


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_register_unregister_ok():
    with new_test_dataset(4) as test_ds:
        args = build_register_args(basepath=test_ds.copy_to_s3())
        ds_name = args['name']
        URLs.run(url_info=URLs.register_url(run_async=False), json_body=args)
        URLs.run(url_info=URLs.unregister_url(ds_name=ds_name), expected_dict={'datasetFound': True})
        URLs.run(url_info=URLs.unregister_url(ds_name=ds_name), expected_dict={'datasetFound': False})


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_register_invalid_path():
    args = build_register_args(basepath="s3://thisisnot/apath/")
    URLs.run(url_info=URLs.register_url(run_async=False), json_body=args,
             expected_success=False, errors_are_json=True)


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_register_invalid_column():
    with new_test_dataset(1) as test_ds:
        args = build_register_args(basepath=test_ds.copy_to_s3())
        args['group_id_column'] = 'notagroupidcolumn'
        URLs.run(url_info=URLs.register_url(run_async=False), json_body=args,
                 expected_success=False, errors_are_json=True)


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_register_invalid_args():
    with new_test_dataset(1) as test_ds:
        s3path = test_ds.copy_to_s3()
        args = build_register_args(s3path)
        args['validation_mode'] = 'SINGLEX'  # Invalid
        URLs.run(url_info=URLs.register_url(run_async=False), json_body=args,
                 expected_success=False, errors_are_json=False)

        args = build_register_args(s3path)
        args['validate_uniques'] = '123'  # Invalid
        URLs.run(url_info=URLs.register_url(run_async=False), json_body=args,
                 expected_success=False, errors_are_json=False)


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_register_async():
    with new_test_dataset(2) as test_ds:
        args = build_register_args(test_ds.copy_to_s3())
        URLs.run(url_info=URLs.register_url(run_async=True), json_body=args,
                 expect_stream=True, stream_expected_task_count=2)
        URLs.run(url_info=URLs.unregister_url(ds_name=args['name']), expected_dict={'datasetFound': True})


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_list_datasets():
    ds_list = URLs.run(url_info=URLs.list_datasets_url()).json
    assert type(ds_list) is list
    for e in ds_list:
        assert sorted(e.keys()) == LIST_DATASETS_EXPECTED_KEYS
    original_count = len(ds_list)

    num_parts = 4
    with new_test_dataset(num_parts) as test_ds:
        args = build_register_args(test_ds.copy_to_s3())
        ds_name = args['name']
        URLs.run(url_info=URLs.register_url(), json_body=args)

    updated_list = URLs.run(url_info=URLs.list_datasets_url()).json
    assert len(updated_list) == original_count + 1
    found = [e for e in updated_list if e['id']['name'] == ds_name]
    assert len(found) == 1
    assert found[0]['basepath'] == args['basepath'] and found[0]['totalParts'] == num_parts

    URLs.run(URLs.unregister_url(ds_name))
    updated_list = URLs.run(url_info=URLs.list_datasets_url()).json
    assert len(updated_list) == original_count


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_unregister_force():
    with new_test_dataset(1) as test_ds:
        args = build_register_args(test_ds.copy_to_s3())
        ds_name = args['name']
        URLs.run(url_info=URLs.register_url(), json_body=args)
        curr_second = int(time.time())
        URLs.run(url_info=URLs.query_url(ds_name), json_body={})

        response = URLs.run(url_info=URLs.unregister_url(ds_name=args['name'], force=False),
                            expected_success=False, errors_are_json=True)
        assert curr_second <= response.json['datasetLastUsed'] < curr_second + 10
        assert 'less than safety interval' in response.json['errorMessage']

        URLs.run(url_info=URLs.unregister_url(ds_name=args['name'], force=True),
                 expected_success=True, expected_dict={'datasetFound': True})
        URLs.run(url_info=URLs.unregister_url(ds_name=args['name'], force=False),
                 expected_success=True, expected_dict={'datasetFound': False})


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_query_ok():
    num_parts = 3
    with new_test_dataset(num_parts) as test_ds:
        args = build_register_args(test_ds.copy_to_s3())
        ds_name = args['name']
        URLs.run(url_info=URLs.register_url(), json_body=args)

        query = {
            'query': {
                'conditions': [
                    # Should exclude exactly one user (one with lowest ID)
                    {'filter': [DEFAULT_GROUP_COLUMN, '>', BASE_USER_ID]}
                ]
            },
            'funnel': {
                'sequence': [
                    # Should include exactly one user (one with higehst ID)
                    {'filter': [DEFAULT_TIMESTAMP_COLUMN, '>=', BASE_TIME + (TIME_SHIFT * num_parts)]}
                ],
                "endAggregations": [
                    {'column': TestColumn.bool, 'type': 'countPerValue'}
                ]
            }
        }
        response = URLs.run(url_info=URLs.query_url(ds_name), json_body=query)
        assert response.json['query']['matchingGroups'] == (DEFAULT_GROUP_COUNT * 2) - 1
        assert response.json['funnel']['sequence'][0]['matchingGroups'] == 1
        assert sum(response.json['funnel']['endAggregations'][0]['value'].values()) == \
               response.json['funnel']['sequence'][0]['matchingGroupRows']

        URLs.run(url_info=URLs.unregister_url(ds_name=args['name'], force=True),
                 expected_success=True, expected_dict={'datasetFound': True})


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_query_invalid_args():
    """Errors that should be raise already on invoker side, and result in 'Error: ...' string response"""
    with new_test_dataset(1) as test_ds:
        args = build_register_args(test_ds.copy_to_s3())
        ds_name = args['name']
        URLs.run(url_info=URLs.register_url(), json_body=args)

        response = URLs.run(url_info=URLs.query_url(ds_name), json_body={'blah': 123},
                            expected_success=False, errors_are_json=False)
        assert "'blah' was unexpected" in response.text

        invalid_ds_name = ds_name + 'blah!!!'
        response = URLs.run(url_info=URLs.query_url(invalid_ds_name), json_body={},
                            expected_success=False, errors_are_json=False)
        assert f"Dataset {invalid_ds_name} not found" in response.text

        URLs.run(url_info=URLs.unregister_url(ds_name, force=True))


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_query_missing_files():
    """Missing files (after registration) should cause workers to fail"""
    with new_test_dataset(2) as test_ds:
        args = build_register_args(test_ds.copy_to_s3())
        args['validation_mode'] = 'SINGLE'
        ds_name = args['name']

        # Query works, initially
        URLs.run(url_info=URLs.register_url(), json_body=args)
        URLs.run(url_info=URLs.query_url(ds_name), json_body={})

        # Now re-register; files already cached should be disregarded,
        # and register itself is set to only validate a SINGLE file (that would be cached).
        # Meaning, other parts should not be locally cached, and loading from source should now fail
        URLs.run(url_info=URLs.register_url(), json_body=args)
        for key in test_ds.bucket.objects.all():
            key.delete()

        # Expected to fail "as JSON" (with a status and errorMessage), as it's errors from workers
        response = URLs.run(url_info=URLs.query_url(ds_name), json_body={},
                            expected_success=False, errors_are_json=True)

        URLs.run(url_info=URLs.unregister_url(ds_name, force=True))


@pytest.mark.skipif(SKIP_S3_TESTS, reason="Skipping mock S3 tests")
def test_dataset_details():
    """A non-exhaustive test of fetching dataset parts and schema info (this was tested in detail elsewhere)"""
    with new_test_dataset(2) as test_ds:
        args = build_register_args(test_ds.copy_to_s3())
        ds_name = args['name']
        URLs.run(url_info=URLs.register_url(), json_body=args)

        ds_parts = URLs.run(url_info=URLs.dataset_parts_url(ds_name)).json
        assert ds_parts['filenames'] == test_ds.expected_parts.filenames
        expected_columns = json.loads(datafile_schema().to_json())['columns']

        ds_short_schema = URLs.run(url_info=URLs.dataset_schema_url(ds_name, full=False)).json
        assert ds_short_schema['columns'] == expected_columns

        ds_full_schema = URLs.run(url_info=URLs.dataset_schema_url(ds_name, full=True)).json
        assert ds_full_schema['columns'][DEFAULT_TIMESTAMP_COLUMN]['colattrs']['numericMin'] == BASE_TIME

        URLs.run(url_info=URLs.unregister_url(ds_name))
