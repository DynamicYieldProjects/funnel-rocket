import json
import os
from dataclasses import dataclass
from enum import auto
from typing import Optional, Union
import requests
from frocket.common.helpers.utils import timestamped_uuid
from frocket.common.serializable import AutoNamedEnum
from tests.utils.dataset_utils import new_test_dataset, DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN

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


@dataclass
class ResponseInfo:
    text: str
    json: Optional[Union[dict, list]]


class URLs:
    @staticmethod
    def register_url_info(run_async: bool = False) -> UrlInfo:
        return UrlInfo(
            url=f"{TEST_API_SERVER_URL}datasets/register?stream={str(run_async).lower()}",
            method=UrlMethod.POST)

    @staticmethod
    def unregister_url_info(ds_name: str):
        return UrlInfo(
            url=f"{TEST_API_SERVER_URL}datasets/{ds_name}/unregister",
            method=UrlMethod.POST)

    @staticmethod
    def list_datasets_url_info():
        return UrlInfo(
            url=f"{TEST_API_SERVER_URL}datasets",
            method=UrlMethod.GET)

    @staticmethod
    def run(info: UrlInfo,
            json_body: dict = None,
            expected_success: bool = True,
            expect_stream: bool = False,
            errors_are_json: bool = True,
            expected_dict: dict = None,
            stream_expected_task_count: int = None) -> ResponseInfo:
        print(f"UrlInfo: {info}, json_body: {json_body}, expected success: {expected_success}, "
              f"expected dict: {expected_dict}")
        if info.method == UrlMethod.POST:
            response = requests.post(info.url, json=json_body, stream=expect_stream)
        elif info.method == UrlMethod.GET:
            response = requests.get(info.url, stream=expect_stream)
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

        print(f"Response as text: {response_text}")
        if expected_success:
            assert response.status_code == 200
            assert type(response_json) in [dict, list]
            if type(response_json) is dict:
                assert response_json['success'] is True
                assert not response_json.get('errorMessage', None)
        else:
            assert response.status_code == 500 if not expect_stream else 200
            if errors_are_json:
                assert response_json['success'] is False
                assert response_json['errorMessage'] is not None
            else:
                assert response_text.startswith(ERROR_STRING_PREFIX)

        if expected_dict:
            for k, v in expected_dict.items():
                assert response_json[k] == v

        return ResponseInfo(response_text, response_json)


def test_register_unregister_ok():
    with new_test_dataset(4) as test_ds:
        s3path = test_ds.copy_to_s3()
        ds_name = timestamped_uuid(prefix="ds-")
        body = {
            "name": ds_name,
            "basepath": s3path,
            "group_id_column": DEFAULT_GROUP_COLUMN,
            "timestamp_column": DEFAULT_TIMESTAMP_COLUMN,
            "validation_mode": "FIRST_LAST",
            "validate_uniques": True
        }
        URLs.run(info=URLs.register_url_info(run_async=False), json_body=body)
        URLs.run(info=URLs.unregister_url_info(ds_name=ds_name), expected_dict={'datasetFound': True})
        URLs.run(info=URLs.unregister_url_info(ds_name=ds_name), expected_dict={'datasetFound': False})


def test_register_invalid_path():
    body = {
        "name": "ds-invalid-path",
        "basepath": "s3://thisisnot/apath/",
        "group_id_column": DEFAULT_GROUP_COLUMN,
        "timestamp_column": DEFAULT_TIMESTAMP_COLUMN,
        "validation_mode": "SINGLE",
        "validate_uniques": False
    }
    URLs.run(info=URLs.register_url_info(run_async=False), json_body=body,
             expected_success=False, errors_are_json=True)


def test_register_invalid_column():
    with new_test_dataset(1) as test_ds:
        s3path = test_ds.copy_to_s3()
        body = {
            "name": "ds-invalid-column",
            "basepath": s3path,
            "group_id_column": "notagroupidcolumn",
            "timestamp_column": DEFAULT_TIMESTAMP_COLUMN,
            "validation_mode": "SINGLE",
            "validate_uniques": False
        }
        URLs.run(info=URLs.register_url_info(run_async=False), json_body=body,
                 expected_success=False, errors_are_json=True)


def test_register_invalid_args():
    with new_test_dataset(1) as test_ds:
        s3path = test_ds.copy_to_s3()
        body = {
            "name": "ds-invalid-args",
            "basepath": s3path,
            "group_id_column": DEFAULT_GROUP_COLUMN,
            "timestamp_column": DEFAULT_TIMESTAMP_COLUMN,
            "validation_mode": "SINGLEX",  # Invalid
            "validate_uniques": True
        }
        URLs.run(info=URLs.register_url_info(run_async=False), json_body=body,
                 expected_success=False, errors_are_json=False)

        body = {
            "name": "ds-invalid-args",
            "basepath": s3path,
            "group_id_column": DEFAULT_GROUP_COLUMN,
            "timestamp_column": DEFAULT_TIMESTAMP_COLUMN,
            "validation_mode": "SINGLE",
            "validate_uniques": 123  # Invalid
        }
        URLs.run(info=URLs.register_url_info(run_async=False), json_body=body,
                 expected_success=False, errors_are_json=False)


def test_register_async():
    with new_test_dataset(2) as test_ds:
        s3path = test_ds.copy_to_s3()
        ds_name = timestamped_uuid(prefix="ds-")
        body = {
            "name": ds_name,
            "basepath": s3path,
            "group_id_column": DEFAULT_GROUP_COLUMN,
            "timestamp_column": DEFAULT_TIMESTAMP_COLUMN,
            "validation_mode": "FIRST_LAST",
            "validate_uniques": True
        }
        URLs.run(info=URLs.register_url_info(run_async=True), json_body=body,
                 expect_stream=True, stream_expected_task_count=2)
        URLs.run(info=URLs.unregister_url_info(ds_name=ds_name), expected_dict={'datasetFound': True})


def test_list_datasets():
    ds_list = URLs.run(info=URLs.list_datasets_url_info()).json
    assert type(ds_list) is list
    for e in ds_list:
        assert sorted(e.keys()) == LIST_DATASETS_EXPECTED_KEYS
    original_count = len(ds_list)

    num_parts = 4
    with new_test_dataset(num_parts) as test_ds:
        ds_name = timestamped_uuid('findmehere-')
        s3path = test_ds.copy_to_s3()
        body = {
            "name": ds_name,
            "basepath": s3path,
            "group_id_column": DEFAULT_GROUP_COLUMN,
            "timestamp_column": DEFAULT_TIMESTAMP_COLUMN,
            "validation_mode": "SINGLE",
            "validate_uniques": False
        }
        URLs.run(info=URLs.register_url_info(), json_body=body)

    updated_list = URLs.run(info=URLs.list_datasets_url_info()).json
    assert len(updated_list) == original_count + 1
    found = [e for e in updated_list if e['id']['name'] == ds_name]
    assert len(found) == 1
    assert found[0]['basepath'] == s3path and found[0]['totalParts'] == num_parts

    URLs.run(URLs.unregister_url_info(ds_name))
    updated_list = URLs.run(info=URLs.list_datasets_url_info()).json
    assert len(updated_list) == original_count


def test_unregister_force():
    pass#TODO


def test_dataset_details():
    pass#TODO get dataset, schema - full & short, parts


def test_query_ok():
    pass#TODO sync, async


def test_query_invalid_args():
    pass#TODO invalid dataset name, invalid schema


def test_query_missing_files():
    pass#TODO


# TODO no raw metrics (and test only expected keys are returned)
# TODO query timeout tests...
