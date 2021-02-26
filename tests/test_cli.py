import json
import logging
import os
import subprocess
from pathlib import Path
from typing import List, Union, cast
from frocket.cli import LOG_LINE_PREFIX
from frocket.common.config import config
from frocket.common.helpers.utils import timestamped_uuid, memoize
from frocket.invoker import invoker_api
from tests.utils.base_test_utils import temp_filename
from tests.utils.dataset_utils import new_test_dataset, DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN, \
    DEFAULT_GROUP_COUNT, TestColumn, datafile_schema
# noinspection PyUnresolvedReferences
from tests.utils.mock_s3_utils import mock_s3_env_variables
# noinspection PyUnresolvedReferences
from tests.utils.redis_fixture import init_test_redis_settings, get_test_redis_env_variables

CLI_MAIN = 'frocket.cli'


@memoize
def subprocess_env():
    full_env = {**os.environ, **get_test_redis_env_variables(), **mock_s3_env_variables()}
    print("Env variables for sub-process:", full_env)
    return full_env


def parse_cli_output(stdout_text: str) -> List[str]:
    assert type(stdout_text) is str
    log_min_failure_level = logging.WARNING
    raw_lines = stdout_text.split('\n')
    lines = []

    for raw_line in raw_lines:
        raw_line = raw_line.strip()
        if not raw_line:  # Empty line
            continue
        if raw_line.startswith(LOG_LINE_PREFIX):  # Detect log line, and assert by severity. Log lines are not returned
            log_level = raw_line.split(' ')[1]
            assert logging.getLevelName(log_level) < log_min_failure_level
        else:
            lines.append(raw_line)
    return lines


def call_cli(cmd: str,
             args: List[str] = None,
             expected_success: bool = True,
             as_single_json: bool = False,
             api_result_format: bool = False) -> Union[List[str], dict]:
    if args is None:
        args = []
    full_args = ['python', '-m', CLI_MAIN, '--notrim', '--nocolor', '--nopretty']
    if not expected_success:
        full_args += ['--loglevel', 'critical']
    full_args += [cmd, *args]

    result = subprocess.run(full_args, capture_output=True, text=True, env=subprocess_env())
    print(f'Expected success: {expected_success}, actual return code: {result.returncode}, args: {result.returncode}')
    print('stdout:', result.stdout)
    print('stderr:', result.stderr)

    assert (result.returncode == 0) == expected_success
    lines = parse_cli_output(result.stdout)
    if as_single_json:
        assert len(lines) == 1
        d = json.loads(lines[0])
        if api_result_format:
            assert d['success'] == expected_success
        return d
    else:
        return lines


def test_invalid_cmd():
    call_cli('whatthehell', expected_success=False)
    call_cli('whattheheck', args=['blah'] * 100, expected_success=False)


def test_list():
    cli_datasets = cast(dict, call_cli('list', as_single_json=True))
    expected_ds_names = sorted([ds.id.name for ds in invoker_api.list_datasets()])
    assert sorted([ds['name'] for ds in cli_datasets]) == expected_ds_names


def test_config():
    cli_config = cast(dict, call_cli('config', as_single_json=True))
    cli_config.pop('log.format')

    expected_config = dict(config)
    expected_config.pop('log.format')
    assert cli_config == expected_config


def test_register_unregister():
    with new_test_dataset(parts=2) as test_ds:
        s3path = test_ds.copy_to_s3()
        name = timestamped_uuid(prefix='testcli-')
        call_cli('register', args=[name, s3path, DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN],
                 as_single_json=True, api_result_format=True)
        call_cli('unregister', args=[name], as_single_json=True, api_result_format=True)


def test_register_fail():
    with new_test_dataset(parts=2) as test_ds:
        s3path = test_ds.copy_to_s3()
        name = timestamped_uuid(prefix='testcli-')
        # Invalid path
        call_cli('register', args=[name, s3path + '/blah', DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN],
                 expected_success=False, as_single_json=True, api_result_format=True)
        # Invalid column name
        call_cli('register', args=[name, s3path, DEFAULT_GROUP_COLUMN + '-not', DEFAULT_TIMESTAMP_COLUMN],
                 expected_success=False, as_single_json=True, api_result_format=True)
        # Invalid validation mode
        call_cli('register',
                 args=[name, s3path, DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN, '--validation', 'nosuchmode'],
                 expected_success=False,
                 as_single_json=False,
                 api_result_format=False)
        # Should work (with optional flags)
        call_cli('register', args=[name, s3path, DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN,
                                   '--validation', 'FIRST_LAST', '--skip-uniques'],
                 as_single_json=True,
                 api_result_format=True)
        call_cli('unregister', args=[name], as_single_json=True, api_result_format=True)


def test_query_modes():
    with new_test_dataset(1) as test_ds:
        s3path = test_ds.copy_to_s3()
        name = timestamped_uuid(prefix='testcli-')
        call_cli('register', args=[name, s3path, DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN],
                 as_single_json=True, api_result_format=True)

        empty_query = '{}'
        # Invalid cases
        call_cli('run', args=[], expected_success=False)
        call_cli('run', args=[name, '--empty', '--string', 'empty_query'], expected_success=False)
        call_cli('run', args=[name, '--file', 'blahblah'], expected_success=False)
        call_cli('run', args=[name, '--string', '"{{{}"'], expected_success=False)
        call_cli('run', args=["notreally...", '--empty'], expected_success=False)

        # Valid empty query in all modes
        empty_result1 = call_cli('run', args=[name, '--empty'], as_single_json=True, api_result_format=True)
        empty_result2 = call_cli('run', args=[name, '--string', empty_query],
                                 as_single_json=True, api_result_format=True)
        empty_query_file = temp_filename()
        Path(empty_query_file).write_text('{}', encoding='utf-8')
        empty_result3 = call_cli('run', args=[name, '--file', empty_query_file],
                                 as_single_json=True, api_result_format=True)
        os.remove(empty_query_file)

        for res in [empty_result1, empty_result2, empty_result3]:
            assert res['query']['matchingGroups'] == DEFAULT_GROUP_COUNT
            assert res['stats']['invoker']['totalTasks'] == 1
            assert res['query']['aggregations'] is None

        # Valid non-empty queries
        q1 = {'query': {'aggregations': [{'column': TestColumn.bool.value, 'type': 'count', 'name': 'aggr_string'}]}}
        result = call_cli('run', args=[name, '--string', json.dumps(q1)], as_single_json=True, api_result_format=True)
        assert result['query']['aggregations'][0]['name'] == 'aggr_string'

        q2 = {'query': {'aggregations': [{'column': TestColumn.bool.value, 'type': 'count', 'name': 'aggr_file'}]}}
        q2file = temp_filename()
        Path(q2file).write_text(json.dumps(q2), encoding='utf-8')
        result = call_cli('run', args=[name, '--file', q2file], as_single_json=True, api_result_format=True)
        assert result['query']['aggregations'][0]['name'] == 'aggr_file'
        os.remove(q2file)

        # Cleanup
        call_cli('unregister', args=[name, '--force'], as_single_json=True, api_result_format=True)


def test_info():
    with new_test_dataset(1) as test_ds:
        s3path = test_ds.copy_to_s3()
        name = timestamped_uuid(prefix='testcli-')
        call_cli('register', args=[name, s3path, DEFAULT_GROUP_COLUMN, DEFAULT_TIMESTAMP_COLUMN],
                 as_single_json=True, api_result_format=True)

        def get_info(full: bool = False):
            args = [name]
            if full:
                args += ['--full']
            result = call_cli('info', args=args)
            assert len(result) == 3
            basic_info = json.loads(result[0])
            parts_info = json.loads(result[1])
            schema = json.loads(result[2])
            return basic_info, parts_info, schema

        basic_info, parts_info, short_schema = get_info(full=False)
        assert basic_info['id']['name'] == name
        assert basic_info['basepath'] == s3path
        assert parts_info['filenames'] == test_ds.expected_parts.filenames
        assert short_schema == json.loads(datafile_schema().to_json())

        basic_info, parts_info, full_schema = get_info(full=True)
        assert basic_info['id']['name'] == name
        assert parts_info['filenames'] == test_ds.expected_parts.filenames
        # print(full_schema)
        group_col_data = full_schema['columns'][DEFAULT_GROUP_COLUMN]
        assert group_col_data['name'] == DEFAULT_GROUP_COLUMN
        assert group_col_data['colattrs']['numericMin'] == 0.0
        assert group_col_data['colattrs']['numericMax'] == DEFAULT_GROUP_COUNT - 1

        call_cli('unregister', args=[name], as_single_json=True, api_result_format=True)
