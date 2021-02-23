import logging
import subprocess
from typing import List

from frocket.cli import LOG_LINE_PREFIX

CLI_MAIN = 'frocket.cli'


def call_cli(cmd: str,
             args: List[str] = None,
             expected_success: bool = True,
             fail_on_log_warnings: bool = True) -> List[str]:
    if args is None:
        args = []
    full_args = ['python', '-m', CLI_MAIN, '--notrim', '--nocolor', cmd, *args]
    result = subprocess.run(full_args, capture_output=True, text=True)
    assert (result.returncode == 0) == expected_success
    raw_lines = result.stdout.split('\n')
    lines = []
    log_min_failure_level = logging.WARNING if fail_on_log_warnings else logging.ERROR
    if expected_success:
        for raw_line in raw_lines:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            if raw_line.startswith(LOG_LINE_PREFIX):
                log_level = raw_line.split(' ')[1]
                assert logging.getLevelName(log_level) < log_min_failure_level
            else:
                lines.append(raw_line)
    return lines


def test_config():
    lines = call_cli('list')
