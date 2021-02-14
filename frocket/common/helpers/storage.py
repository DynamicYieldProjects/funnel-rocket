import logging
import re
import tempfile
import uuid
from fnmatch import fnmatch
from pathlib import Path
from typing import NamedTuple
import boto3

from frocket.common.config import config
from frocket.common.dataset import DatasetPartsInfo, PartNamingMethod
from frocket.common.helpers.utils import memoize

logger = logging.getLogger(__name__)

# TODO refactor to use StorageProviders abstraction (local, S3, future ones).
#  Meanwhile internal stuff prefixed with '_'.

# TODO Later: support numeric pattern? (or discover it)
# TODO support pagination for S3 (for more than 1,000 files)
# TODO convert relative paths to absolute, ensure no paths in pattern

_S3_PATH_REGEX = re.compile(r"^s3://([a-zA-Z0-9_\-.]+)/([a-zA-Z0-9_\-./]*)$")


def is_remote_path(path) -> bool:
    return _is_s3_path(path)


def _is_s3_path(path) -> bool:
    return str(path).startswith("s3://")


class _S3PathInfo(NamedTuple):
    bucket: str
    path_in_bucket: str

    @classmethod
    def from_url(cls, url: str, with_separator=False):
        match = _S3_PATH_REGEX.match(url)
        if not match:
            raise Exception(f"S3 path is invalid: {url}")
        bucket = match.group(1)
        path_in_bucket = match.group(2)

        if with_separator and path_in_bucket and path_in_bucket[-1:] != '/':
            path_in_bucket += '/'

        return _S3PathInfo(bucket=bucket, path_in_bucket=path_in_bucket)


@memoize
def _s3client():
    boto3.set_stream_logger(level=logging.INFO)  # TODO allow configuration
    return boto3.client('s3', **config.aws_client_settings(service='s3'))


class _FileBaseInfo(NamedTuple):
    relpath: str
    size: int


def discover_files(basepath: str, pattern: str) -> DatasetPartsInfo:
    if _is_s3_path(basepath):
        s3path = _S3PathInfo.from_url(basepath, with_separator=True)
        logger.info(f"Listing files in S3 with bucket {s3path.bucket} and prefix {s3path.path_in_bucket}...")
        s3response = _s3client().list_objects_v2(Bucket=s3path.bucket, Prefix=s3path.path_in_bucket)

        filename_start_idx = len(s3path.path_in_bucket)
        path_to_size = {obj['Key'][filename_start_idx:]: obj['Size'] for obj in s3response['Contents']}
        files_info = [_FileBaseInfo(path, size) for path, size in path_to_size.items() if fnmatch(path, pattern)]
    else:
        paths = Path(basepath).iterdir()
        files_info = [_FileBaseInfo(path.name, path.stat().st_size) for path in paths if fnmatch(path.name, pattern)]

    files_info.sort(key=lambda fi: fi.relpath)

    parts_info = DatasetPartsInfo(naming_method=PartNamingMethod.LIST,
                                  total_parts=len(files_info),
                                  total_size=sum([fi.size for fi in files_info]),
                                  filenames=[fi.relpath for fi in files_info],
                                  running_number_pattern=None)
    return parts_info


def get_local_path(fullpath: str) -> str:
    if _is_s3_path(fullpath):
        s3path = _S3PathInfo.from_url(fullpath)
        localpath = str(Path(tempfile.gettempdir()) / str(uuid.uuid4()))
        logger.info(f"Downloading {fullpath} to {localpath}...")
        _s3client().download_file(s3path.bucket, s3path.path_in_bucket, localpath)
        return localpath
    else:
        if not Path(fullpath).is_file():
            raise Exception(f"Path is missing/not a file: {fullpath}")
        return fullpath
