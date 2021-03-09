"""
Simple abstraction of local & remote filesystems.

Currently supports either a local filesystem (for non-distributed usage, or potentially a fast network share)
and S3 (or S3-compatible object stores such as MinIO, which is used for running tests).
Additional protocols are welcome.

TODO backlog: support pagination for S3 listing (so more than 1,000 files per dataset)
TODO backlog: support auto-identification of numbering pattern in dataset files, so the full list of filenames
 would not have to reside in the datastore
"""
import logging
import re
import tempfile
import uuid
from abc import abstractmethod
from enum import Enum, auto
from fnmatch import fnmatch
from pathlib import Path
from typing import NamedTuple, Optional, List
import boto3
from frocket.common.config import config
from frocket.common.dataset import DatasetPartsInfo, PartNamingMethod

logger = logging.getLogger(__name__)


class StorageHandler:
    """Simple abstraction of a storage protocol."""
    class FileBaseInfo(NamedTuple):
        relpath: str
        size: int

    def __init__(self, path: str):
        assert self.valid(path)
        self._path = path

    @classmethod
    def valid(cls, path: str) -> bool:
        """For validation of a path prior to instantiating the handler - a nicety instead of exceptions later,
        to be overriden where appropriate."""
        return True

    @property
    @abstractmethod
    def remote(self) -> bool:
        """This affects the caching behavior used by workers (see part_loader.py)."""
        pass

    @abstractmethod
    def _list_files(self, pattern: str) -> List[FileBaseInfo]:
        """Override in subclasses"""
        pass

    def discover_files(self, pattern: str) -> DatasetPartsInfo:
        files = self._list_files(pattern)
        files.sort(key=lambda fi: fi.relpath)
        # TODO backlog implement PartNamingMethod.RUNNING_NUMBER for compact metadata in large datasets
        parts_info = DatasetPartsInfo(naming_method=PartNamingMethod.LIST,
                                      total_parts=len(files),
                                      total_size=sum([fi.size for fi in files]),
                                      filenames=[fi.relpath for fi in files],
                                      running_number_pattern=None)
        return parts_info

    @abstractmethod
    def _local_path(self, fullpath: str) -> str:
        """
        If the filesystem is remote, download and return a local copy.
        Files should be cleaned-up by the caller which controls the caching behavior.
        """
        pass

    def get_local_path(self, fullpath: str) -> str:
        if not fullpath.startswith(self._path):
            raise Exception(f"Given full path {fullpath} is not under handler's path {self._path}")

        return self._local_path(fullpath)


class FileStorageHanler(StorageHandler):
    """Super-simple local filesystem handler"""
    @property
    def remote(self):
        return False

    def _list_files(self, pattern):
        paths = Path(self._path).iterdir()
        files = [StorageHandler.FileBaseInfo(path.name, path.stat().st_size)
                 for path in paths
                 if fnmatch(path.name, pattern)]
        return files

    def _local_path(self, fullpath):
        if not Path(fullpath).is_file():
            raise Exception(f"Path is missing/not a file: {fullpath}")
        return fullpath


class S3StorageHanler(StorageHandler):
    """S3 filesystem handler, supports datasets directly under the bucket or within a sub-directory."""
    S3_PATH_REGEX = re.compile(r"^s3://([a-zA-Z0-9_\-.]+)/([a-zA-Z0-9_\-./]*)$")

    def __init__(self, path: str):
        super().__init__(path)
        path_parts = self.S3_PATH_REGEX.match(path)
        self._bucket = path_parts.group(1)
        self._path_in_bucket = path_parts.group(2)
        no_trailing_slash = self._path_in_bucket and self._path_in_bucket[-1:] != '/'
        self._path_in_bucket_normalized = self._path_in_bucket + ('/' if no_trailing_slash else '')

    @classmethod
    def valid(cls, path):
        return True if cls.S3_PATH_REGEX.match(path) else False

    @property
    def remote(self):
        return True

    def _list_files(self, pattern):
        path_in_bucket = self._path_in_bucket_normalized
        logger.info(f"Listing files in S3 with bucket {self._bucket} and prefix {path_in_bucket}...")
        # TODO backlog support pagination
        s3response = self._client().list_objects_v2(Bucket=self._bucket, Prefix=path_in_bucket)

        filename_start_idx = len(path_in_bucket)
        path_to_size = {obj['Key'][filename_start_idx:]: obj['Size'] for obj in s3response['Contents']}
        files = [StorageHandler.FileBaseInfo(path, size)
                 for path, size in path_to_size.items()
                 if fnmatch(path, pattern)]
        return files

    def _local_path(self, fullpath):
        localpath = str(Path(tempfile.gettempdir()) / str(uuid.uuid4()))
        logger.info(f"Downloading {fullpath} to {localpath}...")
        self._client().download_file(self._bucket, self._path_in_bucket, localpath)
        return localpath

    @classmethod
    def _client(cls):
        if not hasattr(cls, '_s3client'):
            cls._s3client = boto3.client('s3', **config.aws_client_settings(service='s3'))
        return cls._s3client


class StorageProtocol(Enum):
    FILE = auto()
    S3 = auto()

    @classmethod
    def get(cls, name: str):
        return cls.__members__.get(name.upper())

    @classmethod
    def names(cls) -> List[str]:
        return list(cls.__members__.keys())


PATH_WITH_PROTOCOL_RE = r'(\w+)://(.+)$'
PROTOCOL_TO_HANDLER = {
    StorageProtocol.FILE: FileStorageHanler,
    StorageProtocol.S3: S3StorageHanler
}


def storage_handler_for(path: str, throw_if_missing: bool = True) -> Optional[StorageHandler]:
    """
    Instantiate the appropriate handler for the given path.
    Paths without explicit protocol are considered local.
    """
    path_and_protocol = re.match(PATH_WITH_PROTOCOL_RE, path)
    if path_and_protocol:
        method_name = path_and_protocol.groups()[0]
        method = StorageProtocol.get(method_name)
        if not method:
            if throw_if_missing:
                raise Exception(f"Storage protocol '{method_name}' is not in supported list: {StorageProtocol.names()}")
            else:
                return None
        elif method == StorageProtocol.FILE:
            path = path_and_protocol.groups()[1]
    else:
        method = StorageProtocol.FILE

    handler_cls = PROTOCOL_TO_HANDLER[method]
    if not handler_cls.valid(path):
        raise Exception(f"Invalid path: {path} (protocol: {method.name})")
    return handler_cls(path)
