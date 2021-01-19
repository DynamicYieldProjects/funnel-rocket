from abc import abstractmethod
from typing import Optional
from frocket.common.config import config
from frocket.common.tasks.base import BlobId

BLOB_DEFAULT_TTL = config.int('blobstore.default.ttl')
BLOB_MAX_TTL = config.int('blobstore.max.ttl')


class Blobstore:
    @abstractmethod
    def write_blob(self, data: bytes, ttl: int = None, tag: str = None) -> BlobId:
        pass

    @abstractmethod
    def read_blob(self, blobid: BlobId) -> Optional[bytes]:
        pass

    @abstractmethod
    def delete_blob(self, blobid: BlobId) -> bool:
        pass
