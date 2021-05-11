#  Copyright 2021 The Funnel Rocket Maintainers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from abc import abstractmethod
from typing import Optional
from frocket.common.config import config
from frocket.common.tasks.base import BlobId

BLOB_DEFAULT_TTL = config.int('blobstore.default.ttl')
BLOB_MAX_TTL = config.int('blobstore.max.ttl')


class Blobstore:
    """Simple interface for storing and fetching arbitrary binary data, for ephemeral transport over the network.
    The data is assumed to always have a default TTL - it's not a permanent or big data store."""
    @abstractmethod
    def write_blob(self, data: bytes, ttl: int = None, tag: str = None) -> BlobId:
        pass

    @abstractmethod
    def read_blob(self, blobid: BlobId) -> Optional[bytes]:
        pass

    @abstractmethod
    def delete_blob(self, blobid: BlobId) -> bool:
        pass
