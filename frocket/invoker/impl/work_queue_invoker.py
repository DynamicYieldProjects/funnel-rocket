"""
Invoke tasks by enqueing them in the datastore. Not much to do here :-)
"""
from frocket.invoker.impl.async_invoker import AsyncInvoker


class WorkQueueInvoker(AsyncInvoker):
    def _enqueue(self, requests) -> None:
        self._datastore.enqueue(requests)
