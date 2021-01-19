from frocket.invoker.impl.async_invoker import AsyncInvoker


class WorkQueueInvoker(AsyncInvoker):
    def _enqueue(self, requests) -> None:
        self._datastore.enqueue(requests)
