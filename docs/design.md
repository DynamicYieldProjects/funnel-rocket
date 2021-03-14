# Design TBD

* **Datastore:** Holds metadata on registered datasets, status, results and metrics, of all tasks, and historical data. 
Also used in `work_queue` mode as a queue of tasks to execute, from which workers pop their tasks. 

* **Invoker:** Receives requests to run a given query over a registered datasets, via CLI or an HTTP API - which are 
both a wrapper to the  module `frocket/invoker/invoker_api.py`. After validating the existence of the dataset and the query schema, this
module creates an instance of the concrete invoker type: either based on calling serverless functions (see 
`frocket/invoker/impl/aws_lambda_invoker`) or based on a work queue in the Datastore (see `frocket/invoker/impl/work_queue_invoker.py`).
For each file to be processed, a *task request* is created and enqueued to be run by a *worker* (See below).

The invoker type to use is configurable through environment variables. You can find all configuration options and their 
default values in `frocket/common/config.py`. To override any default value, define environment variables with a 
`FROCKET_` prefix and separate words with underscores. 

For example, to define the invocation type define the environment variables `FROCKET_INVOKER=work_queue|aws_lambda`. When
using Lambdas, to define a non-default name for the worker lambda in your account use `FROCKET_INVOKER_LAMBDA_NAME=my-frocket`.

Both invoker types inherit from `async_invoker.py`, which enqueues the tasks to run without blocking, and then immediately 
proceeds to poll for tasks status & results through the Datastore. 
After a query run is done, the invoker collects all metrics returned through the tasks results and exports them.
Finally, the query results are returned to the called

The Invoker component also provides other services: registering and validating datasets (**TBD** move this util to the 
API), listing registered datasets and **TBD** past query runs

* **Worker:** There are two worker implementations matching the two invoker types: `frocket/worker/impl/aws_lambda_worker.py` 
for serverless, and `frocket/worker/impl/queue_worker.py` for a more traditional long-running process pulling work from 
a queue. For the sake of this documentation, we're also referrering to a running instance of the Lambda as a worker, albeit
a short-lived one. It may well handle multiple requests during its lifetime, depending on the frequency of queries and 
how long the Lambda service would keep this specific instance warm during inactivity. This length of time is fully subject 
to the cloud provider's logic.

Both worker implementations are merely a thin wrapper over the actual core module running tasks: `task_runner.py` 
and its helper module `part_loader.py` which actually loads Pandas DataFrames and caches downloaded files on local disk.

Task requests created by the invoker and received by workers come in two variants, configured through the environment variable 
`FROCKET_PART_SELECTION_MODE=worker|invoker`. The most straightforward mode is `invoker`: in this mode, each task request
comes with a specific filename (*part*) to load. Task invocations are always anonymous, meaning that the invoker does not
know which worker will receive a specific task and isn't able to route a task request to the 
specific worker which may have just handled a specific file. 

To enable this kind of optimization, there is the `worker` mode in 
which the invoker publishes to the datastore a list of files that need to be queries, and workers pick from that list 
independently - preferably selecting a file they already have locally cached, and if there is no such file - picking a 
part at random. It is in any case up to the invoker to decide on the appropriate mode and craft the task request 
accordingly. Worker processes do not assume a specific mode would apply to all requests.

To minimize chances of a messy and inefficient scenario where workers picking parts at random are 'stealing' parts cached 
by other workers, there is a configurable time duration after the query is invoked (200ms by default) in which *only* workers who 
wish to pick a specific part they have cached are allowed to do so. This is nicknamed the "preflight" phase. Post-preflight,
any part can be picked by any worker. Typically, workers which are cold-started would only initialize and get to the 
point of choosing their part after the preflight time is already over, so they will not have an extra delay due to it.

This is a *best-effort* optimization which does not require centrally managing which task has what files cached - which 
would be particularaly fragile when using Lambdas whose termination is fully up to the provider) . A similar
mechanism exists in Spark and similar software for achieving best-effort *data locality*. Again, no invention is claimed 
here. At the time of writing, there is additional work to be done to achieve better hit rates.

* **Engine:** The module `frocket/engine/query_engine.py` implements actually running a query over a DataFrame. It does 
not know of the whole worker & invoker setup, or that it's working on one file out of potentially hundreds. Rather, it 
receives a DataFrame that's already loaded, alongwith the configuration it needs to run. It does provide some methods 
that assist `task_runner.py` in optimizing the file loading: before files are loaded, the engine is given the query, 
analyzes it and return which columns actually need to be loaded. With the Parquet format, loading only a subset of all 
columns typically results in a big time saving.
