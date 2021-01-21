# Funnel Rocket 🚀 (Provisional Documentation, Break to Parts)

<p align="left">
  <a href="#">
    <img src="https://github.com/DynamicYieldProjects/funnel-rocket-oss/workflows/Tests/badge.svg" />
  </a>
</p>

## What's This About?

Funnel Rocket is a cloud-native specialized query engine with flexible deployment options and minimal overhead, 
geared for filling a need not being served nowadays by either SQL and No SQL offerings: based on large datasets of
user activity with millions of users, being able to find which users have each completed a specific set of activities -
optionally with a specific order and time constraints.

Following the filtering stage it returns the number of matching users, plus any other requested aggregations. 
It can also perform a full funnel analysis, in which results are provided for each funnel step: how many users have 
reached that step?

This is not a rare need for companies with analytics needs, yet is a challenge with standard tools, in terms of compute
needs, time to run and cost. The queries needed for accomplishing this are relatively complex and prone to logical 
errors. The challenge here lies in performing a very high cardinality grouping first (with potentially dozens of 
millions of groups, each one being for a single user), and then running multiple passes over each group 
to execute all filters in the desired sequence.

[Link to the original document](https://docs.google.com/document/d/1k9OI4eS5AW61XtK2ImHBm5DmSmcPlx_ZdajF9WAs_Ws/)

## The Technology

There is no magic trick. Rather, the engine is built on a binding together of a few proven elements:

### 1. Serverless
(currently implemented for AWS Lambda; other providers can be integrated): 

Invoking serverless functions is perhaps the easiest way to get hundreds or thousands of processing cores available 
almost immediately, have them each perform an assigned work and terminate. While the price per CPU/second or 
GB RAM/second is higher than with either traditional VMs or even managed containers, serverless can be very cost 
effective if your usage pattern is characterized by short, bursty on-demand spikes. You pay only for the duration of 
actual work done, and have almost no management overhead. There is another mode of running which is more suitable for
large-scale batch processing of "pre-baked" queries - see below [TBD link].

Serverless offerings by the major cloud providers cannot and are not meant to provide very low latency (as in tens
of milliseconds or less), let alone a very consistent latency with minimal deviation. For the kind of ad-hoc 
analytical queries this engine excels at, performance is crucial - yet is measured in seconds rather than milliseconds. 
Queries generally return in a single-digit number of seconds when Lambdas are "cold started", and gain a significant
speedup when subsequent queries are run over the same "warm" dataset in a short time window.

At Dynamic Yield, we're typically seeing end users progressively tweak the same query over and over as they attempt to 
better understanduser behavior. On such subsequent queries, performance may drop from 4-7 seconds "cold" to about 1.5-3 
seconds in total. At the same type, the power-user nature of such queries also means the number of concurrent users
interacting with this feature is much lower than the amount of users doing a more day-to-day operational type of work 
with the system, which makes short-TTL caching more effective.

By using the asynchronous invocation mode of AWS Lambdas, the query invoker component gets two benefits: (a) it does 
need to create and wait on hundreds of simultaneous, potentially long-running HTTPS requests, and (b) AWS puts async.
requests into an internal queue, which is normally evicted really fast (so no noticeable performance penalty) - but does
provide extra cushion in case operational limits are reached - either the fixed "burst limit" (which differs by region) 
or the limit on total concurrent invocations (which can be significantly increased on request).

To realize the potential of serverless, however, we needed a framework for data processing that's not just powerful but
is fast to start and light to run: 

### 2. Pandas and Apache Arrow
Pandas and its core abstraction *the DataFrame* needs no introduction to the many 
engineers and data scientists working with it daily, in the Python Machine Learning ecosystem or through PySpark. 
It allows Funnel Rocket to run complex queries and aggregations without re-inventing the wheel and with good 
performance. Often there is more than one way to achieve your goal with Pandas, with dramatic variations in 
performance between them. This leaves a lot of room for progressive optimizations. One major rule for for performace is
avoiding any materialization of the underlying promitive vectors into Python objects. This is also a crucial element in
how the DataFrame abstraction was adopted in Spark in lieu of RDD's. Apache Arrow project is used here for its excellent
and fast Parquet driver.

### 3. Redis - and the Two Modes of Deployment
The versatility of Redis' data structures and its "just works" nature makes it a natural choice for 
handling lightweight metadata, work queues, real-time status tracking and more. There is a range of managed Redis 
offerings by both cloud providers and other vendors. In this use case, only modest amount of RAM and IO are needed, and
thus using a managed solution does not carry a high price tag. 

Other than for managing metadata, Redis is used by Funnel Rocket in two ways:

1. First, for tracking and storing the status and outputs of all individual tasks, since 
Funnel Rocket does not rely on synchronous invocation of tasks,

2. Optionally, to **support a non-serverless deployment option where Redis also acts as a work queue** from which 
long-running worker processes fetch tasks. 

This latter option is a good match not only for running locally but also for using with containers at scale, 
since it allows scaling capacity up and down smoothly. Each worker is a simple single-thread process anonymously 
fetching work from a shared queue, with no additional cluster management or load balancing required. This mode is 
suitable for running batch jobs, such as a scheduled query run to return the "default" results which an end-user would 
see in the dashaboard when they login. If you're running such a 'default' query for many customers and caching its 
results, it makes sense to allocate a whole bunch of dedicated workers to process these and scale to zero when.

Both options are similar in delegating complexity to managed and by-now-thankfully-mature orchestrators. The two options 
also similarly depend on Redis as their single stateful component which should reliably stay up. Thus, running a muti-AZ 
Redis setup is recommended.

## Preparing Data for Querying

To use your data with Funnel Rocket, ensure it fulfills the following requirements:

### Files and file names

1. Data files should be in Parquet format. **TBD support more?**

2. All files should reside under the same base path, either locally mounted or in S3.

3. Maximum of 1,000 files per dataset. **TBD: guidance on file size limit, e.g. 256mb?** (depends on usage)

4. [TBD - Subject to be more flexible, then move this to details of how to register files rather than basic reqs...] 
All file names in a dataset should follow some naming convention which includes the *part number*. For example, when 
data is partitioned into multiple files by Hadoop M/R or Apache Spark, files are generally saved with a file name pattern
similar to `part-00000.parquet`, `part-00001.parquet`, ...`part-00255.parquet` and so on. In such a case, when 
registering a dataset the client would define the pattern as `part-{:05}.parquet` (patterns are in Python string 
formatting standard). If no padding is needed, a simpler pattern such as `part-{}.parquet` suffices.

### File contents

5. Funnel Rocket is purpose-built to group rows by user ID or any similar group ID, with optional time constraints. Thus,
all files in the dataset **must have a column for the group ID and a column for the timestamp**, with no null values. 
The names for these columns is completely up to the client, at the dataset level.

5.1 The **Group ID** field may be either numeric or a string.

5.2 The **timestamp** field must be numeric, in the common _Unix time format_ (seconds since midnight UTC on 1 January 1970). 
The timestamp may be an integer (signifying seconds) or a float where the digits after the decimal point hold an 
arbitrary precision such as milliseconds - this depends on the granularity of your data.

6. **Partitioning by the Group ID column**: this is most critical requirement to fulfill. Funnel Rocket can only be fast
and operationally simple if the data is organized such that each file includes a unique set of users, so that all the data
rows for specific user are located in the same file. This is what allows each task to work independently of others without 
requiring a notorious _shuffle_ between workers, which is usually the bane of performance in big data processing. 
In effect, if your data isn't already partitioned in such a way, you'll need to prepare a copy of the data that is 
repartitioned appropriately. 

In Spark, this is achieved by calling `repartition(colName)` over the `DataSet` or `DataFrame` you're working with.  
Beware though of `DataFrameWriter.partitionBy()` method, which partitions data into directories a-la Hive style. 
**TBD:** Document the partitioning utility and its limit - test vs. PySpark.

Ideally, you should set the number of parititions so that resulting files are within 20-100 MB per each. Having many small
files would utilitize a large number of workers for diminishing returns in performance. Having large files would make 
querying slower as each file is processed by a single task, and may cause OutOfMemory crashes if memory is too tight.
**TBD link to Lambda/worker suggested RAM settings**

5. Funnel Rocket **does not support joins** between datasets. De-normalize your datasets to include any relevant fields 
you want to be able to query. **TBD** refer to the product feed example

6. **Support for nested data:** Funnel Rocket currently has only limited support for nested data, unlike Spark DataFrames.

6.1 **TBD** implement and documents: bitset columns for a limited set of values (up to 512, non-repeating)

6.2 **TBD** which operators can work on other string/numric lists out of the box?

### Data Format Best Practices

7. **Using Revisions:** TBD document after considering... move from here?

8. **Use numeric and bool types where appropriate:** TBD explain

9. **Using categorical field types:** String columns are usually much more resource-hungry: they are slower to store,
inflate file sizes, slower to read and slower to query. Whenever a string column in a DataFrame seems to only contain the 
same values repeating over and over again, all out of a small set of distinct values, then casting the column the 
'category' type is highly recommended and much easier than trying to map distinct values to int ordinals by yourself. 
 Casting is easy, e.g. `df['some_string_column'] = df['some_string_column'].astype('category')`. 

Making a string column into categorical does not mean losing functionality. All string operations are still supported 
and in fact execute up to an order of magnitude faster. It's a no-brainer to use for a column of 10,000 values with only 5 
unique values, but also useful if there are 50k distict values in a column of 1 million values.

Note that the 'category' type is purely a Pandas feature. The Parquet format does have a similar 
method of dictionary compression, which is automatic and transparent to client. It is however not a data type, and 
does not automatically make columns be loaded into DataFrames as categorical. However, when DataFrames are saved to
Paruqet files and later loaded back, Pandas takes care to store custom metadata in the Parquet file so that it knows how
to cast columns back to their set type when loaded. To get the benefits of this type you'd need to cast the 
relevant columns and then **save to Parquet using Pandas**, rather than via other tools. 
**TBD** a dataset-level mapping of columns to load as category (or other types)

## Components & Flow
**TBD chart...**

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

## Getting Started
### Local Installation
1. Clone this repo.
2. Ensure you have Python version >= 3.8 installed. Typically, this version should be installed side-by-side with the default Python version bundled with your OS, which might be a much older version.
3. Create a virtual environment with your tool of choice - either `virtualenv` or `pipenv`.
     3. To use the classic virtualenv, run: `python 3.8 -m venv venv`, and then activate it with: `source venv/bin/activate`.
4. Run `pip install .` to install dependencies.
5. Additionally, to modify source files and have changes reflected, install the package in development mode: 
`pip install -e .`
6. Local development depends on a running redis. 
You may start a local one by running: 
```
funnel-rocket % docker-compose up -d
Starting funnel-rocket_storage-redis_1 ... done
```
### Containerised dev environment
1. Run `docker build -f docker/funnel-rocket-dev` to build a local dev image.
    1. This will build a local docker image with all needed depenedncies for local development. 

### Unit tests
1. Install test dependencies: `pip install -r requirements/test.txt`
2. Run tests: `pytest`
3. Run test with coverage: `pytest --cov=frocket --cov-report=html`
    3. Coverage reports are available locally under `./htmlcov/index.html`
 
### Preparing the Example Dataset

To give you a sense of how to prepare a dataset, here are the steps to download a public e-commerce dataset from Kaggle, 
optimize it for querying with the help of a readymade script.

Unfortunately, this dataset is fairly obfuscated so it's hard to know what various IDs used stand for. Thus, the 
ingestion script only brings over a few columns.

1. Download the [dataset files](https://www.kaggle.com/retailrocket/ecommerce-dataset/) from Kaggle - free registration 
is required. Extract the files to a new directory. By default, the ingestion script reads and writes from the directory
  `scratch/` under the repository root (this directory name is listed in `.gitignore`).
2. From the repository root directory, run `python frocket/dataprep/ingest_retailrocket_dataset.py`. If you're not using 
the default `scratch` folder, add the argument `-h` for usage notes.
3. This script will:
3.1 Read the approx. 2.5 million user events in `events.csv`
3.2 Load the two auxiliary product properties files and inner-join them with the user events DataFrame based on the 
'itemid' column. This de-normalizes the events data so that relevant product attributes can be looked for without any 
needing any joins at query time - which are not supported.
3.3 Convert a few columns to a more efficient data type: making `available` a boolean column, convert the (probable) 
product price column to float, and last but not least: cast a few string fields to the 
[categorical data type](https://pandas.pydata.org/pandas-docs/stable/user_guide/categorical.html). 
3.4 Finally, the script saves the file as `retailrocket.parquet` in the target folder.
4. We now have a single file with all 2.5 million rows. To make queries parallel, let's partition it into eight parts based
on the 'visitorid' column, with the supplied utility `repartition_dataset.py`
```
(.venv) funnel-rocket % python frocket/dataprep/repartition_dataset.py --input_files scratch/retailrocket.parquet --num_parts 8 --force visitorid
Input files found: 1, map output dir: map, partitions: 8, CPUs: 4, pool size: 1
Task no. 0 for input file scratch/retailrocket.parquet: written 0 files so far... 
All done! total of 8 files created
(.venv) funnel-rocket % ls -l reduce 
total 133472
-rw-r--r--  1 rock  staff  8381029 part-00000.parquet
-rw-r--r--  1 rock  staff  8465614 part-00001.parquet
-rw-r--r--  1 rock  staff  8520166 part-00002.parquet
-rw-r--r--  1 rock  staff  8698879 part-00003.parquet
-rw-r--r--  1 rock  staff  8506691 part-00004.parquet
-rw-r--r--  1 rock  staff  8513456 part-00005.parquet
-rw-r--r--  1 rock  staff  8644739 part-00006.parquet
-rw-r--r--  1 rock  staff  8590802 part-00007.parquet
```

We now have a eight-part local dataset.

To view the schema of these file, get the row count or print a few example rows all from the command-line, install 
parquet-tools. Using brew on a Mac, simply run `brew install parquet-tools`. Here's how the file schema looks like:

```
funnel-rocket % parquet-tools schema reduce/part-00000.parquet 
message schema {
  optional int64 timestamp;
  optional int64 visitorid;
  optional binary event (UTF8);
  optional int64 itemid;
  optional double transactionid;
  optional int64 price;
  optional binary 888 (UTF8);
  optional boolean available;
  optional binary categoryid (UTF8);
  optional int64 __index_level_0__;
}
```

### Registering the Dataset

Here's how to register this new dataset. We need to give it a name (retail), tell Funnel Rocket what's the base path 
(local or remote), how many parts to expects and the name of the "group ID" and timestamp columns in this dataset. By 
default, a few basic validations will be run to ensure the basic schema requirements are met.

```
(.venv) funnel-rocket % python frocket/dataprep/register_dataset.py --help
usage: register_dataset.py [-h] [--revision REVISION] [--skip_validation]
                           name base_path num_parts filename_pattern group_id_column timestamp_column

Register a dataset for querying with Funnel Rocket

positional arguments:
  name                 Dataset name
  base_path            The path all files in the dataset are under. Local paths and "s3://..." paths currently supported
  num_parts            Number of partitioned files in the dataset, e.g. 256
  filename_pattern     support Python formatting, e.g. "part-{:05}.parquet"
  group_id_column      The column name to group rows by, e.g. "userId", "userHash", etc. Each file in the dataset must each have a
                       unique set of groupId values, so that all rows for any given groupId value are found in the same file
  timestamp_column     The column holding the timestamp of each row, e.g. "timestamp" or "ts"

optional arguments:
  -h, --help           show this help message and exit
  --revision REVISION  Revision (can be any string) (default: None)
  --skip_validation    Skip validations of files and columns (default: False)

(.venv) funnel-rocket % mkdir -p data/retailrocket
(.venv) funnel-rocket % mv reduce/part-0000* data/retailrocket 
(.venv) funnel-rocket % python frocket/dataprep/register_dataset.py retail data/retailrocket 8 'part-{:05}.parquet' visitorid timestamp
frocket.common.dataset_utils INFO - Loading first part for validation...
frocket.common.dataset_utils INFO - Column visitorid was found, and without null values
frocket.common.dataset_utils INFO - Column timestamp was found, is numeric and and without NaN values
frocket.common.dataset_utils INFO - Loading last part for validation...
frocket.common.dataset_utils INFO - Column visitorid was found, and without null values
frocket.common.dataset_utils INFO - Column timestamp was found, is numeric and and without NaN values
Validation done!
frocket.datastore.registered_datastores INFO - Creating datastore: RedisDatastore
Dataset registered in RedisDatastore(host: localhost)
```

To list the newly-registered dataset:

```
(.venv) funnel-rocket % python frocket/invoker/cli.py --list
frocket.datastore.registered_datastores INFO - Creating datastore: RedisDatastore
__main__ INFO - 
name       revision    registered_at               base_path                           num_parts  filename_pattern                       group_id_column    timestamp_column
---------  ----------  --------------------------  --------------------------------  -----------  -------------------------------------  -----------------  ------------------
retail                 2020-11-09T15:20:35.358628  data/retailrocket                           8  part-{:05}.parquet                     visitorid          timestamp
```

### Running Queries Locally

First, let's run one or more workers. No configuration changes would be needed, as the default invoker type "work_queue" 
connects to Redis on localhost by default.

In another terminal, activate the virtual environment and run:

```
(.venv) funnel-rocket % python frocket/worker/impl/queue_worker.py
frocket.datastore.registered_datastores INFO - Creating datastore: RedisDatastore
__main__ INFO - Waiting for work...
```

Each worker processes a single task at a time, but you can launch as many as your resources allow for.

Create a JSON file with our initial query below. Let's call this file `scratch/hello-query.json`:

```
{
    "logical_expr": "and",
    "conditions": [
        {
            "type": "filter",
            "filter": {
                "column": "event",
                "operator": "==",
                "target": "'transaction'"
            },
            "evaluator": {
                "type": "size",
                "column": null
            },
            "hitcount": {
                "target": 3,
                "operator": ">="
            }
        }
    ]
}
```

The format is a bit verbose, especially as we're not using any complex features yet.
Let's run it:

```
(.venv) funnel-rocket % python frocket/invoker/cli.py --run retail scratch/hello-query.json

2020-11-09 18:07:49,564 frocket.datastore.registered_datastores INFO - Creating datastore: RedisDatastore
2020-11-09 18:07:49,568 frocket.invoker.registered_invokers INFO - Creating invoker: WorkQueueInvoker
2020-11-09 18:07:49,571 frocket.invoker.impl.async_invoker INFO - Enqueued 8 requests for dataset retail
2020-11-09 18:07:50,352 frocket.invoker.invoker_api INFO - Query successful
{"dataset": {"name": "retail", "revision": null, "registered_at": 1604928035.358628, "base_path": "data/retailrocket", 
 "num_parts": 8, "filename_pattern": "part-{:05}.parquet", "group_id_column": "visitorid", "timestamp_column": "timestamp"}, 
 "request_id": "1604938069-277dcb9d", "success": true, "error_message": null, 
 "counters": {"ended:total": 8, "ended:status:TaskStatus.SUCCESS": 8, "query:users": 1004, "query:rows": 117829}, 
 "metrics": [{"name": "ASYNC_ENQUEUE_TIME", "value": 0.0008623600006103516}, {"name": "ASYNC_POLL_TIME", "value": 0.7805960178375244}, 
             {"name": "INVOKER_TOTAL_TIME", "value": 0.7817888259887695}], 
 "base_labels": {"COMPONENT": "INVOKER", "SUCCESS": true}}
2020-11-09 18:07:50,352 frocket.invoker.invoker_api INFO - Query result: 
2020-11-09 18:07:50,381 __main__ INFO - Result:
{
  "dataset": {
    "name": "retail",
    "revision": null,
    "registered_at": 1604928035.358628,
    "base_path": "data/retailrocket",
    "num_parts": 8,
    "filename_pattern": "part-{:05}.parquet",
    "group_id_column": "visitorid",
    "timestamp_column": "timestamp"
  },
  "request_id": "1604938069-277dcb9d",
  "success": true,
  "error_message": null,
  "counters": {
    "ended:total": 8,
    "ended:status:TaskStatus.SUCCESS": 8,
    "query:users": 1004,
    "query:rows": 117829
  },
  "metrics": [
    {
      "name": "ASYNC_ENQUEUE_TIME",
      "value": 0.0008623600006103516
    },
    {
      "name": "ASYNC_POLL_TIME",
      "value": 0.7805960178375244
    },
    {
      "name": "INVOKER_TOTAL_TIME",
      "value": 0.7817888259887695
    }
  ],
  "base_labels": {
    "COMPONENT": "INVOKER",
    "SUCCESS": true
  }
}
```

As the dataset files are local and not too big, the query should complete in about 2 seconds or less. If there's no progress,
check to see that the worker process is still running.

Buried in all that output, let's focus on the following:

```
  "success": true,
  "error_message": null,
  "counters": {
    "ended:total": 8,
    "ended:status:TaskStatus.SUCCESS": 8,
    "query:users": 1004,
    "query:rows": 117829
  },
```

This tells us that the query was successful, 8 tasks have ran, and the number of matching users is 1004, and they have 
performed almost 118,000 activities (which is probably skewed by a few bots). Quickly adding another filter would tell us
how many of these users had less than 50 pageviews. Here's how the query looks like now:

```
{
    "logical_expr": "and",
    "conditions": [
        {
            "type": "filter",
            "filter": {
                "column": "event",
                "operator": "==",
                "target": "'transaction'"
            },
            "evaluator": {
                "type": "size",
                "column": null
            },
            "hitcount": {
                "target": 3,
                "operator": ">="
            }
        },
        {
            "type": "filter",
            "filter": {
                "column": "event",
                "operator": "==",
                "target": "'view'"
            },
            "evaluator": {
                "type": "size",
                "column": null
            },
            "hitcount": {
                "target": 50,
                "operator": "<"
            }
        }
    ]
}
```

Re-running the query, we now get:

```
    ...
    "query:users": 759,
    "query:rows": 18963
    ...
```

Say, though, we want to know how many users had at least one purchase whose value was at least X (in whatever 
denomination the example dataset is - it seems to be include very high numbers which may actually be in 
fractions of cents; such a representation is needed when converting multiple currencies to a single one).

The gotch here is we want to find users who've made a transaction, and the value of the 'price' column should be X or above
for that transaction row - not any other row which might have a value in the price column. In the query above,
each condition was a separate filter at the user level, not over the same row. 

To state multiple filters over the same row, let's define a condition of type "mfilter":

```
{
    "logical_expr": "and",
    "conditions": [
        {
            "type": "mfilter",
            "relation": "&",
            "filters": [
                {
                    "column": "event",
                    "operator": "==",
                    "target": "'transaction'"
                },
                {
                    "column": "price",
                    "operator": ">=",
                    "target": "100000"
                }
            ],
            "evaluator": {
                "type": "size",
                "column": null
            },
            "hitcount": {
                "target": 1,
                "operator": ">="
            }
        }
    ]
}
```

The result should be 4018 matching users. The `hitcount`, in this case, states how many rows per user should meet the full 
condition.

Of course, we can still use a combination of different conditions, each definining one or more filters which should be met 
in the same row with either an 'and' (&) or an 'or' (|) relation.

Now, for something a bit different: we want to find users who bought from `categoryid` 999. Instead of looking for a number 
of per-user purchases,we'd like to know how many users have made such purchases with a total value of X or more, regardless 
of whether it was all in one purchase or not. We also don't care about the value of any other purchases they've made. 
Here's the updated query:
```
{
    "logical_expr": "and",
    "conditions": [
        {
            "type": "mfilter",
            "relation": "&",
            "filters": [
                {
                    "column": "event",
                    "operator": "==",
                    "target": "'transaction'"
                },
                {
                    "column": "categoryid",
                    "operator": "==",
                    "target": "'999'"
                }
            ],
            "evaluator": {
                "type": "sum",
                "column": "price"
            },
            "hitcount": {
                "target": 100000,
                "operator": ">"
            }
        }
    ]
}
```

...and the answer is: 6 users.

If we wanted to know how many users either purchased from that category over some total amount, *or* over some number of 
times, we'd need to add another top-level condition which looks a lot like the above (but with different `evaluator` and 
`hitcount`), and change to `"logical_expr": "or"` at the top.

Now, let's add an actual funnel to the mix: for users with who've had 5 product views or more, let's figure out how many actually
proceeded to add to cart and then buy something - and how many then did that again? Note the new attribute `breakdown_sequence` 
below:

```
{
    "logical_expr": "and",
    "conditions": [
        {
            "type": "filter",
            "filter": {
                "column": "event",
                "operator": "==",
                "target": "'view'"
            },
            "evaluator": {
                "type": "size",
                "column": null
            },
            "hitcount": {
                "target": 5,
                "operator": ">="
            }
        }
    ],
    "breakdown_sequence": [
        {
            "column": "event",
            "operator": "==",
            "target": "'addtocart'"
        },
        {
            "column": "event",
            "operator": "==",
            "target": "'transaction'"
        },
        {
            "column": "event",
            "operator": "==",
            "target": "'addtocart'"
        },
        {
            "column": "event",
            "operator": "==",
            "target": "'transaction'"
        }
    ]
}
```

This query returns:

```
  "counters": {
    ...
    "query:users": 72254,
    "query:rows": 869706,
    "query-breakdown:step-0:users": 12473,
    "query-breakdown:step-1:users": 4636,
    "query-breakdown:step-2:users": 1044,
    "query-breakdown:step-3:users": 697
  },
```

First, the population is filtered using the given conditions array - and we get ~72k matching users. Then, this population
is iteratively taken through each step in the sequence, in order, and we find that 12,473 have passed the first step 
('addtocart` event), 4,634 users have then performed a transaction, and so on.

Note that a sequence can also be used as a regular condition, in which case only users who've completed *all steps* in order
will be included in `"query:users"`. As a condition, a sequence is defined in the following format:

```
        ... other conditions
        {
            "type": "sequence",
            "filters": [
                {
                    "column": "eventId",
                    "operator": "==",
                    "target": "187359"
                }
            ]
        }
        ...
```

## Running Distributed Queries in Production

### Running Redis

Running Redis in production can be done in multiple ways. In AWS, one of the easiest ways is to use Elasticache. 
It's recommended to have about at least 100 MB of storage in Redis, to allow some historical data to be kept (**TBD** TTL for keys and eviction policy). 
Any of the cache.t3 instance types on offer will suffice, but having at least one replica is recommended, in a multi-AZ setup. 
Generally, avoid using cache.t2 instances due to their weak network performace. 

As with any AWS service, pay close attention to which VPC and Security Group you're selecting, as the invoker and all 
worker components will need access to the Redis endpoint through port 6379. When the cluster is up, write down its *Primary Endpoint*.

### Running an Invoker

**TBD** write a docker image for the API server (or one-shot command line), upload to Docker Hub (build should pull from pypi...)
**TBD** at least instruct to install from pypi...

Running the invoker on an EC2 machine is very similar to running locally, but you should take note of having the correct 
network & permission settings. The invoker is a pretty lightweight process, but it does use multiple threads to invoke
Lambdas concurrently, and on very small machines this will work slower and thus delay queries. Use a 't3.large' instance 
or better.

1. Set an IAM Role for the machine (or create an IAM User and use its credentials) that has the following two roles: 
1.1 The same role you've created before for S3 read access
1.2 The built-in `AWSLambdaRole` to allow invoking Lambdas
2. Make sure the machine has network access to your Redis instance in port 6379, whether they are in the same VPC and Security 
Group or not (using `telnet <redis-host> 6379` is a simple way to check)
3. On the target machine:
3.1 Clone this repository 
3.2 Create and activate a virtualenv based on Python 3.8+
3.3 Run `pip install .` in the repository root dir

### Uploading and registering a dataset

Decide on a location in S3 for datasets. Each dataset can be in a different base path altogether, however it does make 
sense to put them all in the same bucket for easier management and access control. In any case, ensure the bucket and 
Funnel Rocket components are located in the same region.

To start with, you can upload the Retail Rocket dataset we've created before.
Then, use `python frocket/dataprep/register_dataset` from the invoker machine to register it with your Redis instance.

**TBD** calling list to view datasets (plus any easier validation calls?)

### Running Workers Based on a Redis Queue

Here's what it takes to run queue-based workers on multiple nodes:
**TBD** write a docker-compose file with workers! (and later API server)

1. Upload the dataset to a bucket in cloud storage (currently, only S3 support is implemented)
2. Make sure worker machines have read access to it - a mundane yet often painful step. Funnel Rocket uses the 
standard boto3 package to download files from S3, so [any credentials setup that is respected by boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) 
would work.
3. On all worker nodes:
3.1 Clone this repo and install it as a package (`pip install .`)
3.2 Set the environment variable FROCKET_REDIS_HOST=<redis-hostname>
3.3 Run (`python frocket/worker/impl/queue_worker.py`)

### Running Serverless Workers via AWS Lambda
**TBD** supply automated deployment for both layer & code - can be with either AWS SDK / SAM / Terraform / Serverless fw.

#### Manual Packaging

Since Funnel Rocket depends on a few relatively large packages (notably Pandas, NumPy, PyArrow), code deployment is 
broken into two parts:

1. **Packaging dependencies** as a [Lambda Layer](https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html),
which is basically Lambda's equivalent of a base image layer (and multiple ones can be used). This needs to be done only once (or when adding new dependencies).

Since some packages rely on native libraries, the layer packaging process is running in Docker with an Amazon Linux 2 base 
image, to ensure the appropriate native libraries are used. 

```
cd layers
./build-layer.sh
``` 

This may take a few minutes to run the first time, and produces a file named `frocket-packages-layer.zip`.
The result file size is currently about 37 MB, so it cannot be uploaded directly from your computer via the AWS console.
You'll need to upload the .zip file to S3 first, to any bucket that you control, and then point to its S3 path when [creating the layer](https://console.aws.amazon.com/lambda/home#/create/layer).
The layer name is up to you, and all other fields are optional.

2. On each code change, packaging & deploying the Funnel Rocket source code itself, which is lightweight.

From the repository root, run:
```
./layers/build-lambda.sh
```

This will package sources in `./lambda-package.zip`, a compact file which can be manually uploaded (till we have a deployment method **TBD**)

#### Manual Lambda Configuration through the AWS Console UI

Before you create the Lambda itself:

1. Determine the **VPC and Security Group** to assign to the Lambda. The Lambda needs access to your Redis instance, which is
most probably inside a VPC. Using Lambdas _outside_ of a VPC is no longer recommended practice, and does not bring a performance 
benefit anymore.

2. The Lambda function will also need to access S3. While VMs running in a VPC generally don't need any special setup to 
access S3, various AWS managed services, including Lambda, do need an [Endpoint Gateway](https://console.aws.amazon.com/vpc/home#Endpoints:sort=vpcEndpointId) 
set up their VPC and configured to expose the S3 service. Fortunately, that normally takes just a few clicks in the console.
The endpoint handles routing, but not permissions - you will still need to define the right IAM permissions:

3. Create an IAM Role for the Lambda:
 
3.1 First, we'll need an IAM policy allowing read-only access to the bucket/s where datasets are stored (using one of 
the built-in AWS-managed policies such as 'AmazonS3FullAccess' or 'AmazonS3ReadOnlyAccess' is not advisable!). 
[Create a new policy](https://console.aws.amazon.com/iam/home#/policies) with a JSON payload similar to this:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:Get*",
                "s3:List*"
            ],
            "Resource": "arn:aws:s3:::<bucket-name>"
        }
    ]
}
```

3.2 Now, create an IAM Role itself, with two policies attached: the policy you've just created above for S3 access, plus 
the AWS-managed role `AWSLambdaVPCAccessExecutionRole` - which any Lambda requires for running in a VPC.

To create the Lambda itself:

1. Start [creating a new Lambda function](https://console.aws.amazon.com/lambda/home#/create/function).
2. Under 'Basic Information':
2.1 function name: the invoker will try to call a Lambda named `frocket` by default, and that it the recommended name. 
If you choose another name for your Lambda, set the environment variable FROCKET_INVOKER_LAMBDA_NAME to that name for the 
invoker process.
2.2 Runtime: Python 3.8 or later
2.3 Permissions: choose 'Use an existing role' and select the role you've created above.
2.4 Under 'Advanced Settings', choose the appropriate VPC. Choose all subnets and the Security Group to use.
2.5 Click 'Create'
3. Expand the 'Designer' section and click on 'Layers' to add a layer. Choose the custom layer you've created above, at 
version 1.  
4. Going one section down to 'Function code', click the 'Actions' menu and upload the file `lambda-package.zip` you've 
created above. Don't worry if it spins forever looking for the handler (entrypoint) function - we will define that now.
(and the invoker)
5. One section down in 'Runtime settings':
5.1 Click 'Edit' and change the handler to: `frocket.worker.impl.aws_lambda_worker.lambda_handler`
5.2 Add two environment variables:
5.2.1 Set FROCKET_REDIS_HOST to the Redis hostname, a.k.a 'Primary Endpoint' in Elasticache.
5.2.2 Optionally, set FROCKET_WORKER_REJECT_AGE to 1000000. This will allow you to test the Lambda from the AWS console 
with saved test payloads, without the Lambda rejecting the test payload due to its non-recent timestamp.
5.3 In 'Basic Settings' section, click 'Edit':
5.3.1 Memory: setting to 2048 MB is recommended. This is not only to allow enough headroom for some big Parquet files to 
be loaded and queried (remember, Parquet files are heavily compressed on disk), but also since [the portion of vCPU you get
is dependent on allocated memory](https://epsagon.com/observability/how-to-make-aws-lambda-faster-memory-performance): 
**only at slightly below 2GB RAM do you get a full vCPU per Lambda invocation**. In general, the Funnel Rocket worker is single-threaded, though
Apache Arrow's Parquet driver can parallelize loading of multiple columns to some extent. Bottom line, for queries to run
optimally, you'll want about one full vCPU allocated to you per worker.
5.3.2 The default timeout is a bit on the short side - set it to 30 seconds.
5.4 Down below in the 'Asynchronous invocation' section, edit the settings:
5.4.1 Maximum age of event should be a few minutes at max, since older events are part of queries that have most probably 
timed-out already.
5.4.2 Set the no. of retry attempts to zero - Funnel Rocket will handle retries by itself.

Assuming you've already uploaded and registered at least the example Retail Rocket dataset, we can now test a single Lambda
invocation for a file from this dataset:

#### Testing it all - TBD
**TBD** Testing the Lambda with an example part from public S3 bucket and/or with special validation message/
also from invoker side

## Cost Estimation - TBD (feature needed)

## Tech Notes - TBD

### Performance & Caching - TBD
Explain how caching works in warm start

### Lambda Limits - TBD
Lambda limits (concurrency, memory/CPU, storage, network)
per region / provisioned

### Configuration options - TBD

### Query Spec - TBD

## FAQ - TBD

Q: Are there no other established tools to perform such queries?
Q: What happens if Redis fails?
