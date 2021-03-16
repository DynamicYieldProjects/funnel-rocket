<p align="left">
  <a href="#">
    <img alt="Tests status" src="https://github.com/DynamicYieldProjects/funnel-rocket-oss/workflows/Tests/badge.svg" />
  </a>
</p>

<p align="center">
  <img alt="Funnel Rocket logo" src="./docs/logo-blue.svg" width="350">
</p>

# Funnel Rocket

## Intro

Funnel Rocket is a query engine built to efficiently run a specific type of query:

Given a large dataset of user activity (pageviews, events, clicks, etc.), find the users whose activities meet a specific
condition set, optionally with a **specific order of events and time constraints**, and return multiple metrics on the matching group.

The engine can also perform a full funnel analysis, in which user counts and aggregations are returned for each step in the sequence.

### The Challenge

If you're a vendor oferring analytics, personalization, content or product recommendations, etc. you may wish to offer such query capability
to each of your customers, allowing ad-hoc data exploration to better understand user behavior and define audience groups. 

However, such queries are still a challenge to build with existing tools (SQL or NoSQL). The're not only tricky to get right, but are pretty hard to optimize for performance and cost to run & operate. At a high level, executing such a query requires you to first perform a high cardinality grouping first (100 million users => 100 million groups), then run multiple passes over each of these group to execute all conditions in the desired order. An alternative method is to "pre-bake" results by batch processing, thus limiting what freedom your users have to explore the data.

### Project Scope

The original aim of this tool was very specific: replacing an aging solution, but we've found it can be easily extended to perform many more use-cases around large scale user-centric data processing (or other entities, of course). 

Funnel Rocket certainly does not match the expressiveness and flexibility of mature query engines or batch processing frameworks, but for what it offers we've found that's it's very fast to scale with low resource and management overhead, amounting to significantly lower TCO. 

## The Technology Used (or: Clusters are Hard)

Funnel Rocket is basically the bringing together a few excellent, proven components which do most of the heavy lifting.

### 1. Pandas
The concept of the *DataFrame* doesn't need much introduction, and allows for runnning complex transformations at ease with
good performance (if you're mindful enough; *Numba* can help in the performance-critical parts). Coupled with *Apache Arrow* (also by the industrious @wesm) you also get great Parquet support. 

Pandas itself is a library running within a single process, but tools such as *Dask* and *PySpark* have brought either the library itself 
or its core abstractions to the distributed domain. 

However, operating distributed cluster for data processing engines gets pretty tricky as you grow. Deploying, scaling and fixing the 
inevitable periodic issues can get very time-consuming. When something breaks, it can be hard to tell what's going on. As data and needs grow, 
virtually any technology would reach some unexpected size/load threshold and  start performing poorly or behaving erratically.

Scaling usually leaves a lot to be desired: the cluster either has "too much" resources sitting idle, or not enough to handle
temporal load spikes. That translates into a lot of money at scale. 

There's no no silver bullet, of course, yet we can take a stab at the problem from another angle: serverless.

### 2. Serverless
*Currently supporting AWS Lambda, other integrations welcome.*

People tend to be split on serverless, for a bunch of reasons. We've found AWS Lambda to be mature, reliable and (yes) fast enough service which can scale to hundreds or thousands of cores almost immediately. 

The compute cost per GB/second (or vCPU/second) is indeed higher in this model, but you pay only for actual processing time: from the time your handler starts till it ends. You're billed in millisecond granularity, excluding any compute time it took the underlying VM or your process to reach the state where the handler can start its work. Thus, it is very fitting for bursty on-demand jobs. You also spend relatively very little time on operations.

For a tool that's measured in seconds rather than milliseconds, Lambda turned up to be good enough (even aiming for the low single digits). 
If customers tend progressively tweak their queries while exploring, then *warm start* and a bit of smart data caching go a long way to speed up subsequent queries even further, to 2-3 seconds total in our tests. You can always 'pre-bake' some default/common queries beforehand using the non-serverless mode - see below.

Funnel Rocket uses the asynchronous Lambda invocation API, making it much easier to launch hundreds+ of jobs quickly. Invocation reqeusts are put into a queueing mechanism, which adds no meaningful latency in normal operation yet prevents most cases of rate limiting.

Of course, having multiple distributed jobs and tasks in flight, handling retries, etc. still takes some management infrastructure. 
Luckily, there's Redis.

### 3. Redis and the Lightweight Cluster Option
The versatility of Redis data structures makes it a natural choice for handling lightweight metadata, work queues, real-time 
status tracking and more. There is a range of managed offerings to choose from which won't break the bank, as this use case 
only requires a modest amount of RAM.

Other than for managing metadata on datasets, Redis is used in two more ways:

1. For tracking and storing the status and outputs of all individual tasks, since the invoker (server) does not rely on synchronous responses.
1. **To support a non-serverless deployment option where Redis also acts as a work queue** from which 
long-running worker processes fetch tasks. 

This latter option is a pretty easy to set up: each worker is a simple single-threaded process which anonymously 
fetches work from a shared queue, no additional cluster management or load balancing required. Processes take tasks off a Redis list at their own pace, based on what scale you currently have.

You can combine both deployment modes, using this mode for pre-baking default/common queries on cheap spot instances (in AWS jargon), storing the results and scaling down to zero. That way, you only utilize lambdas when users start exploring beyond the default view.

Both deployment options push much of the complexity into battle-tested tools. Both depend on Redis as their single stateful component.
Thus, running a muti-zone Redis setup is recommended in production. In the worst case, you'd need to re-register all active datasets.

For more on the design and components of Funnel Rocket, [click here](./docs/design.md).

## Preparing Data for Querying

Funnel Rocket currently supports Parquet files only, located under the same base path either on a locally-mounted filesystem
or in S3 and compatible object stores (e.g. MinIO). **TBD:** add more storage systems and file formats.

### Mandatory Columns

1. A **group ID** column: a string or numeric column with the user ID / user hash / other group ID, with no null values. 
   All query conditions are performed in the scope of each group. The column name is set by the client, per dataset.
1. A **timestamp** column: either int or float, typically a Unix timestamp in the granularity of your data (e.g. int64 of seconds
   or milliseconds, float of seconds.microseconds, etc.). Currently, Funnel Rocket does not impose a format, as long as it's consistent.

### Partitioning by the Group ID Column

For Funnel Rocket to be fast and simple, the data should be organized so that each file includes a unique set of users, 
with all rows for a specific user located in the same file. This means an expensive shuffle step can be avoided at query time. 

Assuming your dataset is not already partitioned that way, you can use (Py)Spark or similar tools to perform it. 
For example, with PySpark call [DataFrame.repartition()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.repartition.html)
and save the resulting dataset.

A standalone utility for data repartitioning is [included in this repo](./dataprep_example/repartition.py).
It is non-distributed but can use all available CPUs, so you can prepare datasets up to a reasonable size on larger machines.

Aim to have files size in the range of 20-150mb. See the [Operations Guide](./docs/operating.md) for more.

## Running Locally

### Run with docker-compose

This is the easiest and most complete option, as it includes a local S3-based object store, an AWS Lambda-like environment and Redis which Funnel Rocket is configured to use.

#### Launching Services

Clone this repo and `cd` into it.

To have the Funnel Rocket image based on the source code in the local repository instead of being downloaded from Docker Hub, build it locally: 
`docker build -f docker/all-in-one.Dockerfile . -t frocket/all-in-one:latest`.

Run `docker-compose up`. This will start the following services:

* _Redis_ as the datastore, available to the host at localhost port 6380 (as you may have a local Redis running on default port 6379).

* _MinIO_ to test S3-based datasets locally. Its admin UI is available to the host at http://localhost:9000.

* _frocket-queue-worker_ and _frocket-apiserver_ are both based on the **frocket/all-in-one** multi-role image. The API server will be available at http://localhost:5000, while the worker connects to Redis blocking for work.

* _frocket-lambda-worker_ based on the **frocket/local-lambda** image, while will be built locally as it's aimed at local testing purposes only. It is wrapping a Funnel Rocket worker running within the AWS Lambda environment (based on lambci/lambda:python3.8). This worker is not used by default by the API server, but this can be modified by uncommenting `- FROCKET_INVOKER=aws_lambda` in the `docker-compose.yml` file. It is also called directly by unit tests.

To make jobs run faster, you can scale the no. of workers, e.g. `docker-compose up --scale frocket-queue-worker=4`. Workers only take about 50-60mb RAM each.

#### Testing the Setup

The best way to fully validate your setup is to run automated tests with `pytest` against it, which requires also installing Funnel Rocket as a package:

1. Make sure you have Python 3.8+ as the default python in your environment. Using _virtualenv_ or _conda_, etc. for isolation is of course encouraged.
1. Install the package from local sources: `pip install -e .`.
1. Install packages required for tests: `pip install -r test-requirements.txt`
1. Run `./test-docker-compose.sh`. This takes care to set environment variables for tests to connect to the running docker-compose services.

### Run on the Host

#### Installing

To install the latest package from PyPI:

In a Python 3.8+ based environment (preferably an isolated one with _virtualenv_ or _conda_), run `pip install funnel-rocket`

To install from source code:

Clone this repository, `cd` into it and `pip install -e .`. To install test requirements run `pip install -r test-requirements.txt`.

#### Running Redis

Make sure you have Redis running locally. This is usually easy to do with your preferred package manager. 

Funnel Rocket can be configured to use a non-default logical DB (meaning, > db 0) by setting `export FROCKET_REDIS_DB=<logical db number>`. All keys written by Funnel Rocket are prefixed by 'frocket:'. To configure this prefix and for more settings [see here](./docs/operating.md).

#### Running the Worker and API Server

To run a worker waiting on the Redis queue for tasks: `python -m frocket.worker.impl.queue_worker`.
You should see the following output:
```
frocket.datastore.registered_datastores INFO Initialized RedisStore(role datastore, host localhost:6379, db 0)
__main__ INFO Waiting for work...
```
You can launch a few of these in the background, to speed up jobs.

To run the API server with the Flask built-in server: `FLASK_APP=frocket.apiserver python -m flask run` (not for production use; the Docker image uses gunicorn with multiple processes).

#### Testing the Setup

To run tests you'd need the source code and requirements installed locally first - see the section _'Installing the package'_ above.
Most tests require an S3-compatible store for testing. 

You can start MinIO via `docker-compose start mock-s3` (it's pretty lightweight), or alternatively other real/mock S3 services. Assuming you've run the MinIO service as suggested, run `SKIP_LOCAL_LAMBDA_TESTS=true pytest tests/ -vv`

With other services, set the following environment variables to their approriate values first: `MOCK_S3_URL`, `MOCK_S3_USER` and `MOCK_S3_SERCET`.

## Creating & Querying an Example Dataset

Follow [this guide](./docs/example-dataset.md) to learn more on preparing, registering and querying a real dataset.

## Running in Production

For detailed instructions on how to configure, deploy and monitor Funnel Rocket in a production AWS environment, see the [Operations Guide](./docs/operating.md).

## High Level Roadmap

* Features: Extend support for column types: datetime, lists of primitives, exact/contains lookup in delimited string fields
* Features: Implement gaps in conditions, mostly around sequences: step *did not happen*, min/max duration between steps.
* Features: Basic UI for running a query with schema validation. Potentially ad-hoc schema generation per dataset.
* Performance: integrate currently-experimental Numba code for critical points in code (needs to be AOT-compiled for Lambda, requires arch-dependent release and always a Python-only fallback), such as isin().
* Data preparation/Performance: support re-partitioning by group ID as a job. Consider incremental re-partitioning (as new data comes in). Preparing the data by Funnel Rocket also allows applying some important performance optimizations, which are currently experimental
  * Encode list columns as bitsets for superfast conditions, transparent to the client (up to limited cardinality; requires encoding of dictionary in Parquet metadata of each file)
  * Ensure any columns which are good candidates for categorical representation are stored as such (have a dictionary in the Parquet file)
  * Convert non-optimized file formats to Parquet (or Apache Arrow's Feather file format).
* Deployment: automated method/s for AWS Lambda deployment
* Deployment: provide k8s chart/operator and scaling metrics for a non-serverless deployment
* Support more cloud providers

## Maintenance

This project is actively developed by Elad Rosenheim and Avshalom Manevich.  Special thanks to: Omri Keefe (@omrisk) for CI work, Gidi Vigo for the logo.

Funnel Rocket is licensed under Apache License 2.0.
