# Operations Guide

## Running in Production

The most important operational decision to make is how to run workers: either serverless with AWS Lambda, 
or with 'classic' worker processes taking work from a Redis queue.

This is mostly a question of your usage pattern:

* If queries run *en masse* as part of a big batch process, and per-query latency is less an issue than total capacity, 
  then running Redis-based workers makes more sense. This is especially true when using cheap spot instances. Since these workers
  need no centralized master or load balancer, and don't have any complex retry logic/state of their own, then it's ok for workers to simply 
  disappear in a puff of smoke from time to time. You can scale up and down the amount of workers throughout the day dependeing on need.

* In a typical on-demand workload where queries should run fast but don't need a ton of resources up all the time (or for hours on end), 
  then serverless can be the cheapest and best performing option - scaling up quickly to serve requests, 
  without paying for any long-running infra.

Whichever one you use, you will also need to have:
1. **A Redis database**.
1. **An API Server**, preferably more than one for high availability.

Both API Servers and Workers must have access to Redis and access to the dataset files, either on S3 or a shared filesystem.
There is no direct communication between the two components: requests and responses are mediated via Redis, and in the case of using Lambdas 
also via the AWS API.

### Running Redis

Running Redis in production can be done in multiple ways. In AWS, one of the easiest ways is to use Elasticache. 
* It's recommended to have about at least 500 MB of available RAM in Redis. 
* Any of the *cache.t3* instance types on offer should suffice, but having at least one replica is recommended in a multi-AZ setup. 
* In general, avoid using previous-generation *cache.t2* instances due to their weak network performace. 

The Redis port (6379 by default) should be accesible to (a) the Lambda function (if you're using Lambdas), (b) the API server,
and (c) any Redis-based workers.

The port, host and logical DB of the Redis server are configured via the environment variables `FROCKET_REDIS_HOST`, `FROCKET_REDIS_PORT` and
`FROCKET_REDIS_DB`, as documented in the configuration reference below.

### Running the API Server

#### Permissions

##### 1. Redis

Make sure the node has network access to your Redis instance on port 6379, whether both are in the same VPC and Security 
Group or not (using `telnet <redis-host> 6379` is a simple way to check access).

##### 2. Remote Storage

The API Server nodes should have a `s3:ListBucket` IAM permission to the bucket/s where datasets reside.

Generally, there are two ways to grant service access permissions in AWS:
1. Using an IAM role.
1. Using access key & secret of an IAM user. Since Funnel Rocket uses the standard boto3 package3, 
   [any credentials setup that is respected by boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) would work. However,
   you can set or override credentials by using Funnel Rocket-specific configuration varibles (see reference below). 
   This allows you to specify different keys for specific AWS services, and make sure no other component inadvertently uses them.
   
You are encouraged to limit IAM permissions as much as possible. The API Server does *not* need read permissions (`s3:GetObject`),
and certainly not any permission to modify objects or buckets. It does not need permission to access irrelevant buckets.

In future releases, we plan to delegate dataset file listing to workers as well - meaning that the API Server itself will not need any 
permissions to remote storage. Any future features for transforming datasets would necessitate giving limited write permissions
to workers (but not the API Server) to write a modified copy of the data but not replace it.

##### 3. Invoking Lambdas

To use Lambda-based workers, grant the API Server the `lambda:InvokeFunction` IAM permission  
(preferably, limit the IAM policy to only that function's ARN rather than being able to run any Lambda function in the account).

Two other things you'll need to do is grant the Lambda function itself the needed permissions, and set `FROCKET_INVOKER=aws_lambda` for the API Server.
Both are covered further down this guide.

#### Using the Docker Image

This is the simplest option (though providing k8s templates is still TBD). Take a look at `docker-compose.yml` as an example.

Use the image **frocket/all-in-one:latest** which can run either as an API Server or a Redis-based Worker, 
depending on the given command: `apiserver` or `worker`.

When running as an API Server, the container runs a Flask application with a [gunicorn](https://gunicorn.org/) server. 
1. **Port:** The default port is **5000**. 
   * This is configurable by setting the env. varible `APISERVER_PORT` for the container.
1. **Number of webserver workers:** gunicorn is currently configured to use multiple processes - **8 by default**.
   * This is configurable with the variable `APISERVER_NUM_WORKERS`. 
1. **Memory:** Each worker uses about 50-70mb of RAM, so you can comfortably fit a default 8-process API server in 1 GB of RAM
1. **CPU**:
   * If you're going the serverless path for workers, the API Server should be allocated *at least* 2 CPUs with a higher maximum limit (4+).
     This is because invoking a mass of Lambda function via the AWS API on each query is executed in parallel through a thread-pool, and is quite resource-hungry 
     (TBD: improve this mechanism).
   * When using Redis-based workers, a single CPU is enough for the API Server.
1. **High availability:** Have at least 2 API Servers running. There is nothing you need to do for orchestrating these: 
   Each will launch its own requests with unique identifiers, while dataset registration/unregistration in Redis is atomic.

#### Via PyPI

If you'd like to install & run Funnel Rocket yourself:
1. Ensure you have Python 3.8+ installed.
1. Install the code & all dependencies via `pip install funnel-rocket`. 
   * Note that since some dependencies include binary libraries (e.g. NumPy), 
     so using dependencies that were installed in a different OS and copied over as-is to another target machine may cause processes to crash.
1. To run the API Server: `python -m gunicorn.app.wsgiapp frocket.apiserver:app --bind=0.0.0.0:<port> --workers=<num-processes>`. 
   * You can use other WSGI servers, but note that not all necessarily support *HTTP streaming (chunked transfer encoding)* 
     which is an optional feature the API Server can use to send progress updates.
   
See below for all configuration options.

### Running Redis-Based Workers

This is quite similar to running an API Server:

Using the **frocket/all-in-one:latest** Docker image, run the container with the `worker` command.

To run as a regular process, `pip install funnel-rocket` in a Python 3.8+ environment (as above) and run:
```
python -m frocket.worker.impl.queue_worker.py
```
The process will connect to the configured Redis server and start taking on tasks.

#### Compute requirements

1. **CPU:** Allow each worker one full CPU, or you'll get decreased performance.
   * Workers are essentially single-threaded, so you won't generally get improved performance from having multiple CPUs. 
   * However, the Parquet driver can make use of multiple threads when loading files, so limiting the container's CPU at a bit more than one CPU does make sense.
1. **Memory:** The amount of RAM allocated should be 10x (or more) than the typical size of a single Parquet file in your dataset. This
   is because Parquet files are usually well compressed while the in-memory data layout can get quite large. 
   The actual amount needed varies greatly by the number of columns used in a query and their types.
1. **Local storage:** Have at least 0.5 GB of storage available in the tmp directory. The minimal amount should cover downloads of Parquet files 
   from remote storage. Extra space would be used for locally caching files with a simple LRU mechanism.

## Running AWS Lambda-Based Workers

### On the Invoker Side

The default invoker implementation is through Redis queues. 
To switch to Lambda, set the environment variable `FROCKET_INVOKER=aws_lambda` for the API Server.

### No Automation Yet
Currently, we do not offer an automated method for deploying Funnel Rocket as a Lambda function, but only the packaging of files.

This is because in production there are various things to get right (Redis, networking, security) which are outside the scope of
the function itself, which need to be handled with care. Terraform is not a great tool for deploying Lambdas at this time, 
and serverless frameworks solve for some of that yet mostly assume admin-level permissions (i.e. not a good fit for your production).

We'll continue to look at options, including AWS Serverless Application Repository/AWS SAM/The Serverless Framework, at least for development & staging environments.

Code contribution for this (and for non AWS-specific implementations) is welcome!

### Setting the Environment

Here's what to prepare in terms of networking & security *before* you actually create the Lambda function.

#### VPC Assignment

Determine the **VPC and Security Group** to assign to the Lambda. Since your Redis instance is most probably only visible within a VPC 
(and it really ought to be), the Lambda needs to be in a VPC as well. 

In the past, instantiation of Lambda functions within a VPC incured a heavy performance penalty on cold starts, 
but this is no longer the case. The network setup only runs once, and you pay no penalty later. 
It is highly recommended assigning all available subnets in the VPC to the Lambda (typically in multiple AZ's), thus allowing it to
run as many instances as it needs with minimal latency.
   
#### S3 Access

1. Unlike VMs, **Lambda functions running inside a VPC need a special gateway for routing requests to S3**. In the VPC defined for the Lambda,
create a new [Endpoint Gateway](https://console.aws.amazon.com/vpc/home#Endpoints:sort=vpcEndpointId) for S3.
   
2. Create an IAM Role for the Lambda function, holding two policies:
   1. The AWS-managed policy `AWSLambdaVPCAccessExecutionRole` which any Lambda requires for running in a VPC.
   1. A policy allowing read-only access to the bucket/s holding the dataset. 
      Generally, avoid using AWS-managed policies for S3 access as-is without further limiting access to only the needed buckets.

### Packaging the Lambda Layer and Function

[Packages required by Funnel Rocket](../requirements.txt) far outweigh the size of the project codebase, and that list changes only infrequently. 
For that reason, dependencies are packaged as a [Lambda Layer](https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html)
which is essentially equivalent to intermediate layers of a Docker image. The Lambda layer needs to be recreated only when dependencies change.

Some of these packages rely on native libraries. which must match the OS and architecture of the Lambda runtime environment.
Thus, the layer packaging process runs within a Docker container with the same base image used to locally test Lambda functions.

To create .zip files for both the lambda layer and the function, run:
```
./build-lambda.sh --layer
```
This script builds `lambda-layer-<commit-hash>.zip` and `lambda-function-<commit-hash>.zip`, and reports on the file sizes -
which must AWS Lambda's limits.

When the project source code changes, use `./build-lambda.sh` without any flags to only rebuild the function .zip file.

### Deploying the Function through the AWS Console UI

#### Creating a New Function

1. Start [creating a new Lambda function](https://console.aws.amazon.com/lambda/home#/create/function).
1. Under **Basic Information**:
   1. **Function name:** Use the name 'frocket' by default. 
      If you choose another name, set the environment variable `FROCKET_INVOKER_LAMBDA_NAME` to that name on the API Server side.
   1. **Runtime:** Python 3.8 or later.
1. **Permissions:** click *Change default exection role*, then choose *Use an existing role* and select the IAM role created above.
1. Under **Advanced Settings** choose the appropriate VPC and all of its subnets. Choose an appropriate Security Group having access to Redis.
1. Click *Create function*. This takes a bit.

#### Configuring the Function

1. [Create a new layer](https://console.aws.amazon.com/lambda/home#/layers). Name it however you want, set Python 3.8 
   as the compatible runtime, and upload the locally packaged `lambda-layer-<x>.zip` file.
1. Going back to the Lambda function, add the new layer to the function. Assuming you're now in the *Code* tab, 
    scroll down to the *Layers* section to add a new custom layer, choosing the one you've just created.
1. Scrolling back to the top, in the *Code source* section click on *Upload from* to upload the local `lambda-function-<x>.zip` file.
1. One section down in *Runtime settings*, click *Edit* and set the handler function to `frocket.worker.impl.aws_lambda_worker.lambda_handler`.
1. Switch to the *Configuration* tab:
   1. Go into *Basic settings*. To get one full vCPU allocated to each function instance set the memory to 1768mb, since [the portion of vCPU you get
is dependent on allocated memory](https://epsagon.com/observability/how-to-make-aws-lambda-faster-memory-performance). This also provides enough headroom for loading Parquet files. 
   1. Set the *timeout* to 30 seconds or more. The default value is super short.
   1. As the *execution role* choose the role you've created above (the one with S3 and Lambda execution policies).
   1. Go into *Asynchronous invocation*, and set 'Retry attempts' to zero. The API Server already has worker type-agnostic
      retry mechanism implemented, and retries at the Lambda service level can only interfere with that. 
   1. Go to *Environment variables* and add the variable `FROCKET_REDIS_HOST`, setting it to the Redis server full hostname (without port number).
      With Elasticache, that's the *Primary Endpoint* address.
   1. The default log level is `info`, configurable via the environment variable `FROCKET_LOG_LEVEL`. Standard Python levels apply.

### Running and Looking at Logs

Make sure to have your first dataset ready in S3. You can use the one from the [example dataset walkthrough](./example-dataset.md) for starters.

Start by listing datasets, either by [calling the API Server](api.md) or using the CLI directly (as elaborated in the walkthrough) 
from a shell on the API Server machine. There should be no datasets yet, but this is a good sanity check to see the Funnel Rocket successfully connects to Redis.

Then, register that dataset through the API or CLI. That depends on permissions and configuration being correctly set.

Closely follow the logs of both the API Server and the Lambda Function. For the Lambda, logs are automatically collected into CloudWatch and 
are accessible in the AWS Console under the *Monitor* tab in the Lambda's main page.

List the registered datasets again, to see that yours appears. You can now start querying it, perhaps first with an empty query for sanity to see that all files are loaded ok.
### Cost

When using Lambda workers, cost estimation is returned as part of the response for queries, as the `stats.cost` attribute.

Cost estimation is not available with Redis queue-based workers.

## Metrics

Running API Servers expose a `/metrics` endpoint by default, for scraping by Prometheus.
This can be disabled by setting the variable `FROCKET_METRICS_EXPORT_PROMETHEUS=false`.

Since serverless workers are ephemeral and anonymous in nature, they are not a good scrape target for Promethus,
and thus do not expose a `/metrics` endpoint of their own. 
Instead, the API Server collects all metrics from workers as part of the request/response cycle with workers,
and exposes these metrics on the workers' behalf, with the proper labels and values per each task that has run.

**TBD:** Allow running this enpoint in a separate port; document main metrics here.

## Configuration Reference Guide

**Funnel Rocket does not use configuration files at all. All needed configuration is done via environment variables.**

This design decision was made in light of variables being the most common, straightforward way to configure components in
containerized and serverless environments. On the other hand, generating config files for automated deplyoments tends to
be cumbersome from our experience. This not really about ideaology ("no more headphone jacks!") as much as an attempt to streamline things.

Hopefully, defaults are generally good enough that you'll only need to set very few variables - mostly around hostnames and keys.

Variable | Default Value | Description
-------- | ------------- | -----------
`FROCKET_LOG_FILE`|*None*|If set, logs are written to the specified file. If not, logs are written to stdout. 
`FROCKET_LOG_LEVEL`|info|Set to either: debug, info, warning, error.
`FROCKET_LOG_FORMAT`|%(asctime)s %(name)s %(levelname)s %(message)s|In Python logger format. **NOTE:** In Lambda functions, the log format is fixed and cannot be set (so it doesn't interfere with the Python runtime logging). 
`FROCKET_DATASTORE`|redis|Implementation of the datastore (datasets metadata and short-lived request/response infomation). Only a Redis implementation is currently implemented.
`FROCKET_BLOBSTORE`|redis|Implementation of the blobstore (larger short-lived binary objects). By default, both the datastore and blobstore share the same settings. However, they could be configured to use a separate host, port or logical db.
`FROCKET_REDIS_HOST` `FROCKET_DATASTORE_REDIS_HOST` `FROCKET_BLOBSTORE_REDIS_HOST`|localhost|Set `FROCKET_REDIS_HOST` to configure the hostname for both datastore & blobstore. Alternatively, configure only a specific role with `FROCKET_DATASTORE_REDIS_HOST` or `FROCKET_BLOBSTORE_REDIS_HOST`.
`FROCKET_REDIS_PORT` `FROCKET_DATASTORE_REDIS_PORT` `FROCKET_BLOBSTORE_REDIS_PORT`|6379|Set the port either for both datastore & blobstore, or specifically per each one.
`FROCKET_REDIS_DB` `FROCKET_DATASTORE_REDIS_DB` `FROCKET_BLOBSTORE_REDIS_DB`|0|**Not supported on clustered Redis**. The Redis logical DB to use.
`FROCKET_DATASTORE_REDIS_PREFIX`|frocket:|All keys written to the Redis datastore have this prefix.
`FROCKET_BLOBSTORE_DEFAULT_TTL`|600|The default expiry in seconds of blobs written to the blobstore. Should be more than the maximum duration of jobs using blobs.
`FROCKET_BLOBSTORE_MAX_TTL`|86400|The maximum expiry of blobs, in seconds.
`FROCKET_AWS_ACCESS_KEY_ID` `FROCKET_LAMBDA_AWS_ACCESS_KEY_ID` `FROCKET_S3_AWS_ACCESS_KEY_ID` |*None*|If empty, boto3 will [perform its standard lookup of the environment, config files and instance metadata](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials) for AWS credentials. If credentials are not found in either of these locations, or to override them, use this variable. You can also set a per-service (LAMBDA/S3) key, which is useful when connecting to alternative implementations/mocks.
`FROCKET_AWS_SECRET_ACCESS_KEY` *(or per service)*|*None*|See above.
`FROCKET_AWS_ENDPOINT_URL` `FROCKET_LAMBDA_AWS_ENDPOINT_URL` `FROCKET_S3_AWS_ENDPOINT_URL`|*None*|Leave this empty by default. To connect to services which are compatible with or mock AWS services (currently, only S3 and Lambda are used), set the appropriate value.
`FROCKET_AWS_NO_SIGNATURE` *(or per service)*|false|Only set to true for some mock service implementations.
`FROCKET_LAMBDA_AWS_MAX_POOL_CONNECTIONS`|50|Determines the actual concurrency of Lambda function invocations. See boto3 Config object documentation for more.
`FROCKET_LAMBDA_AWS_CONNECT_TIMEOUT`|3|
`FROCKET_LAMBDA_AWS_READ_TIMEOUT`|3|
`FROCKET_LAMBDA_AWS_RETRIES_MAX_ATTEMPTS`|3|
`FROCKET_INVOKER`|work_queue|The method which the invoker (API Server or CLI) will use to launch tasks: either through a Redis queue (with the default setting of *work_queue*), or by invoking Lambda functions (set to *aws_lambda*).  
`FROCKET_INVOKER_RUN_TIMEOUT`|60|How long in seconds the invoker will wait for all tasks to complete before failing the job, including any retries.
`FROCKET_INVOKER_ASYNC_POLL_INTERVAL_MS`|25|How frequently, in milliseconds, the invoker will poll Redis for task updates.
`FROCKET_INVOKER_ASYNC_LOG_INTERVAL_MS`|1000|How frequently, in milliseconds, the invoker will write tasks progress to log (at *info* log level).
`FROCKET_INVOKER_LAMBDA_NAME`|frocket|The name of the Lambda function to invoke.
`FROCKET_INVOKER_LAMBDA_THREADS`|20|How many threads to use for concurrently invoking Lambda function. More threads take considerably more CPU resources, and improve performance but typically only to a point (depending on your machine). The max concurrency limit of calls to AWS is limited at the boto3 level by `FROCKET_LAMBDA_AWS_MAX_POOL_CONNECTIONS` (see above).
`FROCKET_INVOKER_LAMBDA_DEBUG_PAYLOAD`|false|Whether to log all task request payloads when log level is *debug*.
`FROCKET_INVOKER_LAMBDA_LEGACY_ASYNC`|true|When *true*, the API Server will use the officially-deprecated `InvokeAsync` AWS API for invoking Lambdas. When *false*, it will use the standard `Invoke` API with an *Event* payload type. Currently, the legacy function returns much faster, so it is used by default while it is operational.
`FROCKET_INVOKER_RETRY_MAX_ATTEMPTS`|3|Maximum attempts the invoker will perform for a specific task index. Retries are perfoemed for tasks that have either reported a failure, or are considered lost/stuck.
`FROCKET_INVOKER_RETRY_FAILED_INTERVAL`|3|How much time, in seconds, to wait from the time a task has explicitly failed till retrying it.
`FROCKET_INVOKER_RETRY_LOST_INTERVAL`|20|If a task has not ended (either successfully or not) and has not updated its status for this amount of seconds, it is considered 'lost' and a retry would be attempted. You should set this to a value that is significantly above the duration in which tasks typically complete in your system, but it should be no more than half the value of `FROCKET_INVOKER_RUN_TIMEOUT`,  so the retried attempt has a fair chance to complete. If a 'lost' task attempt ends up completing ok, its results can be used. If multiple attempts end up completing for the same task index, Funnel Rocket makes sure to use only the results of one of these attempts.
`FROCKET_WORKER_REJECT_AGE`|60|Workers will reject task requests which are older than this, in seconds. This should be in sync with `FROCKET_INVOKER_RUN_TIMEOUT`.
`FROCKET_DATASET_CATEGORICAL_POTENTIAL_USE`|true|Whether to load string columns which are found to be good candidates for categorical type, as such. Note that string columns already marked as categoricals in Pandas-created Parquet files are always be loaded as such.
`FROCKET_DATASET_CATEGORICAL_RATIO`|0.1|String columns where the ratio of *unique values-to-column length* is lower than this value are candidates to be loaded as [categorical type](https://pandas.pydata.org/pandas-docs/stable/user_guide/categorical.html), to conserve memory and make searches faster. Whether such columns are loaded as categoricals is controlled via `FROCKET_DATASET_CATEGORICAL_POTENTIAL_USE`.
`FROCKET_DATASET_CATEGORICAL_TOP_COUNT`|20|The size of the *top values* list which is stored in the dataset metadata for each categorical column (as part of a dataset's full schema). Set to zero to disable.
`FROCKET_DATASET_CATEGORICAL_TOP_MINPCT`|0.01|For values to be included in the *top values* list, their frequency of the total should be equal or higher than this value (by default, 1%).
`FROCKET_VALIDATION_SAMPLE_RATIO`|0.1|When registering datasets in validation mode `sample`, this is the ratio of files to validate (10% of total file count by default, but never less than 2 if the no. of files in the dataset >= 2).
`FROCKET_VALIDATION_SAMPLE_MAX`|10|The maximum number of files to validate in `sample` mode, regardless of the above ratio setting.
`FROCKET_WORKER_SELF_SELECT_ENABLED`|true|Whether to let workers select the dataset parts they wish to work on, from a set published by the invoker. This allows workers to select parts already cached locally, if any.
`FROCKET_PART_SELECTION_PREFLIGHT_MS`|200|In *worker self select* mode, the duration after invocation of the job started in which only workers wishing to select a specific cached part to process can do so. Random selection is only allowed after that time. Set to zero to disable this behavior.
`FROCKET_WORKER_DISK_CACHE_SIZE_MB`|256|The maximum of size of the local on-disk cache (always under the temp directory). Note that some local storage space in the temp dir is always needed even if cache size is zero - as remote dataset files are downloaded to disk before loading them. The actual space needed is: cache size + size of last downloaded file.
`FROCKET_AGGREGATIONS_TOP_DEFAULT_COUNT`|10|For aggregation that return a map of values to number (e.g. *countPerValue*, *groupsPerValue*), a limit on how many values to return.
`FROCKET_UNREGISTER_LAST_USED_INTERVAL`|30|Attempting to unregister a dataset will fail if this time interval has not yet passed since it was last queried. In seconds. Use zero to disable this protection. If not set, the default is `FROCKET_INVOKER_RUN_TIMEOUT` * 2. Override when unregistering by adding a `force=true` URL parameter.
`FROCKET_STATS_TIMING_PERCENTILES`|0.25, 0.5, 0.75, 0.95, 0.99|When jobs return, the reponse includes a `stats` object. For stats data returned in percentiles, this is the list of percentiles to return. If the number of samples is low, some percentiles may not be returned.
`FROCKET_APISERVER_ADMIN_ACTIONS`|true|Whether the API Server should allow registering/un-registering a dataset, and any other actions which modify data or metadata.
`FROCKET_APISERVER_ERROR_DETAILS`|true|Whether the API Server should return any error detail is has in responses, or only return errors specifically marked as non-sensitive. Any unexpected errors are not returned in the latter mode, but only a generic error text.
`FROCKET_APISERVER_PRETTYPRINT`|true|Whether to pretty-print JSON responses
`FROCKET_METRICS_EXPORT_PROMETHEUS`|true|Whether to expose a `/metrics` URL endpoint for Prometheus to scrape
`FROCKET_METRICS_EXPORT_LASTRUN`|*None*|Set to a filename to have the metrics of the most recent run written. If the file extension is `.parquet`, the file is saved in Parquet format. Otherwise, it is saved as CSV. By default, no file is written to.
`FROCKET_METRICS_BUCKETS_SECONDS`|0.05, 0.1, 0.5, 1, 2, 5, 10, 15|Which buckets to use for histogram metrics ending with `_seconds`
`FROCKET_METRICS_BUCKETS_DOLLARS`|0.01, 0.05, 0.1, 0.5, 1, 2|See above
`FROCKET_METRICS_BUCKETS_BYTES`|1048576, 16777216, 67108864, 134217728, 268435456|See above. These values represent 1MB, 16MB, etc.
`FROCKET_METRICS_BUCKETS_GROUPS`|100, 1000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 500_000_000|Buckets to use for metrics measuring unique groups
`FROCKET_METRICS_BUCKETS_ROWS`|100, 1000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 500_000_000|Buckets to use for metrics measuring row counts
`FROCKET_METRICS_BUCKETS_DEFAULT`|0.1, 0.5, 1, 5, 25, 100, 1000|Which buckets to use for histogram-type metrics if the metric name suffix does not match any of the above.

## Limitations

1. Funnel Rocket **does not support joins** between datasets. De-normalize your datasets to include any relevant fields 
you wish to query - as done for the [example dataset](./example-dataset.md).
1. **A maximum of 1,000 files per each dataset** in S3 (TBD: pagination when listing files in S3).
1. **No support for nested data:** Funnel Rocket currently has no support for nested data, 
   though we're experimenting with how to allow certain nested conditions efficiently.
