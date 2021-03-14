<p align="left">
  <a href="#">
    <img src="https://github.com/DynamicYieldProjects/funnel-rocket-oss/workflows/Tests/badge.svg" />
  </a>
</p>

![Alt Funnel Rocket Logo](./docs/logo.png)
<p align="center">
  <img src="./docs/logo.png" width="348" height="172">
</p>

# Funnel Rocket

## What's This About?

Funnel Rocket is a query engine that was purpose-built to efficiently run a specific type of query:

Given large datasets of user activity (pageviews, events, clicks, etc.), find the users whose activities meet a specific
condition set, optionally with a **specific order of events and time constraints**, and return various aggregations over
the matching group. It can also perform a full funnel analysis, in which user counts and aggregations are 
returned for each step in the sequence.

This is not a rare need for website and app operators, yet is still a challenge to do with standard tools in terms of 
compute needs, time to run and cost. With current SQL and NoSQL databases (choose your poison), the queries you'd need to 
write are relatively complex and prone to bugs. Another route you may take
is through batch processing, limiting you "baked" queries that cannot be tweaked ad-hoc by the end user.

To more exactly frame the challenge: first, you need to perform a very high cardinality grouping first (dozens of millions 
of groups or more, each one being for a single user), and then run multiple passes over each group to execute all conditions 
in the desired order.

While datasets do need some preparation to be efficiently queryable, we've tried to minimize the effort needed - read
on below for more. 

Though the original aim was very specific (to replace an aging solution), we've found that the codebase can be easily
extended to perform many more use-cases focused on large scale user-centric data processing (it doesn't really need to be users,
of course) - and do so in a fashion that's very fast to scale, with low resource overhead, little management and lower cost. 

## The Technology Used, or: Clusters are Hard, and Cloud Native isn't (only) a Buzzword

This tool is basically bringing together a few excellent, proven components that do all the heavy lifting:

### 1. Pandas
The concept of the *DataFrame* doesn't need much introduction, and allows for runnning complex transformations at ease with
good performance (if you're mindful enough). Coupled with Apache Arrow (also by the industrious @wesm) you also get great Parquet support. 
Pandas itself is a library running within a single process, but tools such as Dask and PySpark have brought either the library itself 
or its core abstractions to the distributed domain. 

However, operating distributed cluster for data processing engines gets pretty tricky as you grow. Deploying, scaling and fixing the 
inevitable periodic issues can get very time-consuming. When something breaks, it can be hard to tell what's going on. As data and needs grow, 
virtually any technology would reach some unexpected size/load threshold and  start performing poorly or behaving erratically.

Scaling usually leaves a lot to be desired: the cluster either has "too much" resources sitting idle, or not enough to handle
temporal load spikes. That translates into a lot of money at scale. There's no no silver bullet, of course, yet we can take a stab at the problem from another angle:

### 1. Serverless
(currently supporting AWS Lambda, other integrations welcome)

People tend to be split on serverless, for a bunch of reasons right and wrong. What we've found with AWS Lambda (a bit to our surprise
as well!) is a mature, reliable and (yes) fast enough service which can scale to hundreds or thousands of cores almost immediately. 
The price per GB/second (or vCPU/second) is indeed higher in this model, but since you pay only for "actual work" done, it is very fitting for 
bursty on-demand jobs - in our case, queries. You also spend relatively very little time on operations.

For a tool that's measured in single digit seconds rather than milliseconds, they turned up to be good enough. If you're seeing the users
of your web UI progressively tweak their queries, warm start and some smart data caching go along way to speed up things further. Plus,
you can always 'pre-bake' some default/common queries beforehand using the non-serverless mode - see below.

Funnel Rocket uses the asynchronous Lambda invocation API, making it much easier to launch hunderds+ of jobs quickly.
Your invocation reqeusts are put into a queueing mechanism, which adds no meaningful latency in normal operation, yet prevents most cases of rate limiting.

Of course, having multiple distributed jobs and tasks in flight, handling retries, etc. still takes some management infrastructure. 
Luckily, there's Redis.

### 3. Redis, and the Lightweight Cluster Option
The versatility of Redis data structures makes it a natural choice for handling lightweight metadata, work queues, real-time 
status tracking and more. There is a range of managed offerings to choose from which won't break the bank, as this use case 
only requires a modest amount of RAM.

Other than for managing metadata on datasets, Redis is used in two more ways:

1. For tracking and storing the status and outputs of all individual tasks, since the invoker (server) does not rely on synchronous responses.

2. Optionally, to **support a non-serverless deployment option where Redis also acts as a work queue** from which 
long-running worker processes fetch tasks. 

This latter option is a pretty easy to set up: each worker is a simple single-threaded process which anonymously 
fetches work from a shared queue, with no additional cluster management or load balancing required. The underlying Redis
list is in effect "the manager", with processes taking tasks off the list at their own pace, based on what scale you currently have.

A good use for this deployment option is for pre-baking the "default" queries that end-uses see when they login to your web UI: you can
schedule a nightly process which scales up nodes, runs all such queries on cheap spots, store the results and scale down. That
way, you only utilize lambdas when users start exploring beyond the default view.

Both deployment options push much of the complexity into battle-tested tools. Both depend on Redis as their single stateful component.
Thus, running a muti-zone Redis setup is recommended in production. In the worst case, you'd need to re-register all active datasets.

## Preparing Data for Querying

Funnel Rocket currently supports Parquet files only, located under the same base path either on a locally-mounted filesystem
or in S3 and compatible object stores (e.g. MinIO). **TBD:** add more storage systems and file formats.

### Mandatory columns

1. A **group ID** column: a string or numeric column with the user ID / user hash / other group ID, with no null values. 
   All query conditions are performed in the scope of each group. The column name is set by the client, per dataset.

2. A **timestamp** column: either int or float, typically a Unix timestamp in the granularity of your data (e.g. int64 of seconds
   or milliseconds, float of seconds.microseconds, etc.). Currently, Funnel Rocket does not impose a format, as long as it's consistent.

### Partitioning by the Group ID column

For Funnel Rocket to be fast and simple, the data should be organized so that each file includes a unique set of users, 
with all rows for a specific user located in the same file. This means an expensive shuffle step can be avoided at query time. 

Assuming your dataset is not already partitioned that way, you can use (Py)Spark or similar tools to perform it. 
For example, with PySpark call [DataFrame.repartition()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.repartition.html)
and save the resulting dataset.

A standalone utility for data repartitioning is [included in this repo](https://github.com/DynamicYieldProjects/funnel-rocket-oss/blob/main/dataprep_example/repartition.py).
It is non-distributed but can use all available CPUs, so you can prepare datasets up to a reasonable size on larger machines.

Aim to have files size in the range of 20-150mb. 

See the [Operations Guide] for more (https://github.com/DynamicYieldProjects/funnel-rocket-oss/docs/operating.md)

## Components


**TBD chart...**

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
* Funnel Rocket can be built as a docker image by running: `docker build -f docker/Dockerfile . -t dynamicyield/frocket`
* Run `docker-compose up`, to build and start a `frocket` worker service running on docker with needed dependencies, such as redis.
* Run `docker-compose up --scale funnel-rocket-worker=2` to start multiple workers. 
 
### Unit tests
1. Install test dependencies: `pip install -r requirements/test.txt`
2. Run tests: `pytest`
3. Run test with coverage: `pytest --cov=frocket --cov-report=html`
    3. Coverage reports are available locally under `./htmlcov/index.html`
 
### tutorial link...

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

## Local development


## Cost Estimation - TBD (feature needed)

## Tech Notes - TBD

### Performance & Caching - TBD
Explain how caching works in warm start

### Lambda Limits - TBD
Lambda limits (concurrency, memory/CPU, storage, network)
per region / provisioned

## FAQ - TBD

Q: Are there no other established tools to perform such queries?
Q: What happens if Redis fails?
