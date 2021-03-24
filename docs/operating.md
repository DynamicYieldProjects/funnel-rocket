# Operations Guide

## Running in Production

The most important operational decision to make is: whether you'd like to go the serverless route for the worker processes (with AWS Lambda),
or have long-running worker processes that get their work via Redis. This is mostly a question of cost:

* If there's a never-ending stream of query requests with more or less the same rate throughout the day (or for a known time window - 
  e.g. nightly batch processes), Redis-based workers may make sense - especially when running on cheap spot instances. Since these workers
  need no centralized master or load balancing, and the API server takes care of retries for whatever reason, it's ok for workers to simply 
  disappear in a puff of smoke from time to time.

* In a typical on-demand workload, serverless can be the cheapest and best performing option - scaling up quickly to serve requests, 
  without paying for long-running infra.

Whether you choose the former or the latter, you will need to have:
1. **A Redis database**.
1. **An API Server**, preferably more than one for high availability.

**Both API Servers and Workers must have access to Redis and access to the dataset files** (through S3 or other shared FS).
There is no direct communication between the two components - all requests are done via Redis, and in the case of using Lambdas also
the AWS API.


### Redis

Running Redis in production can be done in multiple ways. In AWS, one of the easiest ways is to use Elasticache. 
* It's recommended to have about at least 500 MB of available RAM in Redis. 
* Any of the cache.t3 instance types on offer will suffice, but having at least one replica is recommended, in a multi-AZ setup. 
* In general, avoid using cache.t2 instances due to their weak network performace. 

The Redis port (6379 by default) should be accesible to (a) the Lambda function (if you're using Lambdas), (b) the API server,
and (c) any Redis-based workers.

The port, host and (optionally) logical DB of the Redis server are configured via `FROCKET_REDIS_HOST`, `FROCKET_REDIS_PORT` and
`FROCKET_REDIS_DB`. There is more fine-grained control over these settings documented in the configuration reference below.

### Running the API Server

#### Permissions

##### 1. Redis

Make sure the node has network access to your Redis instance in port 6379, whether both are in the same VPC and Security 
Group or not (using `telnet <redis-host> 6379` is a simple way to check).

##### 2. Invoking Lambdas

If you're using Lambda-based workers, give the API Server a permission to invoke Lambda functions (but not otherwise modify them).
The server should be given a `lambda:InvokeFunction` IAM permission to the specific function, by its ARN.

##### 3. Remote Storage

The API Server nodes should have a `s3:ListBucket` IAM permission to the bucket/s where datasets reside.

Generally, there are two ways to grant service access permissions in AWS:
1. Using an IAM role.
1. Using access key & secret of an IAM user. Since Funnel Rocket uses the standard boto3 package3, 
   [any credentials setup that is respected by boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) would work. However,
   you can set or override credentials by using Funnel Rocket-specific configuration varibles (see below). 
   This allows you to specify different keys for specific AWS services, and make sure no other component inadvertantly uses them.
   
You are encouraged to limit the IAM permissions as much as possible. The API Server does not need read permissions (`s3:GetObject`),
and certainly not any permission to modify objects or buckets. It does not need permission to access irrelevant buckets.

In future releases, we plan to delegate dataset file listing to workers as well - meaning that the API Server itself will not need *any* 
permissions to remote storage. Any optional features for transforming datasets would necessitate giving limited write permissions
to workers (but not the API Server) to write a modified copy of the data but not replace it.

#### Using the Docker Image

This is the simplest option (though providing k8s templates is still TBD). Take a look at `docker-compose.yml` as an example.

Use the image **frocket/all-in-one:latest** which can run either as an API Server or a Redis-based Worker, 
depending on the given command: `apiserver` or `worker`.

When running as API Server, the container runs a Flask application with a [gunicorn](https://gunicorn.org/) server. 
1. The **default port is 5000**. This is configurable by setting the env. varible `APISERVER_PORT` for the container.
1. gunicorn is currently configured to use **multiple processes - 8 by default** (multi-threaded mode is not yet well-tested).
      This is configurable with the variable `APISERVER_NUM_WORKERS`. 
1. Each worker uses about 50-70mb of RAM, so you can comfortably fit a default 8-process API server in 1GB of RAM
1. If you're going for the serverless worker path, allow the container *at least* 2 CPUs, with a higher limit (4+).
   Mass invocation of Lambda functions on each new query is executed in parallel through a thread-pool, and is quite resource-hungry 
   (TBD: improve this mechanism). When using Redis-based workers, a single CPU is enough for the API Server.
1. Have at least 2 API Servers running for high-availability. There is nothing you need to do for orchestrating these. 
   Each will launch its own requests with unique identifiers, while dataset registration/unregistration in Redis is atomic.

#### Via PyPI

If you'd like to install & run Funnel Rocket yourself:
1. Ensure you have Python 3.8+ installed.
1. Install the code & all dependencies via `pip install funnel-rocket`. Note that since some dependencies include binary libraries (e.g. NumPy),
   using dependencies that were installed in a different OS and copied over as-is may cause processes to crash.
1. To run the API Server: `python -m gunicorn.app.wsgiapp frocket.apiserver:app --bind=0.0.0.0:<port> --workers=<num-processes>`.
   You can use other WSGI servers, but note that not all necessarily support *HTTP streaming (chunked transfer encoding)* 
   which is an optional feature the API Server can use to send progress updates.
   
See below for all configuration options.

### Running Redis-Based Workers

This is quite similar to running an API Server:

Using the **frocket/all-in-one:latest** Docker image, run the container with the `worker` command.

To run as a regular process, `pip install funnel-rocket` in a Python 3.8+ environment (see above) and run:
```
python -m frocket.worker.impl.queue_worker.py
```
The process will connect to the configured Redis server (see configuration reference below) and start taking on tasks.

#### Compute requirements

1. **Allow each worker to have 1 full CPU**, or you'll get decreased performance. Workers are essentially single-threaded,
   so you won't generally get improved performance from having multiple CPUs. The PyArrow Parquet driver can make use of multiple threads when loading files, 
   so limiting the container's CPU at a bit more than one CPU does make sense.
1. **The amount of RAM allocated should be 10x+ the typical size of a single Parquet file** in your datasets. This
   is because Parquet files are usually well compressed while the in-memory layout of Pandas can get quite large. 
   The actual amount needed varies greatly by the number of columns used in a query and their types.
1. **Have at least 0.5GB of storage available in the tmp directory**. The minimal amount should cover downloads of Parquet files 
   from remote storage. Extra space would be used for locally caching files with a simple LRU mechanism.

## Running AWS Lambda-Based Workers

### No Automation Yet
Currently, we do not offer an automated method for deploying Funnel Rocket as a Lambda function but only the packaging of files.

This is because in production there are various things to get right (Redis, networking, security) which are outside the scope of
the function itself, and need to be handled with care. Terraform is not a good tool for this specific need at this time, 
and serverless frameworks solve for some of that yet mostly assume admin-level permissions.

We'll continue to look at options, including AWS SAR (Serverless Application Repository) and using AWS SAM/Serverless Framework
at least for dev/staging accounts.

Ideas, code and non AWS-specific implementations are welcome.

### Setting the Environment

#### VPC Assignment

Determine the **VPC and Security Group** to assign to the Lambda. Since your Redis instance is most probably only visible within a VPC 
(and it really ought to be), the Lambda needs to be in a VPC as well. 

In the past, instantiation of Lambda functions within a VPC incured a heavy performance penalty on cold starts, 
but **this is no longer the case**. The network setup only runs once, and you pay no penalty later. 
It is highly recommended to assign all available subnets in the VPC to the Lambda (typically in multiple AZ's) so it can 
run as many function instances as it needs.
   
#### S3 Access

Unlike VM's running in a VPC, Lambda functions and some other need a special 
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

### Packaging the Lambda Layer and Function

### Deploying the Function

config variables, cloudwatch and metrics

### Looking at Logs

## Configuration

## Limitations

1. Funnel Rocket **does not support joins** between datasets. De-normalize your datasets to include any relevant fields 
you wish to query - as done for the [example dataset](./example-dataset.md).
1. **Maximum of 1,000 files per dataset** in S3 (TBD: pagination when listing files in S3).
1. **No support for nested data:** Funnel Rocket currently has no support for nested data, 
   though we're experimenting with how to allow certain nested conditions efficiently.

==============


### Running an Invoker




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

## Configuration guide


