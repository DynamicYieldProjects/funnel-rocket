# Operations Guide - TBD

## Unorganized

## Dataset guidelines

5. Funnel Rocket **does not support joins** between datasets. De-normalize your datasets to include any relevant fields 
you want to be able to query. **TBD** refer to the product feed example

Maximum of 1,000 files per dataset. **TBD: guidance on file size limit, e.g. 256mb?** (depends on usage)

Ideally, you should set the number of parititions so that resulting files are within 20-100 MB per each. Having many small
files would utilitize a large number of workers for diminishing returns in performance. Having large files would make 
querying slower as each file is processed by a single task, and may cause OutOfMemory crashes if memory is too tight.
**TBD link to Lambda/worker suggested RAM settings**

6. **Support for nested data:** Funnel Rocket currently has only limited support for nested data, unlike Spark DataFrames.

6.1 **TBD** implement and documents: bitset columns for a limited set of values (up to 512, non-repeating)

6.2 **TBD** which operators can work on other string/numric lists out of the box?

### Data Format Best Practices

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

## Running Distributed Queries in Production
Go to operational guide....

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

## Configuration guide


