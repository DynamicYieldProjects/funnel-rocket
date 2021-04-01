# Guide: Creating and Querying an Example Dataset

There are only a few public datasets in the domain of user activity, and those that do exist are naturally very obfuscated - 
which is good enough for stone-cold algorithms but less for human learning.

To give you a taste of using Funnel Rocket, here are the steps to download, prepare and query a public e-commerce dataset from Kaggle.

Before we start, a note on semantics: Funnel Rocket is typically used to query user data, as is the case with the example data here. However, 
the more generic term Funnel Rocket uses is *groups*: any kind of grouping ID common to one or more rows which determines the granularity of queries.

## Preparing the Dataset

### Creating an optimized Parquet file

* Download the [dataset](https://www.kaggle.com/retailrocket/ecommerce-dataset/) from Kaggle and extract the files to a directory of your choosing. 
  A free registration is required.
* From the Funnel Rocket repo directory, run `python dataprep_example/ingest_retailrocket_dataset.py <dataset-files-dir>`.

The above script will:
1. Read the approx. 2.5 million user events in `events.csv`
1. Load the auxiliary product properties files (`item_properties_part1.csv` and `item_properties_part1.csv`) and inner-join them with the user events,
   based on the `itemid` column. This de-normalizes user events data so that product attributes can be queried without needing joins, which are unsupported.
1. Convert a few columns to a more efficient data type, e.g. 
   1. Make `available` a boolean
   1. Convert what looks like the price column to float
   1. Last but not least: cast a few string fields to [categorical](https://pandas.pydata.org/pandas-docs/stable/user_guide/categorical.html).
      The script stores the mysterious '888' column (which looks like product keyword IDs) both as a string column (`cryptic_attrs`) and as a categorical (`cryptic_attrs_cat`), 
      so you can note differences in loading performance and memory usage between the two.
1. Save the result as `retailrocket.parquet` in the given directory. Not all columns are brought over.
1. Print out the schema of the result file, plus some statistics on the performance of loading the Parquet file and memory usage as a DataFrame.

### Re-partitioning

The resulting `retailrocket.parquet` is a single 47.7mb file with 2.5 million rows. This is a good size to query as a single-part dataset, 
or as one part in a much bigger dataset. To experience a bit of parallelism, though, let's make it into an 8-part dataset.

Here's how to partition on the 'visitorid' column with the supplied utility `repartition.py`
```
% python dataprep_example/repartition.py --input <your-dir>/retailrocket.parquet --parts 8 visitorid
Input files found: 1, map output dir: map, target partitions: 8, CPUs: 4, pool size: 1
Map stage done, total of 8 files created
Reduce stage done, output parts are in directory: reduce

% ls -lh reduce/
total 172376
-rw-r--r--  1 elad  staff    10M Mar 18 12:20 part-00000.parquet
-rw-r--r--  1 elad  staff    10M Mar 18 12:20 part-00001.parquet
-rw-r--r--  1 elad  staff    10M Mar 18 12:20 part-00002.parquet
-rw-r--r--  1 elad  staff    11M Mar 18 12:20 part-00003.parquet
-rw-r--r--  1 elad  staff    10M Mar 18 12:20 part-00004.parquet
-rw-r--r--  1 elad  staff    10M Mar 18 12:20 part-00005.parquet
-rw-r--r--  1 elad  staff    11M Mar 18 12:20 part-00006.parquet
-rw-r--r--  1 elad  staff    11M Mar 18 12:20 part-00007.parquet
```

Another command-line tool which can come in handy with Parquet files is `parquet-tools`. This tool makes it easy to inspect the schema of the result files, along with a few other common actions. It can be installed via `pip`, `brew` or downloading the JAR file.

### Registering the Dataset

Let's register this fresh new dataset with the Funnel Rocket CLI utility (note: the CLI acts as a standalone job invoker, it doesn't talk to or need a running API server).
First, run `python frocket/cli.py --help` to see what commands the CLI supports 

Assuming that you're running a local Redis and queue-worker on the host, you can register the local files directly, as all running components on the host have access to local files.

To register our dataset, you need to specify *(a)* a logical name, *(b)* the path where the partitioned files are, 
and *(c)* which columns hold the group ID (user ID) and the timestamp.

Here's the command to run and the output you should get (omitting some of the response metrics):

```
% python frocket/cli.py register retail reduce visitorid timestamp
[Log INFO frocket.invoker.impl.registered_invokers] Creating invoker type: WorkQueueInvoker, for request builder type: <class 'frocket.invoker.jobs.registration_job.RegistrationJob'>
[Log INFO frocket.datastore.registered_datastores] Initialized RedisStore(role datastore, host localhost:6379, db 0)
[Log INFO frocket.invoker.jobs.registration_job] Number of part files: 8, total size 84.15mb
[Log INFO frocket.invoker.impl.async_invoker] Enqueued 2 tasks
[Log INFO frocket.invoker.impl.async_invoker] 50.0% done in 1.02s... ENDED_SUCCESS: 1, RUNNING: 1
[Log INFO frocket.datastore.registered_datastores] Initialized RedisStore(role blobstore, host localhost:6379, db 0)
[Log INFO frocket.invoker.jobs.registration_job] Dataset 'retail' successfully registered in RedisStore(role datastore, host localhost:6379, db 0)
[Log INFO frocket.invoker.invoker_api] Registration successful
API Result: {
  "success": true,
  "errorMessage": null,
  "requestId": "161606[...]",
  "stats": {
    "totalTime": 1.203,
    "invoker": {
      "enqueueTime": 0.001,
      "pollTime": 1.129,
      "totalTasks": 2
    },
    [...]   
    "dataset": {
      "totalSize": 88240183,
      "parts": 8
    }
  },
  "dataset": {
    "basepath": "reduce",
    "totalParts": 8,
    "id": {
      "name": "retail",
      "registeredAt": "2021-03-18T[...]"
    },
    "groupIdColumn": "visitorid",
    "timestampColumn": "timestamp"
  }
}
```

Here's what happened: 

1. Registering a dataset requires launching a job (comprised of tasks sent to workers for processing). In this case, the default `WorkQueueInvoker` class is used, which enqueues task requests via a Redis list. 
1. The default validation mode is `first_last`, meaning that tasks are launched for the first and last files in the dataset, by lexicographic order.
   1. Each task loads the file, ensure the existence and type required needed columns, builds the schema metadata and returns it.
   1. Tasks also return a compressed array of unique group IDs, indirectly through the 'blobstore' (also the same Redis, by default).
1. After tasks were enqueued the invoker goes into polling stage, waiting for all tasks to complete. In our case it should be no more than 1-2 seconds (if not, ensure the queue-based worker is running).
1. After tasks complete: 
   1. The job logic in the invoker will now compare the schemas coming back from individual tasks to ensure they match. 
   1. You'll see another connection to Redis established, but this time for the blobstore role. This is used to pull the unique group IDs lists stored by tasks.It will pull the list generated by the tasks, and make sure
      The invoker will ensure there's no overlap of group IDs between the sampled files. If there is any, it means the dataset is improperly partitioned.

### Registering with docker-compose

With the docker-based option, you'll need to:
1. Upload the files to a bucket in the MinIO container. Either use the MinIO Admin at http://localhost:9000 or `aws cli` (*TBD note the command here*).
2. Configure the CLI to connect to the Redis and MinIO containers instead of the real S3:
```
export FROCKET_REDIS_PORT=6380
export FROCKET_S3_AWS_ENDPOINT_URL=http://localhost:9000
export FROCKET_S3_AWS_ACCESS_KEY_ID=testonly
export FROCKET_S3_AWS_SECRET_ACCESS_KEY=testonly
```
3. Run `python frocket/cli.py register retail s3://<bucket-in-minio>/ visitorid timestamp`
   
### Listing registered datasets

Run:
```
% python frocket/cli.py list
[Log INFO frocket.datastore.registered_datastores] Initialized RedisStore(role datastore, host localhost:6379, db 0)
name                            registered at              parts  group id       timestamp    path
------------------------------  -----------------------  -------  -------------  -----------  --------------------------------------------------
retail                          2021-03-18...            8        visitorid      timestamp    reduce
```

## Running Queries

### The Empty Query

The CLI allows running an "empty" query with no conditions or aggregations, which is essentially the same as passing "{}" as the query string.

#### Run on the Host

Run:
```
% python frocket/cli.py run retail --empty
[Log INFO frocket.datastore.registered_datastores] Initialized RedisStore(role datastore, host localhost:6379, db 0)
[Log INFO frocket.invoker.impl.registered_invokers] Creating invoker type: WorkQueueInvoker, for request builder type: <class 'frocket.invoker.jobs.query_job.QueryJob'>
[Log INFO frocket.invoker.impl.async_invoker] Enqueued 8 tasks
[Log INFO frocket.invoker.invoker_api] Query completed successfully
API Result: {
  "success": true,
  [...]
  "query": {
    "matchingGroups": 1236032,
    "matchingGroupRows": 2500516,
    "aggregations": null
  },
  "funnel": null
}
```
Since no conditions were set, all 1.23 million group IDs are considered a match. The number of rows in these groups is 2.5 million, which is all rows in the dataset.

You can also run an empty query with `python frocket/cli.py run retail --string '{}'`. This would yield the same output.

### Run with the Mock Lambda container

In addition to the env. variables you've set in the above section 'Registering with docker-compose' for S3 and Redis, 
let's add the following to make the CLI invoke Lambda API calls to the mock Lambda container:
```
export FROCKET_INVOKER=aws_lambda
export FROCKET_LAMBDA_AWS_NOSIGN=true
export FROCKET_LAMBDA_AWS_ENDPOINT_URL=http://localhost:9001
export FROCKET_LAMBDA_AWS_REGION=us-east-1
export FROCKET_INVOKER_LAMBDA_LEGACY_ASYNC=false
```

Run the empty query via the CLI and you should get the same counts as above. 
However, when running it once again you'll note a few differences under `stats`:

* Our (single) Lambda instance now has all dataset parts in its local ephemeral cache, so it does not need to download them from S3 again.
```
   "cache": {
      "source": 0,
      "diskCache": 8
   }
```      
* Note the `cost` attribute which should look something like `"cost": 1.8166703e-06`. That's $0.0000018166703 USD. 
  
This number represents the approximate compute-only cost. 
* It is based on the total duration of all tasks run by the Lambda worker,  and the amount of memory allocated to it, in us-east-1 region rates.
* It does not include storage costs in real S3 (~$300 per TB/year) and per-request Lambda/S3 costs, which are in Funnel Rocket's case miniscule compared to the cost of compute. 

With real-life datasets **expect a cost of 0.5-2 cents per query** depending on its size and complexity.

## Running Some Basic Queries

### A Few Aggregations First

Before starting with any conditions, let's collect a bit of intel first. What types of user activities are in the dataset,
and how frequent are they? let's see what's the breakdown of values in the `event` column.

Save the following query as a file:
```
{
    "query": {
        "aggregations": [
            {"column":  "event"}
        ]
    }
}
```
Then, run the query: `python frocket/cli.py run retail --file <query-file>`.
The output looks like:
```
{
  [...]
  "query": {
    "matchingGroups": 1236032,
    "matchingGroupRows": 2500516,
    "aggregations": [
      {
        "column": "event",
        "type": "count",
        "value": 2500516,
        "top": null,
        "name": null
      },
      {
        "column": "event",
        "type": "countPerValue",
        "value": {
          "view": 2410035,
          "addtocart": 68499,
          "transaction": 21982
        },
        "top": 10,
        "name": null
      },
      {
        "column": "event",
        "type": "groupsPerValue",
        "value": {
          "view": 1232600,
          "addtocart": 37387,
          "transaction": 11569
        },
        "top": 10,
        "name": null
      }
    ]
  }
```
Note that we didn't specify any conditions yet (so all users are a match), and did not state what aggregation type we want for the `event` column.
This translates into the *default set of aggregations* which are:
1. *count*: how many rows have a non-null value in this column - all of them in this case. Good.
2. *countPerValue*: count of rows per each observed value in the `event` column.
3. *groupsPerValue*: count of how many unique groups (visitorid's) have each value of `event` in any of their rows.
  Simply put, 11,569 users have performed 21,982 transactions.

To now see *how much money* (or *potential money*) is associated with each event type, let's tweak the query a bit:
```
{
    "query": {
        "aggregations": [
            {"column":  "event"},
            {"column":  "event", "type": "sumPerValue", "otherColumn": "price"}
        ]
    }
}
```
The output will now include:
```
   [...]
      {
        "column": "event",
        "type": "sumPerValue",
        "value": {
          "view": 348450458.245,
          "addtocart": 7853984.856,
          "transaction": 2408968.092
        }
      }
```
This means that the total monetary value of purchases (transactions) is about one-third of the total value of products added to cart.

### Basic Conditions

Now that we have some high-level grasp of activities, users and monetary value in the dataset, let's do some conditions. 
First one is simple:
```
{
    "query": {
        "conditions": [
            {
                "filter": ["event", "==", "transaction"]
            }
        ]
    }
}
```
The output is:
```
  [...]
  "query": {
    "matchingGroups": 11569,
    "matchingGroupRows": 218927,
    "aggregations": null
  }
```
Here's the important thing to note: the `"matchingGroups"` value is the number of **unique group IDs** having one or more
rows with the `transaction` event - not the number of rows with *event == "transaction"*. We're all about users here. 

`matchingGroupRows` is the total rows these users have, regardless of any conditions. These are the rows that any aggregations would run on, meaning:
1. First, all conditions are run to find matching groups. 
1. Then, aggregations are run **over all rows of these matching groups** (rather than just over the specific rows that matched the given conditions).

The following tweak to the query should make that clearer:
```
{
    "query": {
        "conditions": [
            {
                "filter": ["event", "==", "transaction"],
                "target": ["count", ">", 1]
            }
        ]
    }
}
```
Note that `target` attribute, which was omitted in the previous query. What this condition is now looking for is users 
for which the condition is met **more than once per each user**: 2,528. 

When a `target` attribute is not specified, as in the previous query, it is internally set to a default implicit target of `["count", ">=", 1]`

Now, change the the `target` attribute to `["count", "==", 1]`, and you'll get a count of 9041. Together that's 11,569 - 
the number of all users with one or more purchase, as above.

Here's a small but neat feature of conditions: we can look for all users who **did not** make any transaction. 
Change the target value to `["count", "==", 0]`, and the result is 1,224,463. 
Add to that number the 11,569 users with a one or more purchase and you'll get the total number of users again.

The array format used above for the `filter` and `target` attributes is a shorthand notation. The equivalent verbose format is:
```
{
    "query": {
        "conditions": [
            {
                "filter": {
                    "column": "event", 
                    "op": "==", 
                    "value": "transaction"
                },
                "target": {
                    "type": "count", 
                    "op": ">", 
                    "value": 1
                }
            }
        ]
    }
}
```
This is the format that the query engine sees: implicit defaults are made explicit and shorthand notations are 'expanded' during query
validation. This more elaborate version is safer to use when generating queries by code (which we assume to be the main use case),
while the shorthand notation allows easier manual composition by hoomans.

### Multi-Filter Conditions

Let's find users who've made a purchase of at least one item whose price >= 50 (in whatever denomination this dataset uses - we have no idea!):
```
{
    "query": {
        "conditions": [
            {
                "filters": [
                    {"column": "event", "op": "==", "value": "transaction"},
                    {"column": "price", "op": ">=", "value": 50}
                ]
            }
        ]
    }
}
```
That gives us 6,097 users.

Note we're using `filters` (plural) instead of `filter`. The list of conditions within **must all match in the same row**,
while separate condition objects in the `conditions` array only need to match in the same group.
If we wanted to find only users with two or more such rows, we'll need to add an explicit `"target": ["count", ">", 1]`.

### The 'sum' Target

What if instead we'd like to find users for whom the *sum* of all their transactions is above a given value?
For that, we should use a slightly different target, shown in verbose notation this time:
```
{
    "query": {
        "conditions": [
            {
                "filter": {"column": "event", "op": "==", "value": "transaction"},
                "target": {"type": "sum", "column": "price", "op": ">=", "value": 50}
            }
        ]
    }
}
```
Naturally, this query returns a higher matching groups count than the previous one: 6,505 vs. 6,097. 
This query captures all users who matched the previous query, *plus* others for whom no single purchase had a price >= 50.

**We can now phrase more exactly what `filter` means vs. what `target` stands for:**
* `filter`/`filters` specifies what any *single row* must match (always with an 'and' relation when using multiple `filters`). 
* `target` works at the user (i.e. group) level and specifies either:
  * *How many* rows should match the filter in any given group for the condition to match that group, **OR:**
  * Which column to sum for all matching rows in a given group. That value should match the target definition for the condition to match.

### Using Multiple Conditions

Of course, we can also specify multiple conditions - each with their own filter and target. 
The default logical relation between conditions is `and`, but this can be changed or made arbitrarily more complex. More about that in a minute.

Our functional task now is a tad trickier: we wish to find only users having a total transactions price of >= 50 yet who *did not* have any single purchase
whose price >= 50.

This requires two separate conditions: one looks for users with the desired total transaction price (as in the previous query),
the other looks for users with a single purchase value >= 50 **but with a target of count == 0**, 
meaning: users where this filter is not matched in any row.
```
{
    "query": {
        "relation": "and",
        "conditions": [
            {
                "filters": [
                    {"column": "event", "op": "==", "value": "transaction"},
                    {"column": "price", "op": ">=", "value": 50}
                ],
                "target": ["count", "==", 0]
            },
            {
                "filter": ["event", "==", "transaction"],
                "target": ["sum", "price", ">=", 50]
            }
        ]
    }
}
```
...and the resulting count is 408 users (that's basically the delta between the two previous queries: 6,505 - 6,097 = 408).

The `relation` attribute was set explicitly in the above query only for clarity's sake, as that's also the implicit default. 
You can set this to `or` (or use `&&` / `||` syntax if you fancy).

The value of `relation` can also be a logical expression, such as `$0 and ($1 or $2)` to any depth, with `$<n>` referencing the 
zero-based condition index in the conditions array. Alternatively, any condition can also have a `name` attribute, in which case the relation
expression can refer to it by name (e.g. `$big_total_purchases AND $no_big_single_purchase`).

### Sequences

We've promised you that Funnel Rocket can also check for conditions occuring in a specific order (per each user). Let's start with
specifying such a sequnece, but we're not interested yet in how many users matched each step - we just want the users matching all of them.

Let's specify the most classic of e-commerce sequences: find users who've viewed some products, then added something to cart, then made a purchase:
```
{
    "query": {
        "conditions": [
            {
                "sequence": [
                    {
                        "filter": ["event", "==", "view"]
                    },
                    {
                        "filter": ["event", "==", "addtocart"]
                    },
                    {
                        "filter": ["event", "==", "transaction"]
                    }
                ]
            }
        ]
    }
}
```
Each element in the sequence has a `filter` or `filters` attribute. It does not however accept a `target`.
The implicit target in that case is: there should be at least one row with this filter earlier in time than the next step.

We might expect that all users who've made a transaction (11,569 as we've found way above) would match this sequence, 
but the result we get is different: 9,830. Obviously some users either did not go through this classic route, or perhaps
in reality they did - yet the appropriate activities are not included in the data set.

There are various possible reasons for this. Here are two of them:
1. The dataset most probably includes only data from a specific timeframe - meaning some users have performed the first steps in the 
   sequence *before* the point in time in which the dataset starts, yet their transaction falls inside the data set duration. 
   This also goes the other way: some users probably ended up purchasing shortly after the dataset's end time. 
   The shorter the duration of the dataset, the more such cases we'd have (relative to the total user set in the dataset).
1. There may be multiple data quality issues ending up in some activities not being collected: perhaps when the user adds a product to cart 
   from a 'quick look' view rather than the full product page, an 'addtocart' event is not collected? ...and so on.

This goes to show that before you start looking for sequences, it's important to check your assumptions about the data. 
Either reality differs somewhat from expectations (pretty much always...), and/or the collected data doesn't exactly reflect reality (frequently).

### Finally a Funnel

Let's now make the above sequence into an honest funnel. Before going into nuances, let's run this modified query:
```
{
    "funnel": {
        "sequence": [
            {
                "filter": ["event", "==", "view"]
            },
            {
                "filter": ["event", "==", "addtocart"]
            },
            {
                "filter": ["event", "==", "transaction"]
            }
        ]
    }
}
```
Here's what we get in return:
```
  [...]
  "query": {
    "matchingGroups": 1236032,
    "matchingGroupRows": 2500516,
    "aggregations": null
  },
  "funnel": {
    "sequence": [
      {
        "matchingGroups": 1232600,
        "matchingGroupRows": 2495567,
        "aggregations": null
      },
      {
        "matchingGroups": 32553,
        "matchingGroupRows": 388881,
        "aggregations": null
      },
      {
        "matchingGroups": 9830,
        "matchingGroupRows": 209016,
        "aggregations": null
      }
    ],
    "endAggregations": null
  }
```
This means:
1. After executing the query conditions, there are 1,236,032 matching users - in this case, that's everyone as there are no conditions set.
1. Then, after each step in the funnel we see the drop in user count. The drop after the first step is only slight, which is a small relief in terms of data quality: it means there are almost no users in the dataset without a single product pageview.
1. We didn't ask for any aggregations, but looking at the above response you can see that aggregations can be requested at three stages:
   1. After query conditions were ran
   1. After each step in the funnel
   1. Only after the last step
   
This is a good opportunity to clearly define the execution stages:

1. **First, run all conditions** in the `query.conditions` array. 
  * If there are none, all groups match this step. 
  * If there's more than one condition, the `query.relation` attribute dictates the logical operator between conditions (`and` by default).

2. **Second, any aggregations** defined under `query.aggregations` are run. 
   * These run only over matching groups, but take into account all rows in these groups.

3. **Lastly, an optional funnel** is run, based only on users matching the conditions (or all). 
   * Aggregations can be requested after each step, or only at the end following the last one.

Now, let's use the funnel and its aggregations feature a bit differently. can you tell what the following does?
```
{
    "funnel": {
        "sequence": [
            {
                "filter": ["event", "==", "view"]
            },
            {
                "filter": ["event", "==", "view"]
            },
            {
                "filter": ["event", "==", "view"]
            },
            {
                "filter": ["event", "==", "view"]
            },
            {
                "filter": ["event", "==", "view"]
            }
        ],
        "stepAggregations": [
            {"column": "event", "type": "groupsPerValue"},
            {"column": "event", "type": "sumPerValue", "otherColumn": "price"}
        ]
    }
}
```
What we're looking for here is actually the conversion rate from `view` to `addtocart` and `transaction`, first for users
who've had 1+ views, then progressively narrow down till we reach only users with 5+ views. We also ask for the accumulated price per each event type,
so we can check if the average transaction of these 5+ page-viewers is markedly higher than the average. The answer in this dataset is yes.  

It's important to note that these are not distinct user groups: the first step includes everyone in later steps. 
Assuming we wanted to know the average transaction size for those with exactly one pageview, we'd be better off using a condition to enforce this:
```
{
    "query": {
        "conditions": [
            {
                "filter": ["event", "==", "view"], "target": ["count", "==", 1]
            }
        ],
        "aggregations": [
            {"column": "event", "type": "groupsPerValue"},
            {"column": "event", "type": "sumPerValue", "otherColumn": "price"}
        ]
    }
}
```
We can also use a view->addtocart->transaction funnel here in addition to the query condition, to ensure that `addtocart` and `transaction` must follow in order after the `view`. 
Otherwise, there's no *guaranteed* order between these activities.
  
**This brings us to the final note on funnels in this guide:** 

If you're using one, try to wrap your head around what cases it would capture, and what it may not. While this tool is named "Funnel Rocket", it's perfectly happy to run queries without one.

### Specifying a Timeframe for the Whole Query

Actually, there is one more optional execution phase, which takes place before any conditions are run. You can define
a timeframe that will apply to *everything* that executes later on in the query. Here's how it is defined:
```
{
    "timeframe": {
        "from":1577836800000,
        "to":1609459200000
    },
    "query": {
      [...]
```
Make sure to use a timestamp format that matches the values in the timestamp column.

The given timeframe (just a 'from', just a 'to', or both) will affect which rows will be loaded from the Parquet file at all. 
In fancier words, this is a predicate pushed down to the Parquet driver level, making it very efficient.

### Missing Features of Funnels

There are a couple of very useful features yet to be released: 
* Defining a min/max time duration for the funnel, 
* Allowing a "this step did NOT happen" flag for a given step.

Note that having *absolute* time constraints in funnels, and in conditions in general, is possible by filtering on the timestamp column.

## Where to Go From Here

Learn how to deploy & configure serverless (or server-full) workers in production with the [Operations Guide](./operating.md).

Review the full [API and query reference](api.md).