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
