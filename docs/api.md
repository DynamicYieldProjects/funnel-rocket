<img align="left" width="64" height="64" src="./logo-icon-light-blue.svg">

# Funnel Rocket API

## Register a Dataset

URL: `POST http://<apiserver-hostname>:5000/datasets/register[?stream=true|false]`

To query a dataset in Funnel Rocket you first need to register it. Registration involves three steps:
1. **Discovery:** the API Server (or CLI) will list files in the given base path matching the specified pattern.
2. **Validation:** the API Server will invoke workers to load a subset of files from the dataset and verify 
   they match the minimum requirements, which includes having proper partitioning (see the [README](../README.md)) and a common schema.
3. **Storing metadata:** The details of supported columns are stored to the Datastore (currently using Redis).

Currently, all files should be in Parquet format and must reside in the same directory. 

Locally-mounted directories and AWS S3 are supported. You may also use alternative object stores which are fully S3-comptible, e.g. MinIO.
In that case you'll need to configure Funnel Rocket appropriately - see the configuration reference in the [operations guide](./operating.md).

#### Naming Datasets

The same set of files may be registered under multiple dataset names, so you get creative with names and aliases. 

For example, assuming you want to keep multiple revisions of a dataset created over time but also keep track of the latest one:
* Register multiple revisions of a dataset with a naming convention such as `"<customerX>-<date/revision>"`
* Each time a new revision is made ready, register that latest one also as `<customerX>-current` - re-registering over an existing name is ok.
  In such a case, Funnel Rocket will invalidate any cached files from earlier registrations of that dataset.
  
### Request Body

```json5
{
   "name": "example",
   "basepath": "s3://some-bucket/dir/subdir/",
   "groupIdColumn": "userId",
   "timestampColumn": "timestamp",
   "pattern": "*.pattern",         // Optional
   "validationMode": "FIRST_LAST", // Optional
   "validateUniques": true         // Optional
}
```

* **name** - A name to later identify the dataset with. It may include spaces and special characters, but be sure to properly encode the name when used in a JSON request body or in URL parameters.
* **basepath** - The directory under which all dataset files reside. 
  * For local directories, simply provide the path or prefix with `file://`. 
  * For S3, prefix the bucket name with `s3://` (not an HTTPS URL).
* **groupIdColumn** - The column name holding the group ID (e.g. User ID) - see the README for more.
* **timestampColumn** - The column name holding the timestamp when the event detailed in each row took place.
* **pattern** (optional, default is `*.parquet`) - Pattern of file names to look for, directly under the given basepath.
* **validationMode** (optional, default is `SAMPLE`) - Determines which selection of files to validate. 
  Available choices are: `SINGLE`, `FIRST_LAST` and `SAMPLE`. 
  * In `SINGLE` mode, only the first file in the dataset (in lexicographic order) is loaded, meaning that this file's schema is not compared to any other file, and proper partitioning is also not validated. 
  * In `FIRST_LAST` mode, the first and last part are loaded, their schema compared and uniqueness of group IDs in each of them validated. 
  * In `SAMPLE` mode, the number of files to load is relative to the total number of files in the dataset. This ratio is configurable, along with a hard limit on the number of files to sample per each dataset.
    The first and last files are always loaded, plus the appropriate number of other random files.
* **validateUniques** (optional, default `true`) - Allows disabling the test of group ID set uniqueness between sampled files. 
  This check requires a chunk of memory in workers and the API server, plus some temporary storage in the blobstore. 
  If it fails due to a resource issue you may disable this check if you're certain that files are properly partitioned by group ID.
  Alternatively, you may configure a reduced number of files to sample.
  
### Response

Status code `200` on success, `500` on error.

```json5
{
  "success": true,
  "errorMessage": "details of the error", // Only available if success is false
  "requestId": "1611096132-891c0cb0",
  "dataset": {
    "basepath": "s3://some-bucket/dir/subdir",
    "groupIdColumn": "userId",
    "id": {
      "name": "example",
      "registeredAt": "2021-02-10T08:32:12.412792+00:00"
    },
    "timestampColumn": "timestamp",
    "totalParts": 8
  }, 
  "stats": {
    "dataset": {
      "parts": 8,
      "totalSize": 88240183
    },
    // [...] See documentation section below for details on the stats object
  }
}
```

The response includes a few general-purpose attributes: `success`, `errorMessage`, `requestId` and `stats`. 
Here we've only included a few attributes showing the total count of files (a.k.a. *parts*) and the total size in bytes.

### Streaming Updates

The `/register`, `/query` and `/empty-query` endpoints all involve running jobs by multiple workers, and may take some time to complete.
Thus, you have the option to receive status updates on the request progress via *HTTP chunked encoding* (a.k.a. HTTP streaming, as seen in the Twitter API). To enable, add `?stream=true` to the endpoint URL.

This is a simple mechanism supported by many client libraries.
To test locally how this works, register a dataset (such as the [example dataset](./example-dataset.md)) 
and open the URL `http://localhost:5000/datasets/<dataset name>/empty-query?stream=true` in your browser.

<p align="center">
  <img alt="Streaming query in the browser" src="https://github.com/DynamicYieldProjects/funnel-rocket-oss/blob/assets/assets/streaming-query.gif?raw=true" width="600">
</p>

In this mode, the last chunk is the response JSON as you'd receive with the non-streaming default mode.
Any progress updates before that final JSON are in the following format:
```json
{"message": "Polling results", "stage": "RUNNING", "tasks": {"ENDED_SUCCESS": 6, "QUEUED": 1, "RUNNING_QUERY": 1}}
```
* `message` is an optional human-friendly description, which might change.
* `stage` is one of `STARTING`, `RUNNING`, `FINISHING`, `DONE`. Stages can't progress 'in reveres' - only in this order.
* `tasks` holds a map of task status to count. Internally all tasks start their lifecycle in status QUEUED, and the sum of all counts in a progress message should always
  amount to the expected total number of tasks. 
  * Statuses are: `QUEUED`, `LOADING_DATA`, `RUNNING_QUERY`, `RUNNING`, `ENDED_SUCCESS`, `ENDED_FAILED`.
  * When a task is considered failed or lost and re-attempted, the new attempt will begin its lifecycle as 'QUEUED' and proceed from there. 
    Subsequent updates will not include previous attempts in the counts, as they are a point-in time snapshot of current state.

You may receive any number of updates (zero or more) based on the pace of the job. For example, the first update to receive may already be in `RUNNING` stage.

btw, if you're using Postman note that it doesn't support chunked encoding. It will show all chunks at once when the response ends.

#### Status code in streaming mode

In streaming mode, the HTTP response is sent over with the first update with status code `200`. 
The true status of the request is returned with the last chunk as the `success` attribute.

Only if the request fails before tasks are actually launched (e.g. when validating the request body) will you get a `500` HTTP response.

## List Datasets

URL: `GET http://<apiserver-hostname>:5000/datasets`

### Response

Status code `200` on success, `500` on error.

Returns a JSON array with each element describing a registered dataset, or an empty array if there are none.

```json5
[
  {
    "basepath": "data/retail/",
    "groupIdColumn": "visitorid",
    "id": {
      "name": "example",
      "registeredAt": "2021-03-30T08:32:12.412792+00:00"
    },
    "timestampColumn": "timestamp",
    "totalParts": 8
  },
  {
    // [...] More datasets
  }
]
```

## Get Dataset Schema

URL: `GET http://<apiserver-hostname>:5000/datasets/<dataset-name>/schema[?full=true|false]`

Returns the stored schema for a registered dataset. 

By default, the *short schema* is returned. It is more concise and is typically all you'll need. Use `?full=true` to get more detailed data.

### Response

Here's the response for the example dataset:

```json
{
    "columns": {
        "available": "BOOL",
        "categoryid": "STRING",
        "cryptic_attrs": "STRING",
        "cryptic_attrs_cat": "STRING",
        "event": "STRING",
        "itemid": "INT",
        "price": "FLOAT",
        "timestamp": "INT",
        "transactionid": "FLOAT",
        "visitorid": "INT"
    },
    "minTimestamp": 1430622024154.0,
    "maxTimestamp": 1442545187788.0,
    "sourceCategoricals": [
        "event",
        "categoryid",
        "cryptic_attrs_cat"
    ],
    "potentialCategoricals": []
}
```
* **columns** - a map of all *supported* columns and their type. Currently, one of: `BOOL`, `INT`, `FLOAT`, `STRING`.
* **minTimestamp, maxTimestamp** - The minimum & maximum values found in the dataset's timestamp column during registration. 
  * Note: these values are taken from the files sampled for validation - they do not represent the exact min/max timestamp
    in the dataset. However, if the dataset is properly partitioned this should be a good approximation.
* **sourceCategoricals** - If the dataset files were saved by Pandas with some string columns being in Pandas' categorical type, these columns are listed here and will be loaded as such.
* **potentialCategoricals** - String columns which were detected during registration to benefit from being loaded as categoricals during= queries. 
  This behavior is [configurable](./operating.md).

#### Full Schema Details

If `?full=true` is added to the URL, more details are returned:
* The *dtype* (Pandas data type) of each column
* For numeric columns, the min & max values observed in the sampled files during registration.
* For string columns detected as categoricals, a mapping of top values to their frequency in the dataset as a relative number (0-1).
  * The frequency is **an approximation** based on merging the top values' frequencies in files sampled during registration.
  * The length of the list, and the minimum frequency threshold to be included in it, are configurable.
* For categoricals, **catUniqueRatio** is the ratio of unique value count (`series.nunique()`) to the row count (`len(series)`). 
  * The smaller this number, more memory is saved in the categorical representation, and `contains` operators are made faster.
  
Here's an excerpt from the full schema, based on the example dataset:
```json5
{
  "columns": {
    "available": {
      "colattrs": {
        "categorical": false
      },
      "coltype": "BOOL",
      "dtypeName": "bool",
      "name": "available"
    },
    "categoryid": {
      "colattrs": {
        "catTopValues": {
          "1051": 0.03079,
          "1173": 0.01025,
          "1279": 0.01380,
          "1483": 0.02526,
          "1613": 0.01256,
          "196": 0.01129,
          // [...]
        },
        "catUniqueRatio": 0.00366,
        "categorical": true
      },
      "coltype": "STRING",
      "dtypeName": "category",
      "name": "categoryid"
    },
    "itemid": {
      "colattrs": {
        "categorical": false,
        "numericMax": 466864.0,
        "numericMin": 3.0
      },
      "coltype": "INT",
      "dtypeName": "int64",
      "name": "itemid"
    },
    // [...] More columns
  },
  "groupIdColumn": "visitorid",
  "timestampColumn": "timestamp"
}
```

## Get Dataset Parts

URL: `GET http://<apiserver-hostname>:5000/datasets/<dataset-name>/parts`

Returns information on files in the dataset: all file names (relative to the basepath), total parts and total size in bytes.

The attribute `namingMethod` is currently always set to `LIST`, 
meaning that all files names are stored in the metadata rather than a pattern template. 

### Response

```json
{
    "filenames": [
        "part-00000.parquet",
        "part-00001.parquet",
        "part-00002.parquet",
        "part-00003.parquet",
        "part-00004.parquet",
        "part-00005.parquet",
        "part-00006.parquet",
        "part-00007.parquet"
    ],
    "namingMethod": "LIST",
    "totalParts": 8,
    "totalSize": 88240183
}
```

## Run an Empty Query

URL: `GET http://<apiserver-hostname>:5000/datasets/<dataset-name>/empty-query[?stream=true|false]`

Runs the *empty query* on a registered dataset, meaning: a query with no conditions and no aggregations. 
It is similar to running a query with the string `{}`, with the only difference being one of convenience: this is a `GET` request without a request body. 

This endpoint is useful as a sanity test to ensure that all files in the dataset are successfully downloaded & loaded by workers. 
The response includes the total count of rows and unique groups in the dataset as `matchingGroupRows` and `matchingGroups`, respectively.

### Response

```json5
{
  "success": true,
  "query": {
    "matchingGroupRows": 2500516,
    "matchingGroups": 1236032
  },
  "requestId": "1617106320-6a1e53e9",
  "stats": {
    // [...] See below section on stats
  }
}
```

### Streaming Updates

See the documentation above for the `/register` endpoint.

## Run Query

URL: `POST http://<apiserver-hostname>:5000/datasets/<dataset-name>/query[?stream=true|false]`

### Request Body

Here's an annotated version of the full query spec. 

**It is strongly encouraged to go through the [example dataset guide](./example-dataset.md) which covers most features 
in a step-by-step fashion.**

*(annotated in JSON5 format, but actual queries are always in JSON)*

```json5
{
  // OPTIONAL: Timeframe affecting all operations (conditions, funnel, aggregations)
  // Either from, to or both can be specified. Value format should match the timestamp column
  "timeframe": {
    "from": 156587800,
    "to": 158345260
  },
  
  // OPTIONAL. If omitted, a default query is run with no conditions. Only total group and row counts will be returned.
  "query": {
    // OPTIONAL
    // The default logical relation between multiple conditions is AND.
    // Set to:
    // * Either one of: [ and, or, &&, || ] to apply that operator between all conditions, or:
    // * To a string expression referring to specific conditions by their index ($0, $1, ...) or name
    //   with operators between them, such as: "$0 or ($1 and $made_multiple_purchases)".
    "relation": "and",

    // OPTIONAL. If omitted, any aggregations and the funnel specified will run over all groups.
    "conditions": [
      {
        // Condition name is optional, and useful when specifying a relation between complex conditions.
        "name": "made_multiple_purchases",

        // Shorthand notation of a filter. Equals to specifying an object with the keys: column, op, value.
        "filter": [
          "eventId",
          "==",
          280953
        ],
        
        // OPTIONAL:
        // Shorthand notation of a "count"-type target. Equals to specifying an object with keys: type, op, value.
        // If omitted, default target is ["count", ">", 0], meaning the filter should match one or more rows in a group.
        // For a "sum"-type target the short notation is: ["sum", "eventValue", ">", 123434].
        "target": [
          "count",
          "<",
          3
        ],

        // OPTIONAL
        // Relevant only for a count-type target: whether to treat a group with ZERO MATCHING ROWS as a match, 
        // i.e. being able to define "find users who have purchased one or zero times" via a single condition.
        // includeZero is FALSE by default UNLESS the target is ["count", "==", 0], in which case it cannot be false.
        // If includeZero is explicitly set to an invalid value for the given target, a validation error is returned.
        "includeZero": true
      },
      
      // Another condition, this time with the verbose notation
      {
        "filter": {
          "column": "eventId",
          "op": "==",
          "value": 280953
        },
        "target": {
          "type": "sum",
          "column": "eventValue",
          "op": "<",
          "value": 350
        }
      },
      
      // A multi-filter condition: all filters must match IN THE SAME ROW. 
      // The relation between filters in the array is always AND.
      // Filters in the array must be in verbose notation.
      {
        "filters": [
          {
            "column": "eventType",
            "op": "==",
            "value": "purchase"
          },
          {
            "column": "itemCount",
            "op": ">=",
            "value": 3
          },
        ],
                    
        // Target is optional and defined the same way as with single-filter conditions.
        "target": [
          /**/
        ],
      },

      // Sequence-type condition (not broken down like a funnel: all steps much match).
      // Each item holds a filter or filters attribute, in verbose notation. 
      // There is no target definition: each step should match at least once prior to the next step.
      {
        "sequence": [
          {
            "filter": ["eventType", "==", "addToCart"]
          },
          {
            // Multi-filter step (all filters in this step must match in the same row)
            "filters": [
              {
                "column": "eventType", op: "==", value: "purchase"
              },
              {
                "column": "itemCount", op: ">=", value: 3
              }
            ]
          }
        ]
      }
    ],
    
    // OPTIONAL:
    // Aggregations to run AFTER filtering groups to only those matching the above conditions (if any).
    "aggregations": {
      "columns": [
        {
          // Returns a default set of aggregations: count, countPerValue, groupsPerValue.
          "column": "device"
        },
        {
          // An aggretion with an explit type and an optional name (which will appear in the response).
          "column": "transactionId",
          "type": "count",
          "name": "purchase count"
        },
        {
          // For each value of goalId, summarize another column (i.e. per each goalId, give me the total goalValue).
          "column": "goalId",
          "type": "sumPerValue",
          "otherColumn": "goalValue",
          "top": 50 // OPTIONAL. Set the max amount of top values to return in aggregations returning a map of values.
        }
      ],
    }
  },
  
  // OPTIONAL:
  // After filtering by the query conditions and running query aggregations, run the given sequence and return the count
  // of groups and rows after EACH step, optionally with additional aggregations after each step or the final one only.
  "funnel": {
    // The funnel's sequence definition is similar to that of a sequence condition (see above)
    "sequence": [
      {
        "filter": [
          "eventType",
          "==",
          "addToCart"
        ]
      }
      /*...*/
    ],

    // OPTIONAL: Aggregations to perform after each step, with the same syntax as query aggreagtion above.
    "stepAggregations": {
      "columns": [
        // ...
      ]
    },
    
    // OPTIONAL: Aggregations to perform after the final step.
    "endAggregations": {
      "columns": [
        // ...
      ]
    }
  }
}
```

#### Supported Operators

* Numeric columns: `==`, `!=`, `>`, `>=`, `<`, `<=`
* Boolean columns: `==`, `!=` 
* String columns: `==`, `!=`, `>`, `<`, `contains` (**TBD:** more to come)

### Response

Here's an annotated response for a query run over the example dataset:

```json5
{
  "success": true,
  "requestId": "1617111031-2d3a69d9",
  "query": {
    // Count of unique groups matching query conditions (or all groups if no conditions set)
    "matchingGroups": 2528,
    // Count of all rows in the matching groups.
    "matchingGroupRows": 145184,

    // Applied over rows over matching groups.
    "aggregations": [
      {
        // For the count-type aggregation the 'value' is a single number: the count of rows with non-null values.
        "column": "event",
        "type": "count",
        "value": 145184
      },
      {
        // The "countPerValue" aggregation (plus "groupsPerValue", "sumPerValue") return a map of column value to the
        // aggregated result.
        "column": "event",
        "top": 10,
        // Only this number of top results is returned (can be set in the query)
        "type": "countPerValue",
        "value": {
          "addtocart": 17195,
          "transaction": 12941,
          "view": 115048
        }
      },
      {
        "column": "event",
        "top": 10,
        "type": "groupsPerValue",
        "value": {
          "addtocart": 2470,
          "transaction": 2528,
          "view": 2469
        }
      }
    ]
  },
  "funnel": {
    // Results for each step in the funnel, in order.
    "sequence": [
      {
        "matchingGroupRows": 144916,
        "matchingGroups": 2469,
        "aggregations": [
          // Step aggregations, if any
          {
            "column": "event",
            "name": null,
            "top": 10,
            "type": "groupsPerValue",
            "value": {
              "addtocart": 2421,
              "transaction": 2469,
              "view": 2469
            }
          }
        ]
      },
      {
        "matchingGroupRows": 144286,
        "matchingGroups": 2351,
        "aggregations": [
          // Step aggregations for this step
          {
            "column": "event",
            "name": null,
            "top": 10,
            "type": "groupsPerValue",
            "value": {
              "addtocart": 2310,
              "transaction": 2351,
              "view": 2351
            }
          }
        ],
      }
    ],
    // End aggregations running after the final step
    "endAggregations": [
      {
        "column": "event",
        "top": 10,
        "type": "sumPerValue",
        "value": {
          "addtocart": 1923637.740,
          "transaction": 1371991.848,
          "view": 13824174.912
        }
      }
    ]
  },
  "stats": {
    // [...] See documentation below for the stats object
  },
}
```

### Streaming Updates

See the documentation above for the `/register` endpoint.

## Unregister

URL: `POST http://<apiserver-hostname>:5000/datasets/<dataset-name>/unregister[?force=true|false]`

Unregister a dataset. This only removes the metadata in the datastore, but does not touch any files.

By default, trying to unregister a dataset that was just queried will fail with status `500` for a short interval of time,
to ensure that any inflight jobs complete first. To override this behavior, add `?force=true` to the request URL, 
or control via configuration with the `FROCKET_UNREGISTER_LAST_USED_INTERVAL` variable.

### Request Body

*None*

### Response

```json
{
  "success": true,
  "datasetFound": true,
  "datasetLastUsed": 1615203035
}
```
* If the dataset name is not found, the request **will not fail**, but will return with `"datasetFound": false`.
* **datasetLastUsed** is set to the UNIX timestamp (in seconds) of when the dataset was last queried. 
  This is the timestamp that Funnel Rocket uses to determine if the dataset can be safely unregistered.

## Stats Object

API requests which involve invoking tasks via workers also return a `stats` attribute as part of the response JSON.

Below is an actual `stats` object, returned by a no-conditions query run with AWS Lambda-based workers. 
The dataset contains about 250 million rows of activities by 14.5 million users, partitioned into 256 Parquet files.

To put the numbers below in proper context:
* This is a subsequent run over a dataset so Lambda function instances are warm, and most have successfully self-selected a locally-cached part.
* No conditions or optional aggregations are defined, so only two columns are actually loaded: the group ID and timestamp column. 
  These two are always loaded.

Here's the annotated response:
```json5
{
  "totalTime": 2.11058,
  "invoker": {
    // Time it took to complete invoking all 256 tasks via a thread pool.
    "enqueueTime": 0.71042,
    // Time the invoker spent after enqueing in polling task statuses till all tasks were done
    "pollTime": 1.00970,
    // How many tasks were run by workers - one per each part (file) in the dataset
    "totalTasks": 256,
    "failedTasks": 0,
    // A time series in 0.25-second resolution, showing the count of tasks ended successfully in each time slot.
    // Non-cumulative, only data points with any tasks completed are included
    "taskSuccessOverTime": {
      "0.5": 74, // Represents the '0.25s < time <= 0.5s' slot.
      "0.75": 62,
      "1.0": 40,
      "1.25": 28,
      "1.5": 36,
      "1.75": 16
    }
  },
  // The stats below were collected and aggregated from all workers (as part of task results)
  "worker": {
    "coldTasks": 0,
    "warmTasks": 256, // All tasks were run by a warm worker!
    // Stats for the time it took since invocation of tasks started till a task began processing by the Lambda handler.
    // This timing is affected by the time it takes the invoker to enqueue all tasks (see above - 0.71 secs.), 
    // plus the latency of the Lambda system itself from the API being called to the Lambda handler starting.
    "invokeLatency": {
      "mean": 0.59070,
      "min": 0.16777,
      "25%": 0.40236,
      "50%": 0.59363,
      "75%": 0.74199,
      "95%": 1.01390,
      "99%": 1.13268,
      "max": 1.20589
    },
    // Time it took to load the Parquet file (one per task), including download if not locally cached.
    // About two-thirds of the files were cached on local disk, see below.
    "loadTime": {
      "mean": 0.21169,
      "min": 0.04079,
      "25%": 0.06028,
      "50%": 0.07073,
      "75%": 0.44651,
      "95%": 0.63991,
      "99%": 0.77359,
      "max": 0.90352
    },
    // Total time tasks took to run (from the time the handler function started till it completed)
    "totalTime": {
      "mean": 0.22912,
      "min": 0.05613,
      "25%": 0.07623,
      "50%": 0.08619,
      "75%": 0.46496,
      "95%": 0.66509,
      "99%": 0.79398,
      "max": 0.92872
    },
    "scannedRows": 254110234,
    "scannedGroups": 14452091,
    // How many files were loaded from source (S3) vs. from local disk cache. This should be further worked on.
    "cache": {
      "source": 84,
      "diskCache": 172
    }
  },
  "dataset": {
    "totalSize": 10222623169,
    "parts": 256
  },
  // Dollar cost of the Lambda compute time: 0.175 cents. Per-request costs of AWS Lambda and S3 are not included,
  // but are generally a very small fraction of the cost in Funnel Rocket's case.
  "cost": 0.00175
}
```
