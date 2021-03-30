<img align="left" width="64" height="64" src="./logo-icon-light-blue.svg">

# Funnel Rocket API

## Register a Dataset

URL: `POST http://<api-server-host>:5000/datasets/register[?stream=true|false]`

To query a dataset in Funnel Rocket, you first need to register it. Registration involves three steps:
1. **Discovery:** the API Server (or CLI) will list files in the given base path matching the supplied pattern.
2. **Validation:** the API Server will launch a job (enqueue tasks to workers) to load a selection of files from the dataset and verify 
   they match the minimum requirements including proper partitioning (see the [README](../README.md)) and have a common schema.
3. **Storing metadata:** The details of supported columns and their statistics are stored to the Datastore (currently using Redis).

Currently, all files should be in Parquet format and must reside in the same directory. 
Locally-mounted directories and AWS S3 are supported. You may also use alternative object stores which are fully S3-comptible, e.g. MinIO.
In that case you'll need to configure Funnel Rocket appropriately - see the [configuration reference](./operating.md).

#### Naming Datasets

The same set of files may be registered under multiple dataset names, so you be creative with names and aliases. 
For example, register multiple revisions of a dataset with a naming convention such as `"<customerX>-<date/revision>"`, 
but also register the latest one as `<customerX>-current` any time a new revision is ready. There is no special flag needed to reuse an existing dataset name
when registering. Funnel Rocket will make sure not to use any earlier-cached files for a dataset that's been re-registered with the same name.

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

* **name** - Any given name to later identify the dataset with. It may include spaces and special characters, but be sure to properly encode the name when used in JSON body and in URL parameters.
* **basepath** - The directory under which all dataset files reside. 
  * For local directories, simply provide the path or prefix with `file://`. 
  * For S3, prefix the bucket name with `s3://` (not an HTTPS URL).
* **groupIdColumn** - The column name holding the group ID (e.g. User ID) - see the README for more.
* **timestampColumn** - The column name holding the timestamp when the event detailed in each row took place.
* **pattern** (optional, default is `*.parquet`) - Pattern of file names to look for, directly under the given basepath.
* **validationMode** (optional, default is `SAMPLE`) - Determines which selection of files to validate. 
  Available choices are: `SINGLE`, `FIRST_LAST` and `SAMPLE`. 
  * In `SINGLE` mode only the first file in the dataset (in lexicographic order) is loaded, meaning that this file's schema is not compared to any other file, and proper partitioning is also not validated. 
  * In `FIRST_LAST` mode, the first and last part are loaded, their schema compared and uniqueness of group IDs in each file validated. 
  * In `SAMPLE` mode, the number of files to load is relative to the total file count in the dataset (with a ratio and a maximum defined in configuration). 
    The first and last files are always loaded, plus the appropriate number of other random files.
* **validateUniques** (optional, default `true`) - Allows disabling the test of group ID set uniqueness across between sampled files. 
  This check requires a chunk of memory in workers and the API server, plus some temporary storage in the blobstore. 
  If it fails due to a resource issue you may disable this check, provided that if you're certain the files are properly partitioned by group ID.
  Alternatively, you may configure a reduced number of files to sample.
  
### Response

Status code `200` on success, `500` on error.

```json5
{
  "success": true,
  "errorMessage": "only set if success is false", // Only available if success is false
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
    // [...] See documentation section below for details
  }
}
```

The response includes a few general-purpose attributes: `success`, `errorMessage`, `requestId` and `stats`. For `stats` 
please see details later in this page. Here we've only included a few attributes of it showing the total count of files (a.k.a *parts*) 
and the total size in bytes.

### Streaming usage

The `/register`, `/query` and `/empty-query` endpoints all involve running jobs by multiple workers, and may take some time to complete.
Thus, you have the option to receive status updates on the request progress via *HTTP chunked encoding* (a.k.a. HTTP streaming, as seen in the Twitter API). To enable, add `?stream=true` to the endpoint URL.

This ain't no fancy streaming protocol, but for pushing a few updates it's more than enough - and supported by many client libraries.
It's easy to see how this works locally with a browser: register a dataset (such as the [example dataset](./example-dataset.md)) and go to `http://localhost:5000/datasets/<name>/empty-query` 

<p align="center">
  <img alt="Streaming query in the browser" src="https://github.com/DynamicYieldProjects/funnel-rocket-oss/blob/assets/assets/streaming-query.gif?raw=true" width="600">
</p>
![str]
## List Datasets

## Get Dataset Schema

## Get Dataset Parts

## Run Query

## Unregister

## Stats
