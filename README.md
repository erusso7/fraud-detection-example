# Trino Demo

This repository contains a demo of trino showing how to fetch `.parquet` files from "S3" (minio in this case) and performing some queries with trino SQL.

## Tech stack
* Trino (formerly PrestoSQL)
* Minio - for hosting the file. AWS S3 compatible.
* Hive Metastore - for accessing files from Trino using Hive connector
* MariaDB - As a direct dependency for the hive-metastore

The following file types are supported for the Hive connector:

- ORC
- Parquet
- Avro
- RCText (RCFile using ColumnarSerDe)
- RCBinary (RCFile using LazyBinaryColumnarSerDe)
- SequenceFile
- JSON (using org.apache.hive.hcatalog.data.JsonSerDe)
- CSV (using org.apache.hadoop.hive.serde2.OpenCSVSerde)
- TextFile

## Getting started
* `docker-compose up -d`
* Docker volumes are locally mounted. This should help in understanding the data of different service.

## Setup some data

##### Using the existing file with 100 rows with 0% corrupted data
```shell
cp fraud_100_corrupted_0.parquet data/minio/fraud/fraud_data/
```

##### Or creating a file with the number of rows you need 
-> **N**: number of rows to generate. Must be a positive integer (default: 100).

-> **CT**: corruption type ('missing': will remove some columns. 'empty': will keep the column but remove the data instead).

->  **CN**: approximate percentage of corrupted data, must be between 0 and 100 (default: 0).

```shell
go run . -r <N> -ct <CT> -cp <CP> && cp fraud_<N>_<CT>_<CP>.parquet data/minio/fraud/fraud_data/
```

## Start Trino CLI and run some queries

```bash
$ docker exec -it trino trino
```

```sql
trino> CREATE SCHEMA IF NOT EXISTS hive.fraud WITH (location = 's3a://fraud/');
```

```sql
-- Path s3a://fraud.fraud_data is the holding directory.
-- We dont give full file path. Only parent directory
trino> CREATE TABLE IF NOT EXISTS hive.fraud.fraud_data (
  timestamp INTEGER,
  label     VARCHAR,
  user_id   INTEGER,
  amount    DOUBLE,
  merchant_id INTEGER,
  trans_type VARCHAR,
  foreign BOOLEAN,
  interarrival DOUBLE
) WITH (
  external_location = 's3a://fraud/fraud_data',
  format = 'PARQUET'
);
```

```sql
-- Testing
trino> SELECT * FROM hive.fraud.fraud_data LIMIT 10;
```