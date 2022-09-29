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

## Seed with sample data
1. Create the bucket: `mkdir -p data/minio/fraud/fraud_data`
2. Choose one of these alternatives:

##### A. Use the existing example file with 100 rows
```shell
cp fraud_100.parquet data/minio/fraud/fraud_data/
```

##### B. Create a file with the number of rows you need 
```shell
go run . <N> && cp fraud_<N>.parquet data/minio/fraud/fraud_data/
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