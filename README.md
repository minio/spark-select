# Spark Select Connector Library
A library for downloading dataframes to Amazon S3 using Select API.

## Requirements
This library requires Spark 2.1+

## Features
This package can be used to download dataframe to Amazon S3 using Select API.

### Scala API
Spark 2.1+:
```scala
dataFrame.read
    .format("io.minio.spark-select")
    .option("bucketName", "my-bucketname")
    .option("objectName", "my-objectname")
    .option("query", "select * from S3Object")
    .load()
```
