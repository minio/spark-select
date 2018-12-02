# Spark S3 Select Connector Library
A library for downloading dataframes from S3 compatible object storage using Select API.

## Requirements
This library requires Spark 2.3+

## Features
This package can be used to download a dataframe from S3 compatible object storage using Select API.

### Scala API
Spark 2.3+:

With custom schema for `CSV`.
```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val struct = StructType(
  StructField("name", StringType, true) ::
    StructField("age", IntegerType, false) :: Nil)

var df = spark.read.format("selectCSV").schema(struct).load("s3://sjm-airlines/people.csv")

println(df.count())

println(df.show())
```

With custom schema for `JSON`.
```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val struct = StructType(
  StructField("name", StringType, true) ::
    StructField("age", IntegerType, false) :: Nil)

var df = spark.read.format("selectJSON").schema(struct).load("s3://sjm-airlines/people.json")

println(df.count())

println(df.show())
```

Inferred schema by automatically parsing the first record `CSV`
```scala
spark.read
    .format("selectCSV")
    .load("s3://bucket/object.csv")
```

Inferred schema by automatically parsing the first record in `JSON`
```scala
spark.read
    .format("selectJSON")
    .load("s3://bucket/object.json")
```

### Using spark-select

Setup all environment variables
> NOTE: It is assumed that you have already installed hadoop-2.8.5, spark 2.3.1 at relevant locations.
```
export HADOOP_HOME=${HOME}/spark/hadoop-2.8.5/
export PATH=${PATH}:${HADOOP_HOME}/bin
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export SPARK_HOME=${HOME}/spark/spark-2.3.1-bin-without-hadoop/
export PATH=${PATH}:${SPARK_HOME}/bin
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/

git clone https://github.com/minio/spark-select
sbt assembly
spark-shell --jars target/scala-2.11/spark-select-assembly-0.0.1.jar
```

Once the `spark-shell` has been successfully invoked.
```
Spark context Web UI available at http://10.0.0.13:4040
Spark context available as 'sc' (master = local[*], app id = local-1543570505559).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.3.1
      /_/

Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_181)
Type in expressions to have them evaluated.
Type :help for more information.

scala> :load examples/custom-csv.scala
Loading examples/custom-csv.scala...
import org.apache.spark.sql._
import org.apache.spark.sql.types._
struct: org.apache.spark.sql.types.StructType = StructType(StructField(name,StringType,true), StructField(age,IntegerType,false))
df: org.apache.spark.sql.DataFrame = [name: string, age: int]
3
+-------+---+
|   name|age|
+-------+---+
|Michael| 31|
|   Andy| 30|
| Justin| 19|
+-------+---+

()
```
