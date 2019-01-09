# Minio Spark Select
Minio Spark select enables retrieving only required data from an object using Select API.

## Requirements
This library requires
- Spark 2.3+
- Scala 2.11+

## Features
S3 Select is supported with CSV, JSON and Parquet files using `selectCSV`, `selectJSON` and `selectParquet` values to specify the data format.

### HowTo
Include this package in your Spark Applications using:

#### *spark-shell*, *pyspark*, or *spark-submit*
```
> $SPARK_HOME/bin/spark-shell --packages io.minio:spark-select_2.11:1.1
```

#### *sbt*
If you use the [sbt-spark-package plugin](http://github.com/databricks/sbt-spark-package), in your sbt build file, add:
```
spDependencies += "minio/spark-select:1.1"
```
Otherwise,
```
libraryDependencies += "io.minio" % "spark-select_2.11" % "1.1"
```

#### *Maven*
In your pom.xml, add:
```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>io.minio</groupId>
    <artifactId>spark-select_2.11</artifactId>
    <version>1.1</version>
  </dependency>
</dependencies>
```

#### *Source*

Setup all required environment variables
> NOTE: It is assumed that you have already installed hadoop-2.8.5, spark 2.3.1 at some locations locally.
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
scala> :load examples/csv.scala
Loading examples/csv.scala...
import org.apache.spark.sql._
import org.apache.spark.sql.types._
schema: org.apache.spark.sql.types.StructType = StructType(StructField(name,StringType,true), StructField(age,IntegerType,false))
df: org.apache.spark.sql.DataFrame = [name: string, age: int]
+-------+---+
|   name|age|
+-------+---+
|Michael| 31|
|   Andy| 30|
| Justin| 19|
+-------+---+

scala>
```

### API

#### *PySpark*
```py
spark
  .read
  .format("selectCSV") // "selectJSON" for JSON or "selectParquet" for Parquet
  .schema(...) // mandatory
  .options(...) // optional
  .load("s3://path/to/my/datafiles")
```

#### *R*
```
read.df("s3://path/to/my/datafiles", "selectCSV", schema)
```

#### *Scala*
```
spark
  .read
  .format("selectCSV") // "selectJSON" for JSON or "selectParquet" for Parquet
  .schema(...) // mandatory
  .options(...) // optional. Examples:
  // .options(Map("quote" -> "\'", "header" -> "true")) or
  // .option("quote", "\'").option("header", "true")
  .load("s3://path/to/my/datafiles")
```

#### *SQL*
```
CREATE TEMPORARY VIEW MyView (number INT, name STRING) USING selectCSV OPTIONS (path "s3://path/to/my/datafiles")
```

### Options
The following options are available when using `selectCSV` and `selectJSON`. If not specified, default values are used.

#### Common Options
| Option | Default | Usage |
|---|---|---|
| `endpoint` | "" | endpoint is an URL as listed below: (Required)|
| | |https://s3.amazonaws.com |
| | |https://play.minio.io:9000 |
|`access_key`   | ""  |access_key is like user-id that uniquely identifies your account. (Optional)|
|`secret_key`  | "" |secret_key is the password to your account. (Optional)|
|`path_style_access` | "false" |Enable S3 path style access ie disabling the default virtual hosting behaviour. (Optional)|

#### *Options with selectCSV*
| Option | Default | Usage |
|---|---|---|
| `compression` | "none" | Indicates whether compression is used. "gzip", "bzip2" are values supported besides "none".
| `delimiter` | "," | Specifies the field delimiter.
| `header` | "true" | "false" specifies that there is no header. "true" specifies that a header is in the first line. Only headers in the first line are supported, and empty lines before a header are not supported.

#### *Options with selectJSON*
| Option | Default | Usage |
|---|---|---|
| `compression` | "none" | Indicates whether compression is used. "gzip", "bzip2" are values supported besides "none".
| `multiline` | "false" | "false" specifies that the JSON is in S3 Select LINES format, meaning that each line in the input data contains a single JSON object. "true" specifies that the JSON is in S3 Select DOCUMENT format, meaning that a JSON object can span multiple lines in the input data.

#### *Options with selectParquet*
There are no **options** needed with Parquet files.

### Additional Examples
With schema with two columns for `CSV`.
```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val schema = StructType(
 s List(
    StructField("name", StringType, true),
    StructField("age", IntegerType, false)
  )
)

var df = spark.read.format("selectCSV").schema(schema).option("endpoint", "http://127.0.0.1:9000").option("access_key", "minio").option("secret_key", "minio123").option("path_style_access", "true").load("s3://sjm-airlines/people.csv")

println(df.show())

println(df.select("age").filter("age > 19").show())
```

With custom schema for `JSON`.
```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._

vl schema = StructType(
  List(
    StructField("name", StringType, true),
    StructField("age", IntegerType, false)
  )
)

var df = spark.read.format("selectJSON").schema(schema).option("endpoint", "http://127.0.0.1:9000").option("access_key", "minio").option("secret_key", "minio123").option("path_style_access", "true").load("s3://sjm-airlines/people.json")

println(df.show())

println(df.select("age").filter("age > 19").show())
```

With custom schema for `Parquet`.
```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val schema = StructType(
 s List(
    StructField("name", StringType, true),
    StructField("age", IntegerType, false)
  )
)

var df = spark.read.format("selectParquet").schema(schema).option("endpoint", "http://127.0.0.1:9000").option("access_key", "minio").option("secret_key", "minio123").option("path_style_access", "true").load("s3://sjm-airlines/people.csv")

println(df.show())

println(df.select("age").filter("age > 19").show())
```
