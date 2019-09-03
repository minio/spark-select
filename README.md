# MinIO Spark Select
MinIO Spark select enables retrieving only required data from an object using Select API.

## Requirements
This library requires
- Spark 2.3+
- Scala 2.11+

## Features
- S3 Select is supported with CSV, JSON and Parquet files using `minioSelectCSV`, `minioSelectJSON` and `minioSelectParquet` values to specify the data format.
- S3 Select supports select on multiple objects.
- S3 Select supports querying SSE-C encrypted objects.

### Limitations
- Spark CSV and JSON options such as nanValue, positiveInf, negativeInf, and options related to corrupt records (for example, failfast and dropmalformed mode) are not supported.
- Using commas (,) within decimals is not supported. For example, 10,000 is not supported and 10000 is.
- The following filters are not pushed down to MinIO:
  - Aggregate functions such as COUNT() and SUM().
  - Filters that CAST() an attribute. For example, CAST(stringColumn as INT) = 1.
  - Filters with an attribute that is an object or is complex. For example, intArray[1] = 1, objectColumn.objectNumber = 1.
  - Filters for which the value is not a literal value. For example, intColumn1 = intColumn2
  - Only Select [Supported Data Types](https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-data-types.html) are supported with the documented limitations.

### HowTo
Include this package in your Spark Applications using:

#### *spark-shell*, *pyspark*, or *spark-submit*
```
> $SPARK_HOME/bin/spark-shell --packages io.minio:spark-select_2.11:2.1
```

#### *sbt*
If you use the [sbt-spark-package plugin](http://github.com/databricks/sbt-spark-package), in your sbt build file, add:
```
spDependencies += "minio/spark-select:2.1"
```
Otherwise,
```
libraryDependencies += "io.minio" % "spark-select_2.11" % "2.1"
```

#### *Maven*
In your pom.xml, add:
```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>io.minio</groupId>
    <artifactId>spark-select_2.11</artifactId>
    <version>2.1</version>
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
spark-shell --jars target/scala-2.11/spark-select-assembly-2.1.jar
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
  .format("minioSelectCSV") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
  .schema(...) // mandatory
  .options(...) // optional
  .load("s3://path/to/my/datafiles")
```

#### *R*
```
read.df("s3://path/to/my/datafiles", "minioSelectCSV", schema)
```

#### *Scala*
```
spark
  .read
  .format("minioSelectCSV") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
  .schema(...) // mandatory
  .options(...) // optional. Examples:
  // .options(Map("quote" -> "\'", "header" -> "true")) or
  // .option("quote", "\'").option("header", "true")
  .load("s3://path/to/my/datafiles")
```

#### *SQL*
```
CREATE TEMPORARY VIEW MyView (number INT, name STRING) USING minioSelectCSV OPTIONS (path "s3://path/to/my/datafiles")
```

### Options
The following options are available when using `minioSelectCSV` and `minioSelectJSON`. If not specified, default values are used.

#### *Options with minioSelectCSV*
| Option | Default | Usage |
|---|---|---|
| `compression` | "none" | Indicates whether compression is used. "gzip", "bzip2" are values supported besides "none".
| `delimiter` | "," | Specifies the field delimiter.
| `quote` | '"' | Specifies the quote character. Specifying an empty string is not supported and results in a malformed XML error.
| `escape` | '"' | Specifies the quote escape character.
| `header` | "true" | "false" specifies that there is no header. "true" specifies that a header is in the first line. Only headers in the first line are supported, and empty lines before a header are not supported.
| `comment` | "#" | Specifies the comment character.

#### *Options with minioSelectJSON*
| Option | Default | Usage |
|---|---|---|
| `compression` | "none" | Indicates whether compression is used. "gzip", "bzip2" are values supported besides "none".
| `multiline` | "false" | "false" specifies that the JSON is in Select LINES format, meaning that each line in the input data contains a single JSON object. "true" specifies that the JSON is in Select DOCUMENT format, meaning that a JSON object can span multiple lines in the input data.

#### *Options with minioSelectParquet*
There are no **options** needed with Parquet files.

### Full Examples

#### *Scala*

Schema with two columns for `CSV`.
```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object app {
  def main(args: Array[String]) {
    val schema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("age", IntegerType, false)
      )
    )

    val df = spark
      .read
      .format("minioSelectCSV")
      .schema(schema)
      .load("s3://sjm-airlines/people.csv")

    println(df.show())

    println(df.select("*").filter("age > 19").show())

  }
}
```

With custom schema for `JSON`.
```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object app {
  def main(args: Array[String]) {
    val schema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("age", IntegerType, false)
      )
    )

    val df = spark
      .read
      .format("minioSelectJSON")
      .schema(schema)
      .load("s3://sjm-airlines/people.json")

    println(df.show())

    println(df.select("*").filter("age > 19").show())

  }
}
```

With custom schema for `Parquet`.
```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object app {
  def main(args: Array[String]) {
    val schema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("age", IntegerType, false)
      )
    )

    val df = spark
      .read
      .format("minioSelectParquet")
      .schema(schema)
      .load("s3://sjm-airlines/people.parquet")

    println(df.show())

    println(df.select("*").filter("age > 19").show())

  }
}
```

#### *Python*

Schema with two columns for `CSV`.
```py
from pyspark.sql import *
from pyspark.sql.types import *

if __name__ == "__main__":
    # create SparkSession
    spark = SparkSession.builder \
        .master("local") \
        .appName("spark-select in python") \
        .getOrCreate()

    # filtered schema
    st = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), False),
    ])

    df = spark \
        .read \
        .format('minioSelectCSV') \
        .schema(st) \
        .load("s3://testbucket/people.csv")

    # show all rows.
    df.show()

    # show only filtered rows.
    df.select("*").filter("age > 19").show()
```

```
> $SPARK_HOME/bin/spark-submit --packages io.minio:spark-select_2.11:2.1 <python-file>
```
