import org.apache.spark.sql._
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("name", StringType, true),
    StructField("age", IntegerType, false)
  )
)

var df = spark.read.format("minioSelectCSV").schema(schema).option("endpoint", "http://127.0.0.1:9000").option("access_key", "minio").option("secret_key", "minio123").option("path_style_access", "true").load("s3://sjm-airlines/people.csv")

println(df.show())

println(df.select("*").filter("name not like '%Justin%'").show())
