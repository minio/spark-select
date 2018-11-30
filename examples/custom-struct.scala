import org.apache.spark.sql._
import org.apache.spark.sql.types._

val struct = StructType(
  StructField("Category", IntegerType, true) ::
    StructField("Mandatory", StringType, false) ::
    StructField("Maximum", IntegerType, false) :: Nil)

var df = spark.read.format("selectCSV").schema(struct).load("s3://sjm-airlines/test.csv")

println(df.count())

println(df.show())
