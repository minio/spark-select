import org.apache.spark.sql._
import org.apache.spark.sql.types._

val struct = StructType(
  StructField("name", StringType, true) ::
    StructField("age", IntegerType, false) :: Nil)

var df = spark.read.format("selectJSON").schema(struct).load("s3://sjm-airlines/people.json")

println(df.count())

println(df.show())
