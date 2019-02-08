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
