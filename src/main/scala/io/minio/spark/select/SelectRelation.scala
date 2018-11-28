/*
 * Copyright 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.minio.spark.select

// For AmazonS3 client
import com.amazonaws.services.s3.AmazonS3

// Select object
import com.amazonaws.services.s3.model.CSVInput
import com.amazonaws.services.s3.model.CSVOutput
import com.amazonaws.services.s3.model.CompressionType
import com.amazonaws.services.s3.model.ExpressionType
import com.amazonaws.services.s3.model.InputSerialization
import com.amazonaws.services.s3.model.OutputSerialization
import com.amazonaws.services.s3.model.SelectObjectContentEvent
import com.amazonaws.services.s3.model.SelectObjectContentEvent.RecordsEvent
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor
import com.amazonaws.services.s3.model.SelectObjectContentRequest
import com.amazonaws.services.s3.model.SelectObjectContentResult

import org.apache.log4j.Logger

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Abstract relation class for download data from Amazon S3
  */
private[select] case class SelectRelation(
  params: Map[String, String],
  s3Client: AmazonS3,
  userSchema: Option[StructType] = None)(@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan {

  private val logger = Logger.getLogger(getClass)
  private val bucketName = params.getOrElse("bucketName", sys.error("Amazon S3 Bucket has to be provided"))
  private val objectName = params.getOrElse("objectName", sys.error("Amazon S3 Object has to be provided"))
  private val query = params.getOrElse("query", sys.error("Amazon S3 Select query has to be provided"))

  override lazy val schema: StructType = {
    userSchema.getOrElse {
      val fields = new Array[StructField](10)
      var i = 0
      while (i < 10) {
        fields(i) = StructField("dummy", StringType, false)
        i = i + 1
      }
      new StructType(fields)
    }
  }

  override def toString: String = s"SelectRelation($query)"

  override def buildScan(): RDD[Row] = {
    val request = new SelectObjectContentRequest()
    request.setBucketName(bucketName)
    request.setKey(objectName)
    request.setExpression(query)
    request.setExpressionType(ExpressionType.SQL)

    val inputSerialization = new InputSerialization()
    inputSerialization.setCsv(new CSVInput())
    inputSerialization.setCompressionType(CompressionType.NONE)
    request.setInputSerialization(inputSerialization)

    val outputSerialization = new OutputSerialization()
    outputSerialization.setCsv(new CSVOutput())
    request.setOutputSerialization(outputSerialization)

    val result = s3Client.selectObjectContent(request)
    var numRows = 0
    try {
      val resultInputStream = result.getPayload().getRecordsInputStream(
        new SelectObjectContentEventVisitor() {
          override def visit(event: SelectObjectContentEvent.RecordsEvent) = {
            numRows = numRows + 1
          }
        }
      )
    } finally {
      result.close()
    }
    val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
    val emptyRow = RowEncoder(StructType(Seq.empty)).toRow(Row(Seq.empty))
    sqlContext.sparkContext
      .parallelize(1L to numRows, parallelism)
      .map(_ => emptyRow)
      .asInstanceOf[RDD[Row]]
  }

}
