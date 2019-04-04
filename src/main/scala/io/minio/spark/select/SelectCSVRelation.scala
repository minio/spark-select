/*
 * Copyright 2018 MinIO, Inc.
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

import scala.collection.JavaConversions.asScalaBuffer

import java.io.InputStreamReader
import java.io.BufferedReader

// Import all utilities
import io.minio.spark.select.util._

// Apache commons
import org.apache.commons.csv.{CSVFormat, QuoteMode}

// For AmazonS3 client
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary

import org.apache.commons.csv.{CSVParser, CSVFormat, CSVRecord, QuoteMode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.{ListBuffer, ArrayBuffer}

/**
  * Abstract relation class to download data from S3 compatible storage
  */
case class SelectCSVRelation protected[spark] (
  location: Option[String],
  params: Map[String, String],
  userSchema: StructType = null)(@transient val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan
    with PrunedScan
    with PrunedFilteredScan {

  private val API_PATH_STYLE_ACCESS = s"fs.s3a.path.style.access"
  private val SERVER_ENDPOINT = s"fs.s3a.endpoint"
  private val SERVER_REGION = s"fs.s3a.region"

  private val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
  private val pathStyleAccess = hadoopConfiguration.get(API_PATH_STYLE_ACCESS, "false") == "true"
  private val endpoint = hadoopConfiguration.get(SERVER_ENDPOINT, "https://s3.amazonaws.com")
  private val region = hadoopConfiguration.get(SERVER_REGION, "us-east-1")
  private val s3Client =
    AmazonS3ClientBuilder.standard()
      .withCredentials(Credentials.load(location, hadoopConfiguration))
      .withPathStyleAccessEnabled(pathStyleAccess)
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
      .build()

  override lazy val schema: StructType = Option(userSchema).getOrElse({
      // With no schema we return error.
      throw new RuntimeException(s"Schema cannot be empty")
  })

  private def getRows(schema: StructType, filters: Array[Filter]): Seq[Row] = {
    var records = new ListBuffer[Row]
    var req = new ListObjectsV2Request()
    var result = new ListObjectsV2Result()
    var s3URI = new AmazonS3URI(location.getOrElse(""))

    req.withBucketName(s3URI.getBucket())
    req.withPrefix(s3URI.getKey().stripSuffix("*"))
    req.withMaxKeys(1000)

    do {
      result = s3Client.listObjectsV2(req)
      asScalaBuffer(result.getObjectSummaries()).foreach(objectSummary => {
        val br = new BufferedReader(new InputStreamReader(
          s3Client.selectObjectContent(
            Select.requestCSV(
              objectSummary.getBucketName(),
              objectSummary.getKey(),
              params,
              schema,
              filters,
              hadoopConfiguration)
          ).getPayload().getRecordsInputStream()))
        var line : String = null
        while ( {line = br.readLine(); line != null}) {
          var row = new Array[Any](schema.fields.length)
          var rowValues = line.split(params.getOrElse("delimiter", ","))
          var index = 0
          while (index < rowValues.length) {
            val field = schema.fields(index)
            row(index) = TypeCast.castTo(rowValues(index), field.dataType,
              field.nullable)
            index += 1
          }
          records += Row.fromSeq(row)
        }
        br.close()
      })
      req.setContinuationToken(result.getNextContinuationToken())
    } while (result.isTruncated())
    records.toList
  }

  override def toString: String = s"SelectCSVRelation()"

  private def tokenRDD(schema: StructType, filters: Array[Filter]): RDD[Row] = {
    sqlContext.sparkContext.makeRDD(getRows(schema, filters))
  }

  override def buildScan(): RDD[Row] = {
    tokenRDD(schema, null)
  }

  override def buildScan(columns: Array[String]): RDD[Row] = {
    tokenRDD(pruneSchema(schema, columns), null)
  }

  override def buildScan(columns: Array[String], filters: Array[Filter]): RDD[Row] = {
    tokenRDD(pruneSchema(schema, columns), filters)
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }
}
