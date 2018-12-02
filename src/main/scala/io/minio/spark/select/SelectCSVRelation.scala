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

import java.io.InputStreamReader
import java.io.BufferedReader

// Import all utilities
import io.minio.spark.select.util._

// Apache commons
import org.apache.commons.csv.{CSVFormat, QuoteMode}

// For AmazonS3 client
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder

// For BasicAWSCredentials
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

// Select API
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.CSVInput
import com.amazonaws.services.s3.model.CSVOutput
import com.amazonaws.services.s3.model.CompressionType
import com.amazonaws.services.s3.model.ExpressionType
import com.amazonaws.services.s3.model.InputSerialization
import com.amazonaws.services.s3.model.OutputSerialization
import com.amazonaws.services.s3.model.SelectObjectContentRequest
import com.amazonaws.services.s3.model.SelectObjectContentResult
import com.amazonaws.services.s3.model.SelectObjectContentEvent
import com.amazonaws.services.s3.model.SelectObjectContentEvent.RecordsEvent
import com.amazonaws.services.s3.model.FileHeaderInfo

import org.apache.log4j.Logger
import org.apache.commons.csv.{CSVParser, CSVFormat, CSVRecord, QuoteMode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources.{BaseRelation, TableScan, PrunedScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import org.apache.hadoop.conf.Configuration

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
    with PrunedScan {

  private val logger = Logger.getLogger(getClass)
  private val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
  private val pathStyleAccess = hadoopConfiguration.get(s"fs.s3a.path.style.access", "false") == "true"
  private val s3Client =
    AmazonS3ClientBuilder.standard()
      .withCredentials(loadFromHadoop(hadoopConfiguration))
      .withPathStyleAccessEnabled(pathStyleAccess)
      .withEndpointConfiguration(
      new EndpointConfiguration(hadoopConfiguration.get(s"fs.s3a.endpoint", null), "us-east-1"))
      .build()

  private val defaultCsvFormat =
    CSVFormat.DEFAULT.withRecordSeparator(System.getProperty("line.separator", "\n"))

  val csvFormat = customCSVFormat(params, defaultCsvFormat)

  private def customCSVFormat(params: Map[String, String], csvFormat: CSVFormat): CSVFormat = {

    // delimiter :  "," :  Specifies the field delimiter.
    // quote : '\"' : Specifies the quote character. Specifying an empty string is not supported and results in a malformed XML error.
    // escape : '\\' : Specifies the escape character.
    // comment : "#" : Specifies the comment character. The comment indicator cannot be disabled.
    //                 In other words, a value of \u0000 is not supported.
    val delimiter = params.getOrElse("delimiter", ",")
    val quote = params.getOrElse("quote", "\"")
    val escape = params.getOrElse("escape", "\\")
    val comment = params.getOrElse("comment", "#")

    csvFormat
      .withDelimiter(delimiter.charAt(0))
      .withQuote(quote.charAt(0))
      .withEscape(escape.charAt(0))
      .withCommentMarker(comment.charAt(0))
  }

  private def staticCredentialsProvider(credentials: AWSCredentials): AWSCredentialsProvider = {
    new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = {}
    }
  }

  private def loadFromHadoop(hadoopCfg: Configuration): AWSCredentialsProvider = {
    val accessKey = hadoopCfg.get(s"fs.s3a.access.key", null)
    val secretKey = hadoopCfg.get(s"fs.s3a.secret.key", null)
    if (accessKey != null && secretKey != null) {
      Some(staticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
    } else {
      None
    }
  }.getOrElse {
    // Finally, fall back on the instance profile provider
    new DefaultAWSCredentialsProviderChain()
  }

  private def resolveQuery(schema: StructType): String = {
    if (schema == null) {
      // With no schema we are not filtering any column names.
      "select * from S3Object"
    } else {
      var index = 0
      var selector = new Array[Any](schema.fields.length)
      while (index < schema.fields.length) {
        selector(index) = schema.fields(index).name
        index += 1
      }
      "select " ++ selector.mkString(",") ++ " from S3Object"
    }
  }

  private def selectRequest(location: Option[String], params: Map[String, String]): SelectObjectContentRequest = {
    val s3URI = new AmazonS3URI(location.getOrElse(""))

    // TODO support
    //
    // compression : "none" : Indicates whether compression is used. "gzip" is the only setting supported besides "none".
    // header : "false" : "false" specifies that there is no header. "true" specifies that a header is in
    //                    the first line. Only headers in the first line are supported, and empty lines
    //                    before a header are not supported.
    // nullValue : "" :

    new SelectObjectContentRequest() { request =>
      request.setBucketName(s3URI.getBucket())
      request.setKey(s3URI.getKey())
      request.setExpression(resolveQuery(userSchema))
      request.setExpressionType(ExpressionType.SQL)

      val inputSerialization = new InputSerialization()
      val csvInput = new CSVInput()
      csvInput.withFileHeaderInfo(FileHeaderInfo.USE)
      csvInput.withRecordDelimiter('\n')
      csvInput.withFieldDelimiter(params.getOrElse("delimiter", ","))
      inputSerialization.setCsv(csvInput)
      inputSerialization.setCompressionType(CompressionType.NONE)
      request.setInputSerialization(inputSerialization)

      val outputSerialization = new OutputSerialization()
      val csvOutput = new CSVOutput()
      csvOutput.withRecordDelimiter('\n')
      csvOutput.withFieldDelimiter(params.getOrElse("delimiter", ","))
      outputSerialization.setCsv(csvOutput)
      request.setOutputSerialization(outputSerialization)
    }
  }

  override lazy val schema: StructType = {
    Option(userSchema).getOrElse(inferSchema())
  }

  private lazy val rows: List[Array[String]] = {
    var records = new ListBuffer[Array[String]]()
    val br = new BufferedReader(new InputStreamReader(s3Client.selectObjectContent(selectRequest(location, params))
      .getPayload()
      .getRecordsInputStream()))
    var line : String = null
    while ( {line = br.readLine(); line != null}) {
      records += line.split(",")
    }
    br.close()
    records.toList
  }

  override def toString: String = s"SelectCSVRelation()"

  private def tokenRDD(schema: StructType): RDD[Row] = {
    sqlContext.sparkContext.makeRDD(rows).mapPartitions{ iter =>
      iter.map { m =>
        var index = 0
        val rowArray = new Array[Any](schema.fields.length)
        while (index < schema.fields.length) {
          val field = schema.fields(index)
          rowArray(index) = TypeCast.castTo(m(index), field.dataType, field.nullable)
          index += 1
        }
        Row.fromSeq(rowArray)
      }
    }
  }

  override def buildScan(): RDD[Row] = {
    tokenRDD(schema)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val requiredFields = requiredColumns.map(schema(_))
    val schemaFields = schema.fields
    // Check if requested fields are same as schema
    if (schemaFields.deep == requiredFields.deep) {
      buildScan()
    } else {
      tokenRDD(StructType(requiredFields))
    }
  }

  private def inferSchema(): StructType = {
    val fields = new Array[StructField](rows(0).length)
    var index = 0
    while (index < rows(0).length) {
      fields(index) = StructField(s"s._${index+1}", StringType, nullable = true)
      index += 1
    }
    new StructType(fields)
  }
}
