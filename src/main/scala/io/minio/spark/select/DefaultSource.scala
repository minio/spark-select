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

import java.io.File

// For BasicAWSCredentials
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

// For AmazonS3 client
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder

// Select API
import com.amazonaws.services.s3.model.CSVInput
import com.amazonaws.services.s3.model.CSVOutput
import com.amazonaws.services.s3.model.CompressionType
import com.amazonaws.services.s3.model.ExpressionType
import com.amazonaws.services.s3.model.InputSerialization
import com.amazonaws.services.s3.model.OutputSerialization
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor
import com.amazonaws.services.s3.model.SelectObjectContentRequest
import com.amazonaws.services.s3.model.SelectObjectContentResult

import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider with DataSourceRegister {
  private val logger = Logger.getLogger(getClass)

  override def shortName: String = "selectCSV"

  override def equals(other: Any): Boolean = other match {
    case _: DefaultSource => true
    case _ => false
  }

  private val defaultCsvFormat =
    CSVFormat.DEFAULT.withRecordSeparator(System.getProperty("line.separator", "\n"))

  private val s3Client =
    AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("minio", "minio123")))
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(
      new EndpointConfiguration("http://localhost:9000", "us-east-1"))
      .build()

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

  private def selectRequest(params: Map[String, String]): SelectObjectContentRequest = {
    val bucketName = params.getOrElse("bucketName", sys.error("Amazon S3 Bucket has to be provided"))
    val objectName = params.getOrElse("objectName", sys.error("Amazon S3 Object has to be provided"))
    val query = params.getOrElse("query", sys.error("Amazon S3 Select query has to be provided"))

    // TODO support
    //
    // compression : "none" : Indicates whether compression is used. "gzip" is the only setting supported besides "none".
    // header : "false" : "false" specifies that there is no header. "true" specifies that a header is in
    //                    the first line. Only headers in the first line are supported, and empty lines
    //                    before a header are not supported.
    // nullValue : "" :

    new SelectObjectContentRequest() { request =>
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
    }
  }

  /**
    * Reading files from Amazon S3.
    */
  override def createRelation(sqlContext: SQLContext, params: Map[String, String]): SelectRelation = {
    SelectRelation(s3Client.selectObjectContent(selectRequest(params)), defaultCsvFormat, None)(sqlContext)
  }

  /**
    * Reading files from Amazon S3.
    */
  override def createRelation(sqlContext: SQLContext, params: Map[String, String], schema: StructType): SelectRelation = {
    SelectRelation(s3Client.selectObjectContent(selectRequest(params)), defaultCsvFormat, Some(schema))(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): SelectRelation = {
    sys.error("Write is not supported")
  }
}
