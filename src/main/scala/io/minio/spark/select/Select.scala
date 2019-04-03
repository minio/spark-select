/*
 * Copyright 2019 Minio, Inc.
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

import org.apache.hadoop.conf.Configuration

// Select API
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.JSONInput
import com.amazonaws.services.s3.model.JSONType
import com.amazonaws.services.s3.model.CSVInput
import com.amazonaws.services.s3.model.CSVOutput
import com.amazonaws.services.s3.model.ParquetInput
import com.amazonaws.services.s3.model.CompressionType
import com.amazonaws.services.s3.model.ExpressionType
import com.amazonaws.services.s3.model.SSECustomerKey
import com.amazonaws.services.s3.model.InputSerialization
import com.amazonaws.services.s3.model.OutputSerialization
import com.amazonaws.services.s3.model.SelectObjectContentRequest
import com.amazonaws.services.s3.model.SelectObjectContentResult
import com.amazonaws.services.s3.model.SelectObjectContentEvent
import com.amazonaws.services.s3.model.SelectObjectContentEvent.RecordsEvent
import com.amazonaws.services.s3.model.FileHeaderInfo

import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._

private[spark] object Select {
  private val SERVER_ENCRYPTION_ALGORITHM = s"fs.s3a.server-side-encryption-algorithm"
  private val SERVER_ENCRYPTION_KEY = s"fs.s3a.server-side-encryption.key"

  private def compressionType(params: Map[String, String]): CompressionType = {
    params.getOrElse("compression", "none") match {
      case "none" => CompressionType.NONE
      case "gzip" => CompressionType.GZIP
      case "bzip2" => CompressionType.BZIP2
    }
  }

  private def jsonType(params: Map[String, String]): JSONType = {
    params.getOrElse("multiline", "false") match {
      case "false" => JSONType.LINES
      case "true" => JSONType.DOCUMENT
    }
  }

  private def headerInfo(params: Map[String, String]): FileHeaderInfo = {
    params.getOrElse("header", "true") match {
      case "false" => FileHeaderInfo.NONE
      case "true" => FileHeaderInfo.USE
    }
  }

  private def sseCustomerKey(algo: String, key: String): SSECustomerKey = {
    algo match {
      case "SSE-C" =>
        if (key != null) {
          new SSECustomerKey(key)
        } else {
          null
        }
      case other =>
        throw new IllegalArgumentException(s"Unrecognized algorithm $algo; expected SSE-C")
    }
  }

  def requestParquet(bucket: String, key: String, params: Map[String, String],
    schema: StructType, filters: Array[Filter],
    hadoopConfiguration: Configuration): SelectObjectContentRequest = {

    new SelectObjectContentRequest() { request =>
      request.setBucketName(bucket)
      request.setKey(key)
      request.setExpression(FilterPushdown.queryFromSchema(schema, filters))
      request.setExpressionType(ExpressionType.SQL)
      val algo = hadoopConfiguration.get(SERVER_ENCRYPTION_ALGORITHM, null)
      if (algo != null) {
        request.withSSECustomerKey(sseCustomerKey(algo,
          hadoopConfiguration.get(SERVER_ENCRYPTION_KEY, null)))
      }

      val inputSerialization = new InputSerialization()
      val parquetInput = new ParquetInput()
      inputSerialization.setParquet(parquetInput)
      request.setInputSerialization(inputSerialization)

      val outputSerialization = new OutputSerialization()
      val csvOutput = new CSVOutput()
      outputSerialization.setCsv(csvOutput)
      request.setOutputSerialization(outputSerialization)
    }
  }

  def requestJSON(bucket: String, key: String, params: Map[String, String],
    schema: StructType, filters: Array[Filter],
    hadoopConfiguration: Configuration): SelectObjectContentRequest = {

    new SelectObjectContentRequest() { request =>
      request.setBucketName(bucket)
      request.setKey(key)
      request.setExpression(FilterPushdown.queryFromSchema(schema, filters))
      request.setExpressionType(ExpressionType.SQL)
      val algo = hadoopConfiguration.get(SERVER_ENCRYPTION_ALGORITHM, null)
      if (algo != null) {
        request.withSSECustomerKey(sseCustomerKey(algo,
          hadoopConfiguration.get(SERVER_ENCRYPTION_KEY, null)))
      }

      val inputSerialization = new InputSerialization()
      val jsonInput = new JSONInput()
      jsonInput.withType(jsonType(params))
      inputSerialization.setJson(jsonInput)
      inputSerialization.setCompressionType(compressionType(params))
      request.setInputSerialization(inputSerialization)

      val outputSerialization = new OutputSerialization()
      val csvOutput = new CSVOutput()
      outputSerialization.setCsv(csvOutput)
      request.setOutputSerialization(outputSerialization)
    }
  }


  def requestCSV(bucket: String, key: String, params: Map[String, String],
    schema: StructType, filters: Array[Filter],
    hadoopConfiguration: Configuration): SelectObjectContentRequest = {
    new SelectObjectContentRequest() { request =>
      request.setBucketName(bucket)
      request.setKey(key)
      request.setExpression(FilterPushdown.queryFromSchema(schema, filters))
      request.setExpressionType(ExpressionType.SQL)
      val algo = hadoopConfiguration.get(SERVER_ENCRYPTION_ALGORITHM, null)
      if (algo != null) {
        request.withSSECustomerKey(sseCustomerKey(algo,
          hadoopConfiguration.get(SERVER_ENCRYPTION_KEY, null)))
      }

      val inputSerialization = new InputSerialization()
      val csvInput = new CSVInput()
      csvInput.withFileHeaderInfo(headerInfo(params))
      csvInput.withRecordDelimiter('\n')
      csvInput.withQuoteCharacter(params.getOrElse(s"quote", "\""))
      csvInput.withQuoteEscapeCharacter(params.getOrElse(s"escape", "\""))
      csvInput.withComments(params.getOrElse(s"comment", "#"))
      csvInput.withFieldDelimiter(params.getOrElse(s"delimiter", ","))
      inputSerialization.setCsv(csvInput)
      inputSerialization.setCompressionType(compressionType(params))
      request.setInputSerialization(inputSerialization)

      val outputSerialization = new OutputSerialization()
      val csvOutput = new CSVOutput()
      csvOutput.withRecordDelimiter('\n')
      csvOutput.withFieldDelimiter(params.getOrElse("delimiter", ","))
      outputSerialization.setCsv(csvOutput)
      request.setOutputSerialization(outputSerialization)
    }
  }
}
