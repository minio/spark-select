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

// For AmazonS3 client
import com.amazonaws.services.s3.AmazonS3

// Select object
import com.amazonaws.services.s3.model.SelectObjectContentResult
import com.amazonaws.services.s3.model.SelectObjectContentEvent
import com.amazonaws.services.s3.model.SelectObjectContentEvent.RecordsEvent

import org.apache.log4j.Logger
import org.apache.commons.csv.{CSVParser, CSVFormat, CSVRecord, QuoteMode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Abstract relation class for download data from Amazon S3
  */
private[select] case class SelectRelation(
  sResult: SelectObjectContentResult,
  csvFormat: CSVFormat,
  userSchema: Option[StructType] = None)(@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan {

  private val logger = Logger.getLogger(getClass)

  override lazy val schema: StructType = userSchema.getOrElse(inferSchema())

  private lazy val rows: List[Array[String]] = {
    var records : List[Array[String]] = null
    val br = new BufferedReader(new InputStreamReader(sResult.getPayload().getRecordsInputStream()))
    var line : String = null
    while ( {line = br.readLine(); line != null}) {
      val r = CSVParser.parse(line, csvFormat).getRecords
      if (r.isEmpty) {
        logger.warn(s"Ignoring empty line: $line")
      } else {
        records :+ r.toArray
      }
    }
    br.close()
    records
  }

  override def toString: String = s"SelectRelation()"

  override def buildScan: RDD[Row] = {
    val aSchema = schema
    sqlContext.sparkContext.makeRDD(rows).mapPartitions{ iter =>
      iter.map { m =>
        var index = 0
        val rowArray = new Array[Any](aSchema.fields.length)
        while(index < aSchema.fields.length) {
          val field = aSchema.fields(index)
          rowArray(index) = TypeCast.castTo(field.name, field.dataType, field.nullable)
          index += 1
        }
        Row.fromSeq(rowArray)
      }
    }

    }

  private def inferSchema(): StructType = {
    val fields = new Array[StructField](rows(0).length)
    var index = 0
    while (index < rows(0).length) {
      fields(index) = StructField("C$index", StringType, nullable = true)
      index += 1
    }
    new StructType(fields)
  }
}
