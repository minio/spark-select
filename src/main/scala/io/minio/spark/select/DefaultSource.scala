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

import org.apache.log4j.Logger
import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends DataSourceRegister with RelationProvider with SchemaRelationProvider {
  private val logger = Logger.getLogger(getClass)

  override def shortName(): String = "selectCSV"

  private val defaultCsvFormat =
    CSVFormat.DEFAULT.withRecordSeparator(System.getProperty("line.separator", "\n"))

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

  override def createRelation(sqlContext: SQLContext, params: Map[String, String]): BaseRelation = {
    SelectRelation(params, defaultCsvFormat, None)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, params: Map[String, String], schema: StructType): BaseRelation = {
    SelectRelation(params, defaultCsvFormat, Some(schema))(sqlContext)
  }

}
