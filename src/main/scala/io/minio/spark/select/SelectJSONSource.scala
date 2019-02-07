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

// Java standard libraries
import java.io.File

// Spark internal libraries
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.sources.DataSourceRegister

class SelectJSONSource
  extends SchemaRelationProvider
  with DataSourceRegister {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for JSON data."))
  }

  /**
   * Short alias for spark-select data source.
   */
  override def shortName(): String = "minioSelectJSON"

  override def createRelation(sqlContext: SQLContext, params: Map[String, String], schema: StructType): SelectJSONRelation = {
    val path = checkPath(params)
    SelectJSONRelation(Some(path), params, schema)(sqlContext)
  }
}
