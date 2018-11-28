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

import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  private val logger = Logger.getLogger(getClass)

  private val s3Client =
    AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("minio", "minio123")))
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(
      new EndpointConfiguration("http://localhost:9000", "us-east-1"))
      .build()

  /**
    * Reading files from Amazon S3.
    */
  override def createRelation(sqlContext: SQLContext, params: Map[String, String]): SelectRelation = {
    SelectRelation(params, s3Client, None)(sqlContext)
  }

  /**
    * Reading files from Amazon S3.
    */
  override def createRelation(sqlContext: SQLContext, params: Map[String, String], schema: StructType): SelectRelation = {
    SelectRelation(params, s3Client, Some(schema))(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): SelectRelation = {
    sys.error("Write is not yet supported")
  }
}
