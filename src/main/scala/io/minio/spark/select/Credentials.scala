/*
 * Copyright 2019 MinIO, Inc.
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

import java.net.URI

// For BasicAWSCredentials
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import org.apache.hadoop.conf.Configuration

private[spark] object Credentials {
  private def staticCredentialsProvider(credentials: AWSCredentials): AWSCredentialsProvider = {
    new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = {}
    }
  }

  def load(location: Option[String], hadoopConfiguration: Configuration): AWSCredentialsProvider = {
    val uri = new URI(location.getOrElse(""))
    val uriScheme = uri.getScheme

    uriScheme match {
      case "s3" | "s3a" =>
        // This matches what S3A does, with one exception: we don't
        // support anonymous credentials. First, try to parse from URI:
        Option(uri.getUserInfo).flatMap { userInfo =>
          if (userInfo.contains(":")) {
            val Array(accessKey, secretKey) = userInfo.split(":")
            Some(staticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
          } else {
            None
          }
        }.orElse {
          val accessKey = hadoopConfiguration.get(s"fs.s3a.access.key", null)
          val secretKey = hadoopConfiguration.get(s"fs.s3a.secret.key", null)
          if (accessKey != null && secretKey != null) {
            Some(staticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
          } else {
            None
          }
        }.getOrElse {
          // Finally, fall back on the instance profile provider
          new DefaultAWSCredentialsProviderChain()
        }
      case other =>
        throw new IllegalArgumentException(s"Unrecognized scheme $other; expected s3, or s3a")
    }
  }
}
