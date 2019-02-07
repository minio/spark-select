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

// For BasicAWSCredentials
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

private[spark] object Credentials {
  private def staticCredentialsProvider(credentials: AWSCredentials): AWSCredentialsProvider = {
    new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = {}
    }
  }

  def load(params: Map[String, String]): AWSCredentialsProvider = {
    val accessKey = params.getOrElse(s"access_key", null)
    val secretKey = params.getOrElse(s"secret_key", null)
    if (accessKey != null && secretKey != null) {
      Some(staticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
    } else {
      None
    }
  }.getOrElse {
    // Finally, fall back on the instance profile provider
    new DefaultAWSCredentialsProviderChain()
  }
}
