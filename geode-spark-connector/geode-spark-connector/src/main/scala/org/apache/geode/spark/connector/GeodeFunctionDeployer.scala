/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.spark.connector

import java.io.File
import java.net.URL
import org.apache.commons.httpclient.methods.PostMethod
import org.apache.commons.httpclient.methods.multipart.{FilePart, Part, MultipartRequestEntity}
import org.apache.commons.httpclient.HttpClient
import org.apache.spark.Logging

object GeodeFunctionDeployer {
  def main(args: Array[String]) {
    new GeodeFunctionDeployer(new HttpClient()).commandLineRun(args)
  }
}

class GeodeFunctionDeployer(val httpClient:HttpClient) extends Logging {

  def deploy(host: String, port: Int, jarLocation: String): String =
    deploy(host + ":" + port, jarLocation)
  
  def deploy(host: String, port: Int, jar:File): String =
    deploy(host + ":" + port, jar)
  
  def deploy(jmxHostAndPort: String, jarLocation: String): String =
    deploy(jmxHostAndPort, jarFileHandle(jarLocation))
  
  def deploy(jmxHostAndPort: String, jar: File): String = {
    val urlString = constructURLString(jmxHostAndPort)
    val filePost: PostMethod = new PostMethod(urlString)
    val parts: Array[Part] = new Array[Part](1)
    parts(0) = new FilePart("resources", jar)
    filePost.setRequestEntity(new MultipartRequestEntity(parts, filePost.getParams))
    val status: Int = httpClient.executeMethod(filePost)
    "Deployed Jar with status:" + status
  }

  private[connector] def constructURLString(jmxHostAndPort: String) =
    "http://" + jmxHostAndPort + "/gemfire/v1/deployed"

  private[connector]def jarFileHandle(jarLocation: String) = {
    val f: File = new File(jarLocation)
    if (!f.exists()) {
      val errorMessage: String = "Invalid jar file:" + f.getAbsolutePath
      logInfo(errorMessage)
      throw new RuntimeException(errorMessage)
    }
    f
  }
  
  def commandLineRun(args: Array[String]):Unit = {
    val (hostPort: String, jarFile: String) =
    if (args.length < 2) {
      logInfo("JMX Manager Host and Port (example: localhost:7070):")
      val bufferedReader = new java.io.BufferedReader(new java.io.InputStreamReader(System.in))
      val jmxHostAndPort = bufferedReader.readLine()
      logInfo("Location of geode-functions.jar:")
      val functionJarLocation = bufferedReader.readLine()
      (jmxHostAndPort, functionJarLocation)
    } else {
      (args(0), args(1))
    }
    val status = deploy(hostPort, jarFile)
    logInfo(status)
  }
}
