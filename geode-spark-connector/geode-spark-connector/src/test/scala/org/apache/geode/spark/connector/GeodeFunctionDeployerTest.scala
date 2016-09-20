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

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import org.apache.commons.httpclient.HttpClient
import java.io.File


class GeodeFunctionDeployerTest extends FunSuite with Matchers with MockitoSugar {
  val mockHttpClient: HttpClient = mock[HttpClient]
    
  test("jmx url creation") {
    val jmxHostAndPort = "localhost:7070"
    val expectedUrlString = "http://" + jmxHostAndPort + "/gemfire/v1/deployed"
    val gfd = new GeodeFunctionDeployer(mockHttpClient);
    val urlString = gfd.constructURLString(jmxHostAndPort)
    assert(urlString === expectedUrlString)
  }
    
  test("missing jar file") {
    val missingJarFileLocation = "file:///somemissingjarfilethatdoesnot.exist"
    val gfd = new GeodeFunctionDeployer(mockHttpClient);
    intercept[RuntimeException] { gfd.jarFileHandle(missingJarFileLocation)}
  }
  
  test("deploy with missing jar") {
    val missingJarFileLocation = "file:///somemissingjarfilethatdoesnot.exist"
    val gfd = new GeodeFunctionDeployer(mockHttpClient);
    intercept[RuntimeException] {(gfd.deploy("localhost:7070", missingJarFileLocation).contains("Deployed"))}
    intercept[RuntimeException] {(gfd.deploy("localhost", 7070, missingJarFileLocation).contains("Deployed"))}
  }
  
  test("successful mocked deploy") {
    val gfd = new GeodeFunctionDeployer(mockHttpClient);
    val jar = new File("README.md");
    assert(gfd.deploy("localhost:7070", jar).contains("Deployed"))
  }
  

    
}
