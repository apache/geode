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
package io.pivotal.gemfire.spark.connector.internal

import io.pivotal.gemfire.spark.connector.GemFireConnectionConf
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

class DefaultGemFireConnectionManagerTest extends FunSuite  with Matchers with MockitoSugar {

  test("DefaultGemFireConnectionFactory get/closeConnection") {
    // note: connConf 1-4 share the same set of locators
    val connConf1 = new GemFireConnectionConf(Seq(("host1", 1234)))
    val connConf2 = new GemFireConnectionConf(Seq(("host2", 5678)))
    val connConf3 = new GemFireConnectionConf(Seq(("host1", 1234), ("host2", 5678)))
    val connConf4 = new GemFireConnectionConf(Seq(("host2", 5678), ("host1", 1234)))
    val connConf5 = new GemFireConnectionConf(Seq(("host5", 3333)))

    val props: Map[String, String] = Map.empty
    val mockConnFactory: DefaultGemFireConnectionFactory = mock[DefaultGemFireConnectionFactory]
    val mockConn1 = mock[DefaultGemFireConnection]
    val mockConn2 = mock[DefaultGemFireConnection]
    when(mockConnFactory.newConnection(connConf3.locators, props)).thenReturn(mockConn1)
    when(mockConnFactory.newConnection(connConf5.locators, props)).thenReturn(mockConn2)

    assert(DefaultGemFireConnectionManager.getConnection(connConf3)(mockConnFactory) == mockConn1)
    // note: following 3 lines do not trigger connFactory.newConnection(...)
    assert(DefaultGemFireConnectionManager.getConnection(connConf1)(mockConnFactory) == mockConn1)
    assert(DefaultGemFireConnectionManager.getConnection(connConf2)(mockConnFactory) == mockConn1)
    assert(DefaultGemFireConnectionManager.getConnection(connConf4)(mockConnFactory) == mockConn1)
    assert(DefaultGemFireConnectionManager.getConnection(connConf5)(mockConnFactory) == mockConn2)

    // connFactory.newConnection(...) were invoked only twice
    verify(mockConnFactory, times(1)).newConnection(connConf3.locators, props)
    verify(mockConnFactory, times(1)).newConnection(connConf5.locators, props)
    assert(DefaultGemFireConnectionManager.connections.size == 3)

    DefaultGemFireConnectionManager.closeConnection(connConf1)
    assert(DefaultGemFireConnectionManager.connections.size == 1)
    DefaultGemFireConnectionManager.closeConnection(connConf5)
    assert(DefaultGemFireConnectionManager.connections.isEmpty)
  }
  
  test("DefaultGemFireConnectionFactory newConnection(...) throws RuntimeException") {
    val connConf1 = new GemFireConnectionConf(Seq(("host1", 1234)))
    val props: Map[String, String] = Map.empty
    val mockConnFactory: DefaultGemFireConnectionFactory = mock[DefaultGemFireConnectionFactory]
    when(mockConnFactory.newConnection(connConf1.locators, props)).thenThrow(new RuntimeException())
    intercept[RuntimeException] { DefaultGemFireConnectionManager.getConnection(connConf1)(mockConnFactory) }
    verify(mockConnFactory, times(1)).newConnection(connConf1.locators, props)
  }

  test("DefaultGemFireConnectionFactory close() w/ non-exist connection") {
    val props: Map[String, String] = Map.empty
    val mockConnFactory: DefaultGemFireConnectionFactory = mock[DefaultGemFireConnectionFactory]
    val connConf1 = new GemFireConnectionConf(Seq(("host1", 1234)))
    val connConf2 = new GemFireConnectionConf(Seq(("host2", 5678)))
    val mockConn1 = mock[DefaultGemFireConnection]
    when(mockConnFactory.newConnection(connConf1.locators, props)).thenReturn(mockConn1)
    assert(DefaultGemFireConnectionManager.getConnection(connConf1)(mockConnFactory) == mockConn1)
    assert(DefaultGemFireConnectionManager.connections.size == 1)
    // connection does not exists in the connection manager
    DefaultGemFireConnectionManager.closeConnection(connConf2)
    assert(DefaultGemFireConnectionManager.connections.size == 1)
  }

}
