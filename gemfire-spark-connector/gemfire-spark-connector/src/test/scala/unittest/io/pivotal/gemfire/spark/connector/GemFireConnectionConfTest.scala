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
package unittest.io.pivotal.gemfire.spark.connector

import org.apache.spark.SparkConf
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FunSuite}
import io.pivotal.gemfire.spark.connector._

class GemFireConnectionConfTest extends FunSuite with Matchers with MockitoSugar {

  test("apply(SparkConf) w/ GemFireLocator property and empty gemfireProps") {
    val (host1, port1) = ("host1", 1234)
    val (host2, port2) = ("host2", 5678)
    val conf = new SparkConf().set(GemFireLocatorPropKey, s"$host1[$port1],$host2[$port2]")
    val connConf = GemFireConnectionConf(conf)
    assert(connConf.locators == Seq((host1, port1),(host2, port2)))
    assert(connConf.gemfireProps.isEmpty)
  }
  
  test("apply(SparkConf) w/ GemFireLocator property and gemfire properties") {
    val (host1, port1) = ("host1", 1234)
    val (host2, port2) = ("host2", 5678)
    val (propK1, propV1) = ("ack-severe-alert-threshold", "1")
    val (propK2, propV2) = ("ack-wait-threshold", "10")
    val conf = new SparkConf().set(GemFireLocatorPropKey, s"$host1[$port1],$host2[$port2]")
                              .set(s"spark.gemfire.$propK1", propV1).set(s"spark.gemfire.$propK2", propV2)
    val connConf = GemFireConnectionConf(conf)
    assert(connConf.locators == Seq((host1, port1),(host2, port2)))
    assert(connConf.gemfireProps == Map(propK1 -> propV1, propK2 -> propV2))
  }

  test("apply(SparkConf) w/o GemFireLocator property") {
    intercept[RuntimeException] { GemFireConnectionConf(new SparkConf()) }
  }

  test("apply(SparkConf) w/ invalid GemFireLocator property") {
    val conf = new SparkConf().set(GemFireLocatorPropKey, "local^host:1234")
    intercept[Exception] { GemFireConnectionConf(conf) }
  }

  test("apply(locatorStr, gemfireProps) w/ valid locatorStr and non gemfireProps") {
    val (host1, port1) = ("host1", 1234)
    val connConf = GemFireConnectionConf(s"$host1:$port1")
    assert(connConf.locators == Seq((host1, port1)))
    assert(connConf.gemfireProps.isEmpty)
  }

  test("apply(locatorStr, gemfireProps) w/ valid locatorStr and non-empty gemfireProps") {
    val (host1, port1) = ("host1", 1234)
    val (host2, port2) = ("host2", 5678)
    val (propK1, propV1) = ("ack-severe-alert-threshold", "1")
    val (propK2, propV2) = ("ack-wait-threshold", "10")
    val props = Map(propK1 -> propV1, propK2 -> propV2)
    val connConf = GemFireConnectionConf(s"$host1:$port1,$host2:$port2", props)
    assert(connConf.locators == Seq((host1, port1),(host2, port2)))
    assert(connConf.gemfireProps == props)
  }

  test("apply(locatorStr, gemfireProps) w/ invalid locatorStr") {
    intercept[Exception] { GemFireConnectionConf("local~host:4321") }
  }

  test("constructor w/ empty (host,port) pairs") {
    intercept[IllegalArgumentException] { new GemFireConnectionConf(Seq.empty) }
  }

  test("getConnection() normal") {
    implicit val mockFactory = mock[GemFireConnectionManager]
    val mockConnection = mock[GemFireConnection]
    when(mockFactory.getConnection(org.mockito.Matchers.any[GemFireConnectionConf])).thenReturn(mockConnection)
    val connConf = GemFireConnectionConf("localhost:1234")
    assert(connConf.getConnection == mockConnection)
    verify(mockFactory).getConnection(connConf)
  }

  test("getConnection() failure") {
    implicit val mockFactory = mock[GemFireConnectionManager]
    when(mockFactory.getConnection(org.mockito.Matchers.any[GemFireConnectionConf])).thenThrow(new RuntimeException)
    val connConf = GemFireConnectionConf("localhost:1234")
    intercept[RuntimeException] { connConf.getConnection }
    verify(mockFactory).getConnection(connConf)
  }

}
