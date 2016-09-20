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
package unittest.org.apache.geode.spark.connector

import org.apache.spark.SparkConf
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FunSuite}
import org.apache.geode.spark.connector._

class GeodeConnectionConfTest extends FunSuite with Matchers with MockitoSugar {

  test("apply(SparkConf) w/ GeodeLocator property and empty geodeProps") {
    val (host1, port1) = ("host1", 1234)
    val (host2, port2) = ("host2", 5678)
    val conf = new SparkConf().set(GeodeLocatorPropKey, s"$host1[$port1],$host2[$port2]")
    val connConf = GeodeConnectionConf(conf)
    assert(connConf.locators == Seq((host1, port1),(host2, port2)))
    assert(connConf.geodeProps.isEmpty)
  }
  
  test("apply(SparkConf) w/ GeodeLocator property and geode properties") {
    val (host1, port1) = ("host1", 1234)
    val (host2, port2) = ("host2", 5678)
    val (propK1, propV1) = ("ack-severe-alert-threshold", "1")
    val (propK2, propV2) = ("ack-wait-threshold", "10")
    val conf = new SparkConf().set(GeodeLocatorPropKey, s"$host1[$port1],$host2[$port2]")
                              .set(s"spark.geode.$propK1", propV1).set(s"spark.geode.$propK2", propV2)
    val connConf = GeodeConnectionConf(conf)
    assert(connConf.locators == Seq((host1, port1),(host2, port2)))
    assert(connConf.geodeProps == Map(propK1 -> propV1, propK2 -> propV2))
  }

  test("apply(SparkConf) w/o GeodeLocator property") {
    intercept[RuntimeException] { GeodeConnectionConf(new SparkConf()) }
  }

  test("apply(SparkConf) w/ invalid GeodeLocator property") {
    val conf = new SparkConf().set(GeodeLocatorPropKey, "local^host:1234")
    intercept[Exception] { GeodeConnectionConf(conf) }
  }

  test("apply(locatorStr, geodeProps) w/ valid locatorStr and non geodeProps") {
    val (host1, port1) = ("host1", 1234)
    val connConf = GeodeConnectionConf(s"$host1:$port1")
    assert(connConf.locators == Seq((host1, port1)))
    assert(connConf.geodeProps.isEmpty)
  }

  test("apply(locatorStr, geodeProps) w/ valid locatorStr and non-empty geodeProps") {
    val (host1, port1) = ("host1", 1234)
    val (host2, port2) = ("host2", 5678)
    val (propK1, propV1) = ("ack-severe-alert-threshold", "1")
    val (propK2, propV2) = ("ack-wait-threshold", "10")
    val props = Map(propK1 -> propV1, propK2 -> propV2)
    val connConf = GeodeConnectionConf(s"$host1:$port1,$host2:$port2", props)
    assert(connConf.locators == Seq((host1, port1),(host2, port2)))
    assert(connConf.geodeProps == props)
  }

  test("apply(locatorStr, geodeProps) w/ invalid locatorStr") {
    intercept[Exception] { GeodeConnectionConf("local~host:4321") }
  }

  test("constructor w/ empty (host,port) pairs") {
    intercept[IllegalArgumentException] { new GeodeConnectionConf(Seq.empty) }
  }

  test("getConnection() normal") {
    implicit val mockFactory = mock[GeodeConnectionManager]
    val mockConnection = mock[GeodeConnection]
    when(mockFactory.getConnection(org.mockito.Matchers.any[GeodeConnectionConf])).thenReturn(mockConnection)
    val connConf = GeodeConnectionConf("localhost:1234")
    assert(connConf.getConnection == mockConnection)
    verify(mockFactory).getConnection(connConf)
  }

  test("getConnection() failure") {
    implicit val mockFactory = mock[GeodeConnectionManager]
    when(mockFactory.getConnection(org.mockito.Matchers.any[GeodeConnectionConf])).thenThrow(new RuntimeException)
    val connConf = GeodeConnectionConf("localhost:1234")
    intercept[RuntimeException] { connConf.getConnection }
    verify(mockFactory).getConnection(connConf)
  }

}
