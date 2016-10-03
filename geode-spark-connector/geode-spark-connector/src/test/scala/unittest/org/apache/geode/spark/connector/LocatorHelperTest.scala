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

import java.net.InetAddress

import org.apache.geode.spark.connector.internal.LocatorHelper
import org.scalatest.FunSuite

class LocatorHelperTest extends FunSuite {

  test("locatorStr2HostPortPair hostname w/o domain") {
    val (host, port) = ("localhost", 10334)
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:$port").get ==(host, port))
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host[$port]").get ==(host, port))
  }

  test("locatorStr2HostPortPair hostname w/ domain") {
    val (host, port) = ("localhost", 10334)
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:$port").get ==(host, port))
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host[$port]").get ==(host, port))
  }

  test("locatorStr2HostPortPair w/ invalid host name") {
    // empty or null locatorStr
    assert(LocatorHelper.locatorStr2HostPortPair("").isFailure)
    assert(LocatorHelper.locatorStr2HostPortPair(null).isFailure)
    // host name has leading `.`
    assert(LocatorHelper.locatorStr2HostPortPair(".localhost.1234").isFailure)
    // host name has leading and/or tail white space
    assert(LocatorHelper.locatorStr2HostPortPair(" localhost.1234").isFailure)
    assert(LocatorHelper.locatorStr2HostPortPair("localhost .1234").isFailure)
    assert(LocatorHelper.locatorStr2HostPortPair(" localhost .1234").isFailure)
    // host name contain invalid characters
    assert(LocatorHelper.locatorStr2HostPortPair("local%host.1234").isFailure)
    assert(LocatorHelper.locatorStr2HostPortPair("localhost*.1234").isFailure)
    assert(LocatorHelper.locatorStr2HostPortPair("^localhost.1234").isFailure)
  }

  test("locatorStr2HostPortPair w/ valid port") {
    val host = "192.168.0.1"
    // port has 2, 3, 4, 5 digits
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:20").get ==(host, 20))
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:300").get ==(host, 300))
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:4000").get ==(host, 4000))
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:50000").get ==(host, 50000))
  }
  
  test("locatorStr2HostPortPair w/ invalid port") {
    // port number is less than 2 digits
    assert(LocatorHelper.locatorStr2HostPortPair("locslhost.9").isFailure)
    // port number is more than 5 digits
    assert(LocatorHelper.locatorStr2HostPortPair("locslhost.100000").isFailure)
    // port number is invalid
    assert(LocatorHelper.locatorStr2HostPortPair("locslhost.1xx1").isFailure)
  }
  
  test("parseLocatorsString with valid locator(s)") {
    val (host1, port1) = ("localhost", 10334)
    assert(LocatorHelper.parseLocatorsString(s"$host1:$port1") == Seq((host1, port1)))
    val (host2, port2) = ("localhost2", 10335)
    assert(LocatorHelper.parseLocatorsString(s"$host1:$port1,$host2:$port2") == Seq((host1, port1),(host2, port2)))
    val (host3, port3) = ("localhost2", 10336)
    assert(LocatorHelper.parseLocatorsString(s"$host1:$port1,$host2:$port2,$host3:$port3") == 
      Seq((host1, port1),(host2, port2),(host3, port3)))
  }

  test("parseLocatorsString with invalid locator(s)") {
    // empty and null locatorsStr
    intercept[Exception] { LocatorHelper.parseLocatorsString("") }
    intercept[Exception] { LocatorHelper.parseLocatorsString(null) }
    // 1 bad locatorStr
    intercept[Exception] { LocatorHelper.parseLocatorsString("local%host.1234") }
    // 1 good locatorStr and 1 bad locatorStr
    intercept[Exception] { LocatorHelper.parseLocatorsString("localhost:2345,local%host.1234") }
    intercept[Exception] { LocatorHelper.parseLocatorsString("local^host:2345,localhost.1234") }
  }

  test("pickPreferredGeodeServers: shared servers and one gf-server per host") {
    val (srv1, srv2, srv3, srv4) = (("host1", 4001), ("host2", 4002), ("host3", 4003),("host4", 4004))
    val servers = Seq(srv1, srv2, srv3, srv4)
    verifyPickPreferredGeodeServers(servers, "host1", "<driver>", Seq(srv1, srv2, srv3))
    verifyPickPreferredGeodeServers(servers, "host2", "0", Seq(srv2, srv3, srv4))
    verifyPickPreferredGeodeServers(servers, "host3", "1", Seq(srv3, srv4, srv1))
    verifyPickPreferredGeodeServers(servers, "host4", "2", Seq(srv4, srv1, srv2))
  }

  test("pickPreferredGeodeServers: shared servers, one gf-server per host, un-sorted list") {
    val (srv1, srv2, srv3, srv4) = (("host1", 4001), ("host2", 4002), ("host3", 4003),("host4", 4004))
    val servers = Seq(srv4, srv2, srv3, srv1)
    verifyPickPreferredGeodeServers(servers, "host1", "<driver>", Seq(srv1, srv2, srv3))
    verifyPickPreferredGeodeServers(servers, "host2", "0", Seq(srv2, srv3, srv4))
    verifyPickPreferredGeodeServers(servers, "host3", "1", Seq(srv3, srv4, srv1))
    verifyPickPreferredGeodeServers(servers, "host4", "2", Seq(srv4, srv1, srv2))
  }

  test("pickPreferredGeodeServers: shared servers and two gf-server per host") {
    val (srv1, srv2, srv3, srv4) = (("host1", 4001), ("host1", 4002), ("host2", 4003), ("host2", 4004))
    val servers = Seq(srv1, srv2, srv3, srv4)
    verifyPickPreferredGeodeServers(servers, "host1", "<driver>", Seq(srv1, srv2, srv3))
    verifyPickPreferredGeodeServers(servers, "host1", "0", Seq(srv2, srv1, srv3))
    verifyPickPreferredGeodeServers(servers, "host2", "1", Seq(srv3, srv4, srv1))
    verifyPickPreferredGeodeServers(servers, "host2", "2", Seq(srv4, srv3, srv1))
  }

  test("pickPreferredGeodeServers: shared servers, two gf-server per host, un-sorted server list") {
    val (srv1, srv2, srv3, srv4) = (("host1", 4001), ("host1", 4002), ("host2", 4003), ("host2", 4004))
    val servers = Seq(srv1, srv4, srv3, srv2)
    verifyPickPreferredGeodeServers(servers, "host1", "<driver>", Seq(srv1, srv2, srv3))
    verifyPickPreferredGeodeServers(servers, "host1", "0", Seq(srv2, srv1, srv3))
    verifyPickPreferredGeodeServers(servers, "host2", "1", Seq(srv3, srv4, srv1))
    verifyPickPreferredGeodeServers(servers, "host2", "2", Seq(srv4, srv3, srv1))
  }

  test("pickPreferredGeodeServers: no shared servers and one gf-server per host") {
    val (srv1, srv2, srv3, srv4) = (("host1", 4001), ("host2", 4002), ("host3", 4003),("host4", 4004))
    val servers = Seq(srv1, srv2, srv3, srv4)
    verifyPickPreferredGeodeServers(servers, "host5", "<driver>", Seq(srv1, srv2, srv3))
    verifyPickPreferredGeodeServers(servers, "host6", "0", Seq(srv2, srv3, srv4))
    verifyPickPreferredGeodeServers(servers, "host7", "1", Seq(srv3, srv4, srv1))
    verifyPickPreferredGeodeServers(servers, "host8", "2", Seq(srv4, srv1, srv2))
  }

  test("pickPreferredGeodeServers: no shared servers, one gf-server per host, and less gf-server") {
    val (srv1, srv2) = (("host1", 4001), ("host2", 4002))
    val servers = Seq(srv1, srv2)
    verifyPickPreferredGeodeServers(servers, "host5", "<driver>", Seq(srv1, srv2))
    verifyPickPreferredGeodeServers(servers, "host6", "0", Seq(srv2, srv1))
    verifyPickPreferredGeodeServers(servers, "host7", "1", Seq(srv1, srv2))
    verifyPickPreferredGeodeServers(servers, "host8", "2", Seq(srv2, srv1))


    println("host name: " + InetAddress.getLocalHost.getHostName)
    println("canonical host name: " + InetAddress.getLocalHost.getCanonicalHostName)
    println("canonical host name 2: " + InetAddress.getByName(InetAddress.getLocalHost.getHostName).getCanonicalHostName)
  }

  test("pickPreferredGeodeServers: ad-hoc") {
    val (srv4, srv5, srv6) = (
      ("w2-gst-pnq-04.gemstone.com", 40411), ("w2-gst-pnq-05.gemstone.com", 40411), ("w2-gst-pnq-06.gemstone.com", 40411))
    val servers = Seq(srv6, srv5, srv4)
    verifyPickPreferredGeodeServers(servers, "w2-gst-pnq-03.gemstone.com", "<driver>", Seq(srv4, srv5, srv6))
    verifyPickPreferredGeodeServers(servers, "w2-gst-pnq-04.gemstone.com", "1", Seq(srv4, srv5, srv6))
    verifyPickPreferredGeodeServers(servers, "w2-gst-pnq-05.gemstone.com", "0", Seq(srv5, srv6, srv4))
    verifyPickPreferredGeodeServers(servers, "w2-gst-pnq-06.gemstone.com", "2", Seq(srv6, srv4, srv5))
  }
  
  def verifyPickPreferredGeodeServers(
    servers: Seq[(String, Int)], hostName: String, executorId: String, expectation: Seq[(String, Int)]): Unit = {
    val result = LocatorHelper.pickPreferredGeodeServers(servers, hostName, executorId)
    assert(result == expectation, s"pick servers for $hostName:$executorId")
  }

}
