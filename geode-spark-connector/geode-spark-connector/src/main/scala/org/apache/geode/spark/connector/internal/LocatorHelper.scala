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
package org.apache.geode.spark.connector.internal

import java.net.InetSocketAddress
import java.util.{ArrayList => JArrayList}

import org.apache.geode.cache.client.internal.locator.{GetAllServersResponse, GetAllServersRequest}
import org.apache.geode.distributed.internal.ServerLocation
import org.apache.geode.distributed.internal.tcpserver.TcpClient
import org.apache.spark.Logging

import scala.util.{Failure, Success, Try}


object LocatorHelper extends Logging {

  /** valid locator strings are: host[port] and host:port */
  final val LocatorPattern1 = """([\w-_]+(\.[\w-_]+)*)\[([0-9]{2,5})\]""".r
  final val LocatorPattern2 = """([\w-_]+(\.[\w-_]+)*):([0-9]{2,5})""".r

  /** convert single locator string to Try[(host, port)] */
  def locatorStr2HostPortPair(locatorStr: String): Try[(String, Int)] =
    locatorStr match {
      case LocatorPattern1(host, domain, port) => Success((host, port.toInt))
      case LocatorPattern2(host, domain, port) => Success((host, port.toInt))
      case _ => Failure(new Exception(s"invalid locator: $locatorStr"))
    }

  /** 
   * Parse locator strings and returns Seq of (hostname, port) pair. 
   * Valid locator string are one or more "host[port]" and/or "host:port"
   * separated by `,`. For example:
   *    host1.mydomain.com[8888],host2.mydomain.com[8889] 
   *    host1.mydomain.com:8888,host2.mydomain.com:8889 
   */
  def parseLocatorsString(locatorsStr: String): Seq[(String, Int)] =
    locatorsStr.split(",").map(locatorStr2HostPortPair).map(_.get)


  /**
   * Return the list of live Geode servers for the given locators.
   * @param locators locators for the given Geode cluster
   * @param serverGroup optional server group name, default is "" (empty string)
   */
  def getAllGeodeServers(locators: Seq[(String, Int)], serverGroup: String = ""): Option[Seq[(String, Int)]] = {
    var result: Option[Seq[(String, Int)]] = None
    locators.find { case (host, port) =>
      try {
        val addr = new InetSocketAddress(host, port)
        val req = new GetAllServersRequest(serverGroup)
        val tcpClient = new TcpClient();
        val res = tcpClient.requestToServer(addr.getAddress, addr.getPort, req, 2000)
        if (res != null) {
          import scala.collection.JavaConverters._
          val servers = res.asInstanceOf[GetAllServersResponse].getServers.asInstanceOf[JArrayList[ServerLocation]]
          if (servers.size > 0)
            result = Some(servers.asScala.map(e => (e.getHostName, e.getPort)))
        }
      } catch { case e: Exception => logWarning("getAllGeodeServers error", e)
      }
      result.isDefined
    }
    result
  }

  /**
   * Pick up at most 3 preferred servers from all available servers based on
   * host name and Spark executor id.
   *
   * This method is used by DefaultGeodeConnection to create ClientCache. Usually
   * one server is enough to initialize ClientCacheFactory, but this provides two
   * backup servers in case of the 1st server can't be connected.
   *   
   * @param servers all available servers in the form of (hostname, port) pairs
   * @param hostName the host name of the Spark executor
   * @param executorId the Spark executor Id, such as "<driver>", "0", "1", ...
   * @return Seq[(hostname, port)] of preferred servers
   */
  def pickPreferredGeodeServers(
    servers: Seq[(String, Int)], hostName: String, executorId: String): Seq[(String, Int)] = {

    // pick up `length` items form the Seq starts at the `start` position.
    //  The Seq is treated as a ring, so at most `Seq.size` items can be picked
    def circularTake[T](seq: Seq[T], start: Int, length: Int): Seq[T] = {
      val size = math.min(seq.size, length)
      (start until start + size).map(x => seq(x % seq.size))
    }

    // map executor id to int: "<driver>" (or non-number string) to 0, and "n" to n + 1
    val id = try { executorId.toInt + 1 } catch { case e: NumberFormatException => 0 }
    
    // algorithm: 
    // 1. sort server list
    // 2. split sorted server list into 3 sub-lists a, b, and c:
    //      list-a: servers on the given host
    //      list-b: servers that are in front of list-a on the sorted server list
    //      list-c: servers that are behind list-a on the sorted server list
    //    then rotate list-a based on executor id, then create new server list:
    //      modified list-a ++ list-c ++ list-b
    // 3. if there's no server on the given host, then create new server list
    //    by rotating sorted server list based on executor id.
    // 4. take up to 3 servers from the new server list
    val sortedServers = servers.sorted
    val firstIdx = sortedServers.indexWhere(p => p._1 == hostName)
    val lastIdx = if (firstIdx < 0) -1 else sortedServers.lastIndexWhere(p => p._1 == hostName)

    if (firstIdx < 0) { // no local server
      circularTake(sortedServers, id, 3)
    } else {
      val (seq1, seq2) = sortedServers.splitAt(firstIdx)
      val seq = if (firstIdx == lastIdx) {  // one local server
        seq2 ++ seq1
      } else { // multiple local server
        val (seq3, seq4) = seq2.splitAt(lastIdx - firstIdx + 1)
        val seq3b = if (id % seq3.size == 0) seq3 else circularTake(seq3, id, seq3.size)
        seq3b ++ seq4 ++ seq1
      }
      circularTake(seq, 0, 3)
    }
  }  
}
