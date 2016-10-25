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
package ittest.org.apache.geode.spark.connector.testkit

import java.io.{File, IOException}
import java.net.{InetAddress, Socket}
import org.apache.geode.internal.AvailablePort
import scala.util.Try
import org.apache.log4j.PropertyConfigurator
import java.util.Properties

object IOUtils {

  /** Makes a new directory or throws an `IOException` if it cannot be made */
  def mkdir(dir: File): File = {
    if (!dir.mkdirs())
      throw new IOException(s"Could not create dir $dir")
    dir
  }

  private def socketPortProb(host: InetAddress, port: Int) = Iterator.continually {
    Try {
      Thread.sleep(100)
      new Socket(host, port).close()
    }
  }
  
  /**
   * Waits until a port at the given address is open or timeout passes.
   * @return true if managed to connect to the port, false if timeout happened first
   */
  def waitForPortOpen(host: InetAddress, port: Int, timeout: Long): Boolean = {
    val startTime = System.currentTimeMillis()
    socketPortProb(host, port)
      .dropWhile(p => p.isFailure && System.currentTimeMillis() - startTime < timeout)
      .next()
      .isSuccess
  }

  /**
   * Waits until a port at the given address is close or timeout passes.
   * @return true if host:port is un-connect-able, false if timeout happened first
   */
  def waitForPortClose(host: InetAddress, port: Int, timeout: Long): Boolean = {
    val startTime = System.currentTimeMillis()
    socketPortProb(host, port)
      .dropWhile(p => p.isSuccess && System.currentTimeMillis() - startTime < timeout)
      .next()
      .isFailure
  }

  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  def getRandomAvailableTCPPorts(count: Int): Seq[Int] =
    (0 until count).map(x => AvailablePort.getRandomAvailablePortKeeper(AvailablePort.SOCKET))
      .map{x => x.release(); x.getPort}.toArray

  /**
   * config a log4j properties used for integration tests
   */
  def configTestLog4j(level: String, props: (String, String)*): Unit = {
    val pro = new Properties()
    props.foreach(p => pro.put(p._1, p._2))
    configTestLog4j(level, pro)
  }

  def configTestLog4j(level: String, props: Properties): Unit = {
    val pro = new Properties()
    pro.put("log4j.rootLogger", s"$level, console")
    pro.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    pro.put("log4j.appender.console.target", "System.err")
    pro.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    pro.put("log4j.appender.console.layout.ConversionPattern",
      "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
    pro.putAll(props)
    PropertyConfigurator.configure(pro)
    
  }
}
