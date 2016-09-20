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

import java.io.{IOException, File}
import java.net.InetAddress
import java.util.Properties
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.IOFileFilter

/**
* A class that manages Geode locator and servers.  Uses gfsh to
* start and stop the locator and servers.
*/
class GeodeRunner(settings: Properties) {
  val gfshCmd = new File(getCurrentDirectory, "../../geode-assembly/build/install/apache-geode/bin/gfsh").toString
  val cacheXMLFile = settings.get("cache-xml-file")
  val numServers: Int = settings.get("num-of-servers").asInstanceOf[String].toInt
  val cwd = new File(".").getAbsolutePath
  val geodeFunctionsTargetDir = new File("../geode-functions/target")
  val testroot = "target/testgeode"
  val classpath = new File(cwd, "target/scala-2.10/it-classes/")
  val locatorPort = startGeodeCluster(numServers)

  def getLocatorPort: Int = locatorPort

  private def getCurrentDirectory = new File( "." ).getCanonicalPath
  
  private def startGeodeCluster(numServers: Int): Int = {
    //ports(0) for Geode locator, the other ports are for Geode servers
    val ports: Seq[Int] = IOUtils.getRandomAvailableTCPPorts(2 + numServers)
    startGeodeLocator(ports(0), ports(1))
    startGeodeServers(ports(0), ports.drop(2))
    registerFunctions(ports(1))
    ports(0)
  }

  private def startGeodeLocator(locatorPort: Int, jmxHttpPort:Int) {
    println(s"=== GeodeRunner: starting locator on port $locatorPort")
    val locatorDir = new File(cwd, s"$testroot/locator")
    if (locatorDir.exists())
      FileUtils.deleteDirectory(locatorDir)
    IOUtils.mkdir(locatorDir)
    new ProcessBuilder()
      .command(gfshCmd, "start", "locator",
        "--name=locator",
        s"--dir=$locatorDir",
        s"--port=$locatorPort",
        s"--J=-Dgemfire.jmx-manager-http-port=$jmxHttpPort")
      .inheritIO()
      .start()

    // Wait 30 seconds for locator to start
    println(s"=== GeodeRunner: waiting for locator on port $locatorPort")
    if (!IOUtils.waitForPortOpen(InetAddress.getByName("localhost"), locatorPort, 30000))
      throw new IOException("Failed to start Geode locator.")
    println(s"=== GeodeRunner: done waiting for locator on port $locatorPort")
  }

  private def startGeodeServers(locatorPort: Int, serverPorts: Seq[Int]) {
    val procs = for (i <- 0 until serverPorts.length) yield {
      println(s"=== GeodeRunner: starting server${i+1} with clientPort ${serverPorts(i)}")
      val serverDir = new File(cwd, s"$testroot/server${i+1}")
      if (serverDir.exists())
        FileUtils.deleteDirectory(serverDir)
      IOUtils.mkdir(serverDir)
      new ProcessBuilder()
        .command(gfshCmd, "start", "server",
          s"--name=server${i+1}",
          s"--locators=localhost[$locatorPort]",
          s"--bind-address=localhost",
          s"--server-port=${serverPorts(i)}",
          s"--dir=$serverDir",
          s"--cache-xml-file=$cacheXMLFile",
          s"--classpath=$classpath")
        .inheritIO()
        .start()
    }
    procs.foreach(p => p.waitFor)
    println(s"All $serverPorts.length servers have been started") 
  }
  
  private def registerFunctions(jmxHttpPort:Int) {
    import scala.collection.JavaConversions._
    FileUtils.listFiles(geodeFunctionsTargetDir, fileFilter, dirFilter).foreach{  f => registerFunction(jmxHttpPort, f)}
  }
  
  def fileFilter = new IOFileFilter {
    def accept (file: File) = file.getName.endsWith(".jar") && file.getName.startsWith("geode-functions")
    def accept (dir: File, name: String) = name.endsWith(".jar") && name.startsWith("geode-functions")
  }
  
  def dirFilter = new IOFileFilter {
    def accept (file: File) = file.getName.startsWith("scala")
    def accept (dir: File, name: String) = name.startsWith("scala")
  }
  
  private def registerFunction(jmxHttpPort:Int, jar:File) {
    println("Deploying:" + jar.getName)
    import org.apache.geode.spark.connector.GeodeFunctionDeployer
    val deployer = new GeodeFunctionDeployer(new HttpClient())
    deployer.deploy("localhost", jmxHttpPort, jar)
  }

  def stopGeodeCluster(): Unit = {
    stopGeodeServers(numServers)
    stopGeodeLocator()
    if (!IOUtils.waitForPortClose(InetAddress.getByName("localhost"), getLocatorPort, 30000))
      throw new IOException(s"Failed to stop Geode locator at port $getLocatorPort.")
    println(s"Successfully stop Geode locator at port $getLocatorPort.")
  }

  private def stopGeodeLocator() {
    println(s"=== GeodeRunner: stop locator")
    val p = new ProcessBuilder()
      .inheritIO()
      .command(gfshCmd, "stop", "locator", s"--dir=$testroot/locator")
      .start()
     p.waitFor()
  }

  private def stopGeodeServers(numServers: Int) {
   val procs = for (i <-1 to numServers) yield {
       println(s"=== GeodeRunner: stop server $i.")
       new ProcessBuilder()
        .inheritIO()
        .command(gfshCmd, "stop", "server", s"--dir=$testroot/server$i")
        .start()
   }
   procs.foreach(p => p.waitFor())
  }

}
