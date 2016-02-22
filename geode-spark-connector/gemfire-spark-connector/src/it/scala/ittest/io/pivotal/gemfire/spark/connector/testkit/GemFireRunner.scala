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
package ittest.io.pivotal.gemfire.spark.connector.testkit

import java.io.{IOException, File}
import java.net.InetAddress
import java.util.Properties
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.IOFileFilter

/**
* A class that manages GemFire locator and servers.  Uses gfsh to
* start and stop the locator and servers.
*/
class GemFireRunner(settings: Properties) {
  val gfshCmd = new File(getCurrentDirectory, "../../gemfire-assembly/build/install/apache-geode/bin/gfsh").toString
  val cacheXMLFile = settings.get("cache-xml-file")
  val numServers: Int = settings.get("num-of-servers").asInstanceOf[String].toInt
  val cwd = new File(".").getAbsolutePath
  val gemfireFunctionsTargetDir = new File("../gemfire-functions/target")
  val testroot = "target/testgemfire"
  val classpath = new File(cwd, "target/scala-2.10/it-classes/")
  val locatorPort = startGemFireCluster(numServers)

  def getLocatorPort: Int = locatorPort

  private def getCurrentDirectory = new File( "." ).getCanonicalPath
  
  private def startGemFireCluster(numServers: Int): Int = {
    //ports(0) for GemFire locator, the other ports are for GemFire servers
    val ports: Seq[Int] = IOUtils.getRandomAvailableTCPPorts(2 + numServers)
    startGemFireLocator(ports(0), ports(1))
    startGemFireServers(ports(0), ports.drop(2))
    registerFunctions(ports(1))
    ports(0)
  }

  private def startGemFireLocator(locatorPort: Int, jmxHttpPort:Int) {
    println(s"=== GemFireRunner: starting locator on port $locatorPort")
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
    println(s"=== GemFireRunner: waiting for locator on port $locatorPort")
    if (!IOUtils.waitForPortOpen(InetAddress.getByName("localhost"), locatorPort, 30000))
      throw new IOException("Failed to start GemFire locator.")
    println(s"=== GemFireRunner: done waiting for locator on port $locatorPort")
  }

  private def startGemFireServers(locatorPort: Int, serverPorts: Seq[Int]) {
    val procs = for (i <- 0 until serverPorts.length) yield {
      println(s"=== GemFireRunner: starting server${i+1} with clientPort ${serverPorts(i)}")
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
    FileUtils.listFiles(gemfireFunctionsTargetDir, fileFilter, dirFilter).foreach{  f => registerFunction(jmxHttpPort, f)}
  }
  
  def fileFilter = new IOFileFilter {
    def accept (file: File) = file.getName.endsWith(".jar") && file.getName.startsWith("gemfire-functions")
    def accept (dir: File, name: String) = name.endsWith(".jar") && name.startsWith("gemfire-functions")
  }
  
  def dirFilter = new IOFileFilter {
    def accept (file: File) = file.getName.startsWith("scala")
    def accept (dir: File, name: String) = name.startsWith("scala")
  }
  
  private def registerFunction(jmxHttpPort:Int, jar:File) {
    println("Deploying:" + jar.getName)
    import io.pivotal.gemfire.spark.connector.GemFireFunctionDeployer
    val deployer = new GemFireFunctionDeployer(new HttpClient())
    deployer.deploy("localhost", jmxHttpPort, jar)
  }

  def stopGemFireCluster(): Unit = {
    stopGemFireServers(numServers)
    stopGemFireLocator()
    if (!IOUtils.waitForPortClose(InetAddress.getByName("localhost"), getLocatorPort, 30000))
      throw new IOException(s"Failed to stop GemFire locator at port $getLocatorPort.")
    println(s"Successfully stop GemFire locator at port $getLocatorPort.")
  }

  private def stopGemFireLocator() {
    println(s"=== GemFireRunner: stop locator")
    val p = new ProcessBuilder()
      .inheritIO()
      .command(gfshCmd, "stop", "locator", s"--dir=$testroot/locator")
      .start()
     p.waitFor()
  }

  private def stopGemFireServers(numServers: Int) {
   val procs = for (i <-1 to numServers) yield {
       println(s"=== GemFireRunner: stop server $i.")
       new ProcessBuilder()
        .inheritIO()
        .command(gfshCmd, "stop", "server", s"--dir=$testroot/server$i")
        .start()
   }
   procs.foreach(p => p.waitFor())
  }

}
