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

import java.util.Properties

trait GemFireCluster {
  def startGemFireCluster(settings: Properties): Int = {
    println("=== GemFireCluster start()")
    GemFireCluster.start(settings)
  }
}

object GemFireCluster {
  private var gemfire: Option[GemFireRunner] = None

  def start(settings: Properties): Int = {
    gemfire.map(_.stopGemFireCluster()) // Clean up any old running GemFire instances
    val runner = new GemFireRunner(settings)
    gemfire = Some(runner)
    runner.getLocatorPort
  }

  def stop(): Unit = {
    println("=== GemFireCluster shutdown: " + gemfire.toString)
    gemfire match {
      case None => println("Nothing to shutdown.")
      case Some(runner) => runner.stopGemFireCluster()
    }
    gemfire = None
    println("=== GemFireCluster shutdown finished.")
  }
}
