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

import java.util.Properties

trait GeodeCluster {
  def startGeodeCluster(settings: Properties): Int = {
    println("=== GeodeCluster start()")
    GeodeCluster.start(settings)
  }
}

object GeodeCluster {
  private var geode: Option[GeodeRunner] = None

  def start(settings: Properties): Int = {
    geode.map(_.stopGeodeCluster()) // Clean up any old running Geode instances
    val runner = new GeodeRunner(settings)
    geode = Some(runner)
    runner.getLocatorPort
  }

  def stop(): Unit = {
    println("=== GeodeCluster shutdown: " + geode.toString)
    geode match {
      case None => println("Nothing to shutdown.")
      case Some(runner) => runner.stopGeodeCluster()
    }
    geode = None
    println("=== GeodeCluster shutdown finished.")
  }
}
