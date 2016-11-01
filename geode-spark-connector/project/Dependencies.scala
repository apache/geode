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
import sbt._
import sbt.Keys._

object Dependencies {

  object Compile {
    val sparkStreaming = "org.apache.spark" %% "spark-streaming" % "1.3.0" 
    val sparkSql = "org.apache.spark" %% "spark-sql" % "1.3.0"
    val geode = "org.apache.geode" % "geode-core" % "1.0.0-incubating" excludeAll(ExclusionRule(organization = "org.jboss.netty") )
  }

  object Test {
    val scalaTest = "org.scalatest" % "scalatest_2.10" % "2.2.1" % "it, test" //scala test framework
    val mockito = "org.mockito" % "mockito-all" % "1.10.19" % "test" //mockito mock test framework
    val junit = "junit" % "junit" % "4.11" % "it, test" //4.11 because the junit-interface was complaining when using 4.12
    val novoCode = "com.novocode" % "junit-interface" % "0.11" % "it, test"//for junit to run with sbt
  }

  import Test._
  import Compile._

  val unitTests = Seq(scalaTest, mockito, junit, novoCode)

  val connector = unitTests ++ Seq(sparkStreaming, sparkSql, geode)

  val functions = Seq(geode, junit)
 
  val demos = Seq(sparkStreaming, sparkSql, geode)
}
