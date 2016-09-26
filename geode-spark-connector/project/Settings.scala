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
import org.scalastyle.sbt.ScalastylePlugin

object Settings extends Build {
   
  lazy val commonSettings = Seq(
    organization := "io.pivotal",
    version := "0.5.0",
    scalaVersion := "2.10.4",
    organization := "org.apache.geode.spark",
    organizationHomepage := Some(url("http://www.pivotal.io/"))
  ) 

  lazy val gfcResolvers = Seq(
   //"GemStone Official Release" at "http://dist.gemstone.com/maven/release",
  "Repo for JLine" at "http://repo.spring.io/libs-release",
  "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
  "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots/"
  //"Apache Repository" at "https://repository.apache.org/content/repositories/releases/",
  // "Akka Repository" at "http://repo.akka.io/releases/",
  // "Spray Repository" at "http://repo.spray.cc/"
  //"Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
  )


  val gfcITSettings = inConfig(IntegrationTest)(Defaults.itSettings) ++
    Seq(parallelExecution in IntegrationTest := false, fork in IntegrationTest := true)

  val gfcCompileSettings = inConfig(Compile)(Defaults.compileSettings) ++ Seq(unmanagedSourceDirectories in Compile += baseDirectory.value /"../geode-functions/src")

  val gfcSettings = commonSettings ++ gfcITSettings ++ gfcCompileSettings 

  val demoSettings = commonSettings ++ Seq(
      scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageExcludedPackages := ".*"
    )
  
  val scalastyleSettings = Seq( 
        ScalastylePlugin.scalastyleConfig := baseDirectory.value / "project/scalastyle-config.xml"
        )
  
}
