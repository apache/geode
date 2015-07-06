import sbt._
import sbt.Keys._
import org.scalastyle.sbt.ScalastylePlugin

object Settings extends Build {
   
  lazy val commonSettings = Seq(
    organization := "io.pivotal",
    version := "0.5.0",
    scalaVersion := "2.10.4",
    organization := "io.pivotal.gemfire.spark",
    organizationHomepage := Some(url("http://www.pivotal.io/"))
  ) 

  lazy val gfcResolvers = Seq(
  "GemStone Snapshots" at "http://nexus.gemstone.com:8081/nexus/content/repositories/snapshots/",
   //"GemStone Official Release" at "http://dist.gemstone.com/maven/release",
  "Repo for JLine" at "http://repo.springsource.org/libs-release",
  "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
  //"Apache Repository" at "https://repository.apache.org/content/repositories/releases/",
  // "Akka Repository" at "http://repo.akka.io/releases/",
  // "Spray Repository" at "http://repo.spray.cc/"
  //"Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
  )


  val gfcITSettings = inConfig(IntegrationTest)(Defaults.itSettings) ++
    Seq(parallelExecution in IntegrationTest := false, fork in IntegrationTest := true)

  val gfcCompileSettings = inConfig(Compile)(Defaults.compileSettings) ++ Seq(unmanagedSourceDirectories in Compile += baseDirectory.value /"../gemfire-functions/src")

  val gfcSettings = commonSettings ++ gfcITSettings ++ gfcCompileSettings 

  val demoSettings = commonSettings ++ Seq(
      scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageExcludedPackages := ".*"
    )
  
  val scalastyleSettings = Seq( 
        ScalastylePlugin.scalastyleConfig := baseDirectory.value / "project/scalastyle-config.xml"
        )
  
}
