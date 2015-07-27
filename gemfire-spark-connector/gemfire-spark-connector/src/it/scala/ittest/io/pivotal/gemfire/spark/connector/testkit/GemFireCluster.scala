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
