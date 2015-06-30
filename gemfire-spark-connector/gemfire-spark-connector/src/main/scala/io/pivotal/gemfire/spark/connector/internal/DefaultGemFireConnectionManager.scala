package io.pivotal.gemfire.spark.connector.internal

import io.pivotal.gemfire.spark.connector.{GemFireConnection, GemFireConnectionConf, GemFireConnectionManager}

import scala.collection.mutable

/**
 * Default implementation of GemFireConnectionFactory
 */
class DefaultGemFireConnectionManager extends GemFireConnectionManager {

  def getConnection(connConf: GemFireConnectionConf): GemFireConnection =
    DefaultGemFireConnectionManager.getConnection(connConf)

  def closeConnection(connConf: GemFireConnectionConf): Unit =
    DefaultGemFireConnectionManager.closeConnection(connConf)

}

object DefaultGemFireConnectionManager  {

  /** connection cache, keyed by host:port pair */
  private[connector] val connections = mutable.Map[(String, Int), GemFireConnection]()

  /**
   * use locator host:port pair to lookup connection. create new connection and add it
   * to `connections` if it does not exists.
   */
  def getConnection(connConf: GemFireConnectionConf)
    (implicit factory: DefaultGemFireConnectionFactory = new DefaultGemFireConnectionFactory): GemFireConnection = {
    val conns = connConf.locators.map(connections withDefaultValue null).filter(_ != null)
    if (conns.nonEmpty) conns(0)
    else connections.synchronized {
      val conn = factory.newConnection(connConf.locators, connConf.gemfireProps)
      connConf.locators.foreach(pair => connections += (pair -> conn))
      conn
    }
  }

  /**
   * Close the connection and remove it from connection cache.
   * Note: multiple entries may share the same connection, all those entries are removed.
   */
  def closeConnection(connConf: GemFireConnectionConf): Unit = {
    val conns = connConf.locators.map(connections withDefaultValue null).filter(_ != null)
    if (conns.nonEmpty) connections.synchronized {
      conns(0).close()
      connections.retain((k,v) => v != conns(0))
    }
  }
}
