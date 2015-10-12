package io.pivotal.gemfire.spark.connector

/**
 * GemFireConnectionFactory provide an common interface that manages GemFire
 * connections, and it's serializable. Each factory instance will handle
 * connection instance creation and connection pool management.
 */
trait GemFireConnectionManager extends Serializable {

  /** get connection for the given connector */
  def getConnection(connConf: GemFireConnectionConf): GemFireConnection

  /** close the connection */
  def closeConnection(connConf: GemFireConnectionConf): Unit
}
