package io.pivotal.gemfire.spark.connector

import io.pivotal.gemfire.spark.connector.internal.oql.{OQLRelation, QueryRDD}
import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Provide GemFire OQL specific functions
 */
class GemFireSQLContextFunctions(@transient sqlContext: SQLContext) extends Serializable with Logging {

  /**
   * Expose a GemFire OQL query result as a DataFrame
   * @param query the OQL query string.
   */
  def gemfireOQL(
    query: String,
    connConf: GemFireConnectionConf = GemFireConnectionConf(sqlContext.sparkContext.getConf)): DataFrame = {
    logInfo(s"OQL query = $query")
    val rdd = new QueryRDD[Object](sqlContext.sparkContext, query, connConf)
    sqlContext.baseRelationToDataFrame(OQLRelation(rdd)(sqlContext))
  }

  private[connector] def defaultConnectionConf: GemFireConnectionConf =
    GemFireConnectionConf(sqlContext.sparkContext.getConf)
}
