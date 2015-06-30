package io.pivotal.gemfire.spark.connector.internal.oql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{BaseRelation, TableScan}

import scala.tools.nsc.backend.icode.analysis.DataFlowAnalysis

case class OQLRelation[T](queryRDD: QueryRDD[T])(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {

  override def schema: StructType = new SchemaBuilder(queryRDD).toSparkSchema()

  override def buildScan(): RDD[Row] = new RowBuilder(queryRDD).toRowRDD()

}

object RDDConverter {

  def queryRDDToDataFrame[T](queryRDD: QueryRDD[T], sqlContext: SQLContext): DataFrame = {
    sqlContext.baseRelationToDataFrame(OQLRelation(queryRDD)(sqlContext))
  }
}