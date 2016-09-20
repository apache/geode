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
package org.apache.geode.spark.connector.internal.oql

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
