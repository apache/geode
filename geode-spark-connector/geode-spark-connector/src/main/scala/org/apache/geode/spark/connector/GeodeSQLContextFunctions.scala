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
package org.apache.geode.spark.connector

import org.apache.geode.spark.connector.internal.oql.{OQLRelation, QueryRDD}
import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Provide Geode OQL specific functions
 */
class GeodeSQLContextFunctions(@transient sqlContext: SQLContext) extends Serializable with Logging {

  /**
   * Expose a Geode OQL query result as a DataFrame
   * @param query the OQL query string.
   */
  def geodeOQL(
    query: String,
    connConf: GeodeConnectionConf = GeodeConnectionConf(sqlContext.sparkContext.getConf)): DataFrame = {
    logInfo(s"OQL query = $query")
    val rdd = new QueryRDD[Object](sqlContext.sparkContext, query, connConf)
    sqlContext.baseRelationToDataFrame(OQLRelation(rdd)(sqlContext))
  }

  private[connector] def defaultConnectionConf: GeodeConnectionConf =
    GeodeConnectionConf(sqlContext.sparkContext.getConf)
}
