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

import org.apache.geode.cache.query.internal.StructImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

class RowBuilder[T](queryRDD: QueryRDD[T]) {

  /**
   * Convert QueryRDD to RDD of Row
   * @return RDD of Rows
   */
  def toRowRDD(): RDD[Row] = {
    val rowRDD = queryRDD.map(row => {
      row match {
        case si: StructImpl => Row.fromSeq(si.getFieldValues)
        case obj: Object => Row.fromSeq(Seq(obj))
      }
    })
    rowRDD
  }
}
