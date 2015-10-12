package io.pivotal.gemfire.spark.connector.internal.oql

import com.gemstone.gemfire.cache.query.internal.StructImpl
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
