package io.pivotal.gemfire.spark.connector.internal.oql

import com.gemstone.gemfire.cache.query.internal.StructImpl
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
import org.apache.spark.Logging

class SchemaBuilder[T](queryRDD: QueryRDD[T]) extends Logging {

  val nullStructType = StructType(Nil)
  
  val typeMap:Map[Class[_], DataType] = Map( 
    (classOf[java.lang.String], StringType),
    (classOf[java.lang.Integer], IntegerType),
    (classOf[java.lang.Short], ShortType),
    (classOf[java.lang.Long], LongType),
    (classOf[java.lang.Double], DoubleType),
    (classOf[java.lang.Float], FloatType),
    (classOf[java.lang.Boolean], BooleanType),
    (classOf[java.lang.Byte], ByteType),
    (classOf[java.util.Date], DateType),
    (classOf[java.lang.Object], nullStructType)
  )
  
  /**
   * Analyse QueryRDD to get the Spark schema
   * @return The schema represented by Spark StructType
   */
  def toSparkSchema(): StructType = {
    val row = queryRDD.first()
    val tpe = row match {
      case r: StructImpl => constructFromStruct(r)
      case null => StructType(StructField("col1", NullType) :: Nil)
      case default => 
        val value = typeMap.getOrElse(default.getClass(), nullStructType)
        StructType(StructField("col1", value) :: Nil)
    }
    logInfo(s"Schema: $tpe")
    tpe
  }
  
  def constructFromStruct(r:StructImpl) = {
    val names = r.getFieldNames
    val values = r.getFieldValues
    val lb = new ListBuffer[StructField]()
    for (i <- 0 until names.length) {
      val name = names(i)
      val value = values(i)
      val dataType = value match {
        case null => NullType
        case default => typeMap.getOrElse(default.getClass,  nullStructType)
      }
      lb += StructField(name, dataType)
    }
    StructType(lb.toSeq)
  }
}
