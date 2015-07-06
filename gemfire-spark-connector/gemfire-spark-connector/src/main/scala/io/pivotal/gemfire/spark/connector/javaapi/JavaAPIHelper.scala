package io.pivotal.gemfire.spark.connector.javaapi

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.streaming.api.java.{JavaPairDStream, JavaDStream}

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

/**
 *  A helper class to make it possible to access components written in Scala from Java code.
 */
private[connector] object JavaAPIHelper {

  /** Returns a `ClassTag` of a given runtime class. */
  def getClassTag[T](clazz: Class[T]): ClassTag[T] = ClassTag(clazz)

  /**
   * Produces a ClassTag[T], which is actually just a casted ClassTag[AnyRef].
   * see JavaSparkContext.fakeClassTag in Spark for more info.
   */
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  /** Converts a Java `Properties` to a Scala immutable `Map[String, String]`. */
  def propertiesToScalaMap[K, V](props: java.util.Properties): Map[String, String] =
    Map(props.toSeq: _*)

  /** convert a JavaRDD[(K,V)] to JavaPairRDD[K,V] */
  def toJavaPairRDD[K, V](rdd: JavaRDD[(K, V)]): JavaPairRDD[K, V] =
    JavaPairRDD.fromJavaRDD(rdd)

  /** convert a JavaDStream[(K,V)] to JavaPairDStream[K,V] */
  def toJavaPairDStream[K, V](ds: JavaDStream[(K, V)]): JavaPairDStream[K, V] =
    JavaPairDStream.fromJavaDStream(ds)

}
