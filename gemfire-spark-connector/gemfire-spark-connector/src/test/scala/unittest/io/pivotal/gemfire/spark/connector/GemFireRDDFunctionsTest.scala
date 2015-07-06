package unittest.io.pivotal.gemfire.spark.connector

import com.gemstone.gemfire.cache.Region
import io.pivotal.gemfire.spark.connector._
import io.pivotal.gemfire.spark.connector.internal.rdd.{GemFireRDDWriter, GemFirePairRDDWriter}
import org.apache.spark.{TaskContext, SparkContext}
import org.apache.spark.rdd.RDD
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import collection.JavaConversions._
import scala.reflect.ClassTag
import org.mockito.Matchers.{eq => mockEq, any => mockAny}

class GemFireRDDFunctionsTest extends FunSuite with Matchers with MockitoSugar {

  test("test PairRDDFunction Implicit") {
    import io.pivotal.gemfire.spark.connector._
    val mockRDD = mock[RDD[(Int, String)]]
    // the implicit make the following line valid
    val pairRDD: GemFirePairRDDFunctions[Int, String] = mockRDD
    pairRDD shouldBe a [GemFirePairRDDFunctions[_, _]]
  }
  
  test("test RDDFunction Implicit") {
    import io.pivotal.gemfire.spark.connector._
    val mockRDD = mock[RDD[String]]
    // the implicit make the following line valid
    val nonPairRDD: GemFireRDDFunctions[String] = mockRDD
    nonPairRDD shouldBe a [GemFireRDDFunctions[_]]
  }

  def createMocks[K, V](regionPath: String)
    (implicit kt: ClassTag[K], vt: ClassTag[V], m: Manifest[Region[K, V]]): (String, GemFireConnectionConf, GemFireConnection, Region[K, V]) = {
    val mockConnection = mock[GemFireConnection]
    val mockConnConf = mock[GemFireConnectionConf]
    val mockRegion = mock[Region[K, V]]
    when(mockConnConf.getConnection).thenReturn(mockConnection)
    when(mockConnection.getRegionProxy[K, V](regionPath)).thenReturn(mockRegion)
    // mockRegion shouldEqual mockConn.getRegionProxy[K, V](regionPath)
    (regionPath, mockConnConf, mockConnection, mockRegion)
  }

  test("test GemFirePairRDDWriter") {
    val (regionPath, mockConnConf, mockConnection, mockRegion) = createMocks[String, String]("test")
    val writer = new GemFirePairRDDWriter[String, String](regionPath, mockConnConf)
    val data = List(("1", "one"), ("2", "two"), ("3", "three"))
    writer.write(null, data.toIterator)
    val expectedMap: Map[String, String] = data.toMap
    verify(mockRegion).putAll(expectedMap)
  }

  test("test GemFireNonPairRDDWriter") {
    val (regionPath, mockConnConf, mockConnection, mockRegion) = createMocks[Int, String]("test")
    val writer = new GemFireRDDWriter[String, Int, String](regionPath, mockConnConf)
    val data = List("a", "ab", "abc")
    val f: String => (Int, String) = s => (s.length, s)
    writer.write(f)(null, data.toIterator)
    val expectedMap: Map[Int, String] = data.map(f).toMap
    verify(mockRegion).putAll(expectedMap)
  }
  
  test("test PairRDDFunctions.saveToGemfire") {
    import io.pivotal.gemfire.spark.connector._
    val (regionPath, mockConnConf, mockConnection, mockRegion) = createMocks[String, String]("test")
    val mockRDD = mock[RDD[(String, String)]]
    val mockSparkContext = mock[SparkContext]
    when(mockRDD.sparkContext).thenReturn(mockSparkContext)
    val result = mockRDD.saveToGemfire(regionPath, mockConnConf)
    verify(mockConnection, times(1)).validateRegion[String, String](regionPath)
    result === Unit
    verify(mockSparkContext, times(1)).runJob[(String, String), Unit](
      mockEq(mockRDD), mockAny[(TaskContext, Iterator[(String, String)]) => Unit])(mockAny(classOf[ClassTag[Unit]]))

    // Note: current implementation make following code not compilable
    //       so not negative test for this case   
    //  val rdd: RDD[(K, V)] = ...
    //  rdd.saveToGemfire(regionPath, s => (s.length, s))
  }

  test("test RDDFunctions.saveToGemfire") {
    import io.pivotal.gemfire.spark.connector._
    val (regionPath, mockConnConf, mockConnection, mockRegion) = createMocks[Int, String]("test")
    val mockRDD = mock[RDD[(String)]]
    val mockSparkContext = mock[SparkContext]
    when(mockRDD.sparkContext).thenReturn(mockSparkContext)
    val result = mockRDD.saveToGemfire(regionPath, s => (s.length, s), mockConnConf)
    verify(mockConnection, times(1)).validateRegion[Int, String](regionPath)
    result === Unit
    verify(mockSparkContext, times(1)).runJob[String, Unit](
      mockEq(mockRDD), mockAny[(TaskContext, Iterator[String]) => Unit])(mockAny(classOf[ClassTag[Unit]]))

    // Note: current implementation make following code not compilable
    //       so not negative test for this case
    //  val rdd: RDD[T] = ...   // T is not a (K, V) tuple
    //  rdd.saveToGemfire(regionPath)
  }
  
}
