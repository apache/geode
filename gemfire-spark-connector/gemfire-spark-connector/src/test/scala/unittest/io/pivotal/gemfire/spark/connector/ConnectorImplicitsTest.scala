package unittest.io.pivotal.gemfire.spark.connector

import io.pivotal.gemfire.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.scalatest.Matchers

class ConnectorImplicitsTest extends FunSuite with Matchers with MockitoSugar {

  test("implicit map2Properties") {
    verifyProperties(Map.empty)
    verifyProperties(Map("One" -> "1", "Two" -> "2", "Three" ->"3"))
  }
  
  def verifyProperties(map: Map[String, String]): Unit = {
    val props: java.util.Properties = map
    assert(props.size() == map.size)
    map.foreach(p => assert(props.getProperty(p._1) == p._2))    
  }

  test("Test Implicit SparkContext Conversion") {
    val mockSparkContext = mock[SparkContext]
    val gfscf: GemFireSparkContextFunctions = mockSparkContext
    assert(gfscf.isInstanceOf[GemFireSparkContextFunctions])
  }

  test("Test Implicit SQLContext Conversion") {
    val mockSQLContext = mock[SQLContext]
    val gfscf: GemFireSQLContextFunctions = mockSQLContext
    assert(gfscf.isInstanceOf[GemFireSQLContextFunctions])
  }
}
