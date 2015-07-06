package io.pivotal.gemfire.spark.connector

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import org.apache.commons.httpclient.HttpClient
import java.io.File


class GemFireFunctionDeployerTest extends FunSuite with Matchers with MockitoSugar {
  val mockHttpClient: HttpClient = mock[HttpClient]
    
  test("jmx url creation") {
    val jmxHostAndPort = "localhost:7070"
    val expectedUrlString = "http://" + jmxHostAndPort + "/gemfire/v1/deployed"
    val gfd = new GemFireFunctionDeployer(mockHttpClient);
    val urlString = gfd.constructURLString(jmxHostAndPort)
    assert(urlString === expectedUrlString)
  }
    
  test("missing jar file") {
    val missingJarFileLocation = "file:///somemissingjarfilethatdoesnot.exist"
    val gfd = new GemFireFunctionDeployer(mockHttpClient);
    intercept[RuntimeException] { gfd.jarFileHandle(missingJarFileLocation)}
  }
  
  test("deploy with missing jar") {
    val missingJarFileLocation = "file:///somemissingjarfilethatdoesnot.exist"
    val gfd = new GemFireFunctionDeployer(mockHttpClient);
    intercept[RuntimeException] {(gfd.deploy("localhost:7070", missingJarFileLocation).contains("Deployed"))}
    intercept[RuntimeException] {(gfd.deploy("localhost", 7070, missingJarFileLocation).contains("Deployed"))}
  }
  
  test("successful mocked deploy") {
    val gfd = new GemFireFunctionDeployer(mockHttpClient);
    val jar = new File("README.md");
    assert(gfd.deploy("localhost:7070", jar).contains("Deployed"))
  }
  

    
}
