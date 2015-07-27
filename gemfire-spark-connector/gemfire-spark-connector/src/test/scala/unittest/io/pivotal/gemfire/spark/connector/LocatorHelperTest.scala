package unittest.io.pivotal.gemfire.spark.connector

import io.pivotal.gemfire.spark.connector.internal.LocatorHelper
import org.scalatest.FunSuite

class LocatorHelperTest extends FunSuite {

  test("locatorStr2HostPortPair hostname w/o domain") {
    val (host, port) = ("localhost", 10334)
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:$port").get ==(host, port))
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host[$port]").get ==(host, port))
  }

  test("locatorStr2HostPortPair hostname w/ domain") {
    val (host, port) = ("localhost", 10334)
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:$port").get ==(host, port))
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host[$port]").get ==(host, port))
  }

  test("locatorStr2HostPortPair w/ invalid host name") {
    // empty or null locatorStr
    assert(LocatorHelper.locatorStr2HostPortPair("").isFailure)
    assert(LocatorHelper.locatorStr2HostPortPair(null).isFailure)
    // host name has leading `.`
    assert(LocatorHelper.locatorStr2HostPortPair(".localhost.1234").isFailure)
    // host name has leading and/or tail white space
    assert(LocatorHelper.locatorStr2HostPortPair(" localhost.1234").isFailure)
    assert(LocatorHelper.locatorStr2HostPortPair("localhost .1234").isFailure)
    assert(LocatorHelper.locatorStr2HostPortPair(" localhost .1234").isFailure)
    // host name contain invalid characters
    assert(LocatorHelper.locatorStr2HostPortPair("local%host.1234").isFailure)
    assert(LocatorHelper.locatorStr2HostPortPair("localhost*.1234").isFailure)
    assert(LocatorHelper.locatorStr2HostPortPair("^localhost.1234").isFailure)
  }

  test("locatorStr2HostPortPair w/ valid port") {
    val host = "192.168.0.1"
    // port has 2, 3, 4, 5 digits
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:20").get ==(host, 20))
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:300").get ==(host, 300))
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:4000").get ==(host, 4000))
    assert(LocatorHelper.locatorStr2HostPortPair(s"$host:50000").get ==(host, 50000))
  }
  
  test("locatorStr2HostPortPair w/ invalid port") {
    // port number is less than 2 digits
    assert(LocatorHelper.locatorStr2HostPortPair("locslhost.9").isFailure)
    // port number is more than 5 digits
    assert(LocatorHelper.locatorStr2HostPortPair("locslhost.100000").isFailure)
    // port number is invalid
    assert(LocatorHelper.locatorStr2HostPortPair("locslhost.1xx1").isFailure)
  }
  
  test("parseLocatorsString with valid locator(s)") {
    val (host1, port1) = ("localhost", 10334)
    assert(LocatorHelper.parseLocatorsString(s"$host1:$port1") == Seq((host1, port1)))
    val (host2, port2) = ("localhost2", 10335)
    assert(LocatorHelper.parseLocatorsString(s"$host1:$port1,$host2:$port2") == Seq((host1, port1),(host2, port2)))
    val (host3, port3) = ("localhost2", 10336)
    assert(LocatorHelper.parseLocatorsString(s"$host1:$port1,$host2:$port2,$host3:$port3") == 
      Seq((host1, port1),(host2, port2),(host3, port3)))
  }

  test("parseLocatorsString with invalid locator(s)") {
    // empty and null locatorsStr
    intercept[Exception] { LocatorHelper.parseLocatorsString("") }
    intercept[Exception] { LocatorHelper.parseLocatorsString(null) }
    // 1 bad locatorStr
    intercept[Exception] { LocatorHelper.parseLocatorsString("local%host.1234") }
    // 1 good locatorStr and 1 bad locatorStr
    intercept[Exception] { LocatorHelper.parseLocatorsString("localhost:2345,local%host.1234") }
    intercept[Exception] { LocatorHelper.parseLocatorsString("local^host:2345,localhost.1234") }
  }

}
