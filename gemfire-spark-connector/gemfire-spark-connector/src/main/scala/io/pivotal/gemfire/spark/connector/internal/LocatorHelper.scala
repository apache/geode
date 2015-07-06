package io.pivotal.gemfire.spark.connector.internal

import scala.util.{Failure, Success, Try}


object LocatorHelper {

  /** valid locator strings are: host[port] and host:port */
  final val LocatorPattern1 = """([\w-_]+(\.[\w-_]+)*)\[([0-9]{2,5})\]""".r
  final val LocatorPattern2 = """([\w-_]+(\.[\w-_]+)*):([0-9]{2,5})""".r

  /** convert single locator string to Try[(host, port)] */
  def locatorStr2HostPortPair(locatorStr: String): Try[(String, Int)] =
    locatorStr match {
      case LocatorPattern1(host, domain, port) => Success((host, port.toInt))
      case LocatorPattern2(host, domain, port) => Success((host, port.toInt))
      case _ => Failure(new Exception(s"invalid locator: $locatorStr"))
    }

  /** 
   * Parse locator strings and returns Seq of (hostname, port) pair. 
   * Valid locator string are one or more "host[port]" and/or "host:port"
   * separated by `,`. For example:
   *    host1.mydomain.com[8888],host2.mydomain.com[8889] 
   *    host1.mydomain.com:8888,host2.mydomain.com:8889 
   */
  def parseLocatorsString(locatorsStr: String): Seq[(String, Int)] =
    locatorsStr.split(",").map(locatorStr2HostPortPair).map(_.get)

}
