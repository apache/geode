package ittest.io.pivotal.gemfire.spark.connector.testkit

import java.io.{File, IOException}
import java.net.{InetAddress, Socket}
import com.gemstone.gemfire.internal.AvailablePort
import scala.util.Try
import org.apache.log4j.PropertyConfigurator
import java.util.Properties

object IOUtils {

  /** Makes a new directory or throws an `IOException` if it cannot be made */
  def mkdir(dir: File): File = {
    if (!dir.mkdirs())
      throw new IOException(s"Could not create dir $dir")
    dir
  }

  private def socketPortProb(host: InetAddress, port: Int) = Iterator.continually {
    Try {
      Thread.sleep(100)
      new Socket(host, port).close()
    }
  }
  
  /**
   * Waits until a port at the given address is open or timeout passes.
   * @return true if managed to connect to the port, false if timeout happened first
   */
  def waitForPortOpen(host: InetAddress, port: Int, timeout: Long): Boolean = {
    val startTime = System.currentTimeMillis()
    socketPortProb(host, port)
      .dropWhile(p => p.isFailure && System.currentTimeMillis() - startTime < timeout)
      .next()
      .isSuccess
  }

  /**
   * Waits until a port at the given address is close or timeout passes.
   * @return true if host:port is un-connect-able, false if timeout happened first
   */
  def waitForPortClose(host: InetAddress, port: Int, timeout: Long): Boolean = {
    val startTime = System.currentTimeMillis()
    socketPortProb(host, port)
      .dropWhile(p => p.isSuccess && System.currentTimeMillis() - startTime < timeout)
      .next()
      .isFailure
  }

  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  def getRandomAvailableTCPPorts(count: Int): Seq[Int] =
    (0 until count).map(x => AvailablePort.getRandomAvailablePortKeeper(AvailablePort.SOCKET))
      .map{x => x.release(); x.getPort}.toArray

  /**
   * config a log4j properties used for integration tests
   */
  def configTestLog4j(level: String, props: (String, String)*): Unit = {
    val pro = new Properties()
    props.foreach(p => pro.put(p._1, p._2))
    configTestLog4j(level, pro)
  }

  def configTestLog4j(level: String, props: Properties): Unit = {
    val pro = new Properties()
    pro.put("log4j.rootLogger", s"$level, console")
    pro.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    pro.put("log4j.appender.console.target", "System.err")
    pro.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    pro.put("log4j.appender.console.layout.ConversionPattern",
      "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
    pro.putAll(props)
    PropertyConfigurator.configure(pro)
    
  }
}
