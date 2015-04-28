/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.org.jgroups.util.GemFireTracer;

/**
 * Sent to a locator to request it to find (and possibly start)
 * a jmx manager for us. It returns a JmxManagerLocatorResponse.
 * 
 * @author darrel
 * @since 7.0
 *
 */
public class JmxManagerLocatorRequest implements DataSerializableFixedID {

  public JmxManagerLocatorRequest() {
    super();
  }
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
  }

  public void toData(DataOutput out) throws IOException {
  }

  public int getDSFID() {
    return DataSerializableFixedID.JMX_MANAGER_LOCATOR_REQUEST;
  }

  @Override
  public String toString() {
    return "JmxManagerLocatorRequest";
  }
  
  private static final JmxManagerLocatorRequest SINGLETON = new JmxManagerLocatorRequest();

  /**
   * Send a request to the specified locator asking it to find (and start if
   * needed) a jmx manager. A jmx manager will only be started
   * 
   * @param locatorHost
   *          the name of the host the locator is on
   * @param locatorPort
   *          the port the locator is listening on
   * @param msTimeout
   *          how long in milliseconds to wait for a response from the locator
   * @param sslConfigProps
   *          Map carrying SSL configuration that can be used by SocketCreator
   * @return a response object that describes the jmx manager.
   * @throws IOException
   *           if we can not connect to the locator, timeout waiting for a
   *           response, or have trouble communicating with it.
   */
  public static JmxManagerLocatorResponse send(String locatorHost, int locatorPort, int msTimeout, Map<String, String> sslConfigProps)
    throws IOException
  {
    Properties distributionConfigProps = new Properties();
    InetAddress networkAddress = InetAddress.getByName(locatorHost);

    try {
      // Changes for 46623
      // initialize the SocketCreator with props which may contain SSL config
      // empty distConfProps will reset SocketCreator
      if (sslConfigProps != null) {
        distributionConfigProps.putAll(sslConfigProps);
      }

      GemFireTracer tracer = GemFireTracer.getLog(JmxManagerLocator.class);
      tracer.setLogWriter(new LocalLogWriter(InternalLogWriter.NONE_LEVEL));
      tracer.setSecurityLogWriter(new LocalLogWriter(InternalLogWriter.NONE_LEVEL));
      SocketCreator.getDefaultInstance(distributionConfigProps);

      Object responseFromServer = TcpClient.requestToServer(networkAddress, locatorPort, SINGLETON, msTimeout);

      return (JmxManagerLocatorResponse) responseFromServer;
    }
    catch (ClassNotFoundException unexpected) {
      throw new IllegalStateException(unexpected);
    }
    catch (ClassCastException unexpected) {
      // FIXME - Abhishek: object read is type "int" instead of
      // JmxManagerLocatorResponse when the Locator is using SSL & the request 
      // didn't use SSL -> this causes ClassCastException. Not sure how to make 
      // locator meaningful message
      throw new IllegalStateException(unexpected);
    }
    finally {
      distributionConfigProps.clear();
    }
  }

  public static JmxManagerLocatorResponse send(String locatorHost, int locatorPort, int msTimeout) throws IOException {
    return send(locatorHost, locatorPort, msTimeout, null);
  }
  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}
