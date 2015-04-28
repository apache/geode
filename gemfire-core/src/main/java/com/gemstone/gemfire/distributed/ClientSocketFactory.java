/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Interface <code>ClientSocketFactory</code> is used to create non-default
 * client sockets.  Set the system property gemfire.clientSocketFactory to the
 * full name of your factory implementation, and GemFire will use your
 * factory to manufacture sockets when it connects to server caches.
 * 
 * @author Barry Oglesby
 * 
 * @since 6.5
 * 
 */
public interface ClientSocketFactory {

  /**
   * Creates a <code>Socket</code> for the input address and port
   * 
   * @param address
   *          The <code>InetAddress</code> of the server
   * @param port
   *          The port of the server
   * 
   * @return a <code>Socket</code> for the input address and port
   */
  public Socket createSocket(InetAddress address, int port) throws IOException;
}
