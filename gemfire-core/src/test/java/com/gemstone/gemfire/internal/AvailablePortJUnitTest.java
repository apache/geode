/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import com.gemstone.gemfire.admin.internal.InetAddressUtil;
import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.junit.UnitTest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author dsmith
 * @author klund
 */
@Ignore
@Category(UnitTest.class)
public class AvailablePortJUnitTest {
  
  private ServerSocket socket;
  
  @After
  public void tearDown() throws IOException {
    if (socket != null) {
      socket.close();
    }
  }
  
  @Test
  public void testIsPortAvailable() throws IOException {
    socket = new ServerSocket();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    socket.bind(new InetSocketAddress(InetAddressUtil.LOOPBACK,  port));

    assertFalse(AvailablePort.isPortAvailable(port, AvailablePort.SOCKET, InetAddress.getByName(InetAddressUtil.LOOPBACK_ADDRESS)));
    //Get local host will return the hostname for the server, so this should succeed, since we're bound to the loopback address only.
    assertTrue(AvailablePort.isPortAvailable(port, AvailablePort.SOCKET, InetAddress.getLocalHost()));
    //This should test all interfaces.
    assertFalse(AvailablePort.isPortAvailable(port, AvailablePort.SOCKET));
  }
  
  @Test
  public void testWildcardAddressBound() throws IOException {
    //assumeFalse(SystemUtils.isWindows()); // See bug #39368
    socket = new ServerSocket();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    socket.bind(new InetSocketAddress((InetAddress)null, port));
    assertFalse(AvailablePort.isPortAvailable(port, AvailablePort.SOCKET));
  }
}
