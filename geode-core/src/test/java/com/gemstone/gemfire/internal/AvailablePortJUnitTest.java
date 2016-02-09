/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import com.gemstone.gemfire.admin.internal.InetAddressUtil;
import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author dsmith
 * @author klund
 */
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
    System.out.println("bind addr="+System.getProperty("gemfire.bind-address"));
    assertFalse(AvailablePort.isPortAvailable(port, AvailablePort.SOCKET));
  }
  
  // multicast availability is tested in JGroupsMessengerJUnitTest
}
