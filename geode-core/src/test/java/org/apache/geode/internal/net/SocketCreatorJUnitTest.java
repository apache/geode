/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.net;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import javax.net.ssl.SSLSocket;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.util.test.TestUtil;

@Category({MembershipTest.class})
public class SocketCreatorJUnitTest {

  @Test
  public void testCreateSocketCreatorWithKeystoreUnset() throws Exception {
    SSLConfig testSSLConfig = new SSLConfig();
    testSSLConfig.setEnabled(true);
    testSSLConfig.setKeystore(null);
    testSSLConfig.setKeystorePassword("");
    testSSLConfig.setTruststore(getSingleKeyKeystore());
    testSSLConfig.setTruststorePassword("password");
    // GEODE-3393: This would fail with java.io.FileNotFoundException: $USER_HOME/.keystore
    new SocketCreator(testSSLConfig);

  }

  @Test
  public void testConfigureServerSSLSocketSetsSoTimeout() throws Exception {
    final SocketCreator socketCreator = new SocketCreator(mock(SSLConfig.class));
    final SSLSocket socket = mock(SSLSocket.class);

    final int timeout = 1938236;
    socketCreator.handshakeIfSocketIsSSL(socket, timeout);
    verify(socket).setSoTimeout(timeout);
  }

  @Test
  public void testConfigureServerPlainSocketDoesntSetSoTimeout() throws Exception {
    final SocketCreator socketCreator = new SocketCreator(mock(SSLConfig.class));
    final Socket socket = mock(Socket.class);
    final int timeout = 1938236;

    socketCreator.handshakeIfSocketIsSSL(socket, timeout);
    verify(socket, never()).setSoTimeout(timeout);
  }

  @Test
  public void testBindExceptionMessageFormattingWithBindAddr() throws Exception {
    testBindExceptionMessageFormatting(InetAddress.getLocalHost());
  }

  @Test
  public void testBindExceptionMessageFormattingNullBindAddr() throws Exception {
    testBindExceptionMessageFormatting(null);
  }

  private void testBindExceptionMessageFormatting(InetAddress inetAddress) throws Exception {
    final SocketCreator socketCreator = new SocketCreator(mock(SSLConfig.class));
    final Socket socket = mock(Socket.class);

    ServerSocket serverSocket = null;
    try {
      serverSocket = socketCreator.createServerSocket(11234, 10, inetAddress);
      assertThatExceptionOfType(BindException.class).isThrownBy(() -> {
        // call twice on the same port to trigger exception
        socketCreator.createServerSocket(11234, 10, inetAddress);
      }).withMessageContaining("11234")
          .withMessageContaining(InetAddress.getLocalHost().getHostAddress());
    } finally {
      if (serverSocket != null) {
        serverSocket.close();
      }
    }
  }

  private String getSingleKeyKeystore() {
    return TestUtil.getResourcePath(getClass(), "/ssl/trusted.keystore");
  }
}
