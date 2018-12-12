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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * The SocketUtilsJUnitTest class is a test suite of test cases testing the contract and
 * functionality of the SocketUtils utility class.
 * <p/>
 *
 * @see org.apache.geode.internal.net.SocketUtils
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(MembershipTest.class)
public class SocketUtilsJUnitTest {

  @Test
  public void testCloseSocket() throws IOException {
    final Socket mockSocket = mock(Socket.class, "closeSocketTest");
    doNothing().when(mockSocket).close();

    assertThat(SocketUtils.close(mockSocket)).isTrue();
    verify(mockSocket, times(1)).close();
  }

  @Test
  public void testCloseSocketThrowsIOException() throws IOException {
    final Socket mockSocket = mock(Socket.class, "closeSocketThrowsIOExceptionTest");
    doThrow(new IOException("Mock Exception")).when(mockSocket).close();

    assertThat(SocketUtils.close(mockSocket)).isFalse();
    verify(mockSocket, times(1)).close();
  }

  @Test
  public void testCloseSocketWithNull() {
    assertThat(SocketUtils.close((Socket) null)).isTrue();
  }

  @Test
  public void testCloseServerSocket() throws IOException {
    final ServerSocket mockServerSocket = mock(ServerSocket.class, "closeServerSocketTest");
    doNothing().when(mockServerSocket).close();

    assertThat(SocketUtils.close(mockServerSocket)).isTrue();
    verify(mockServerSocket, times(1)).close();
  }

  @Test
  public void testCloseServerSocketThrowsIOException() throws IOException {
    final ServerSocket mockServerSocket =
        mock(ServerSocket.class, "closeServerSocketThrowsIOExceptionTest");
    doThrow(new IOException("Mock Exception")).when(mockServerSocket).close();

    assertThat(SocketUtils.close(mockServerSocket)).isFalse();
    verify(mockServerSocket, times(1)).close();
  }

  @Test
  public void testCloseServerSocketWithNull() {
    assertThat(SocketUtils.close((ServerSocket) null)).isTrue();
  }
}
