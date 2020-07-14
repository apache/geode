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
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLSocket;

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

  @Test
  public void readFromSocketWithHeapBuffer() throws IOException {
    Socket socket = mock(Socket.class);
    SocketChannel channel = mock(SocketChannel.class);
    when(socket.getChannel()).thenReturn(channel);
    final ByteBuffer buffer = ByteBuffer.allocate(100); // heap buffer
    byte[] bytes = new byte[100];
    InputStream stream = new ByteArrayInputStream(bytes);
    when(channel.read(buffer)).thenAnswer((answer) -> {
      buffer.put(bytes);
      return buffer.position();
    });
    assertThat(buffer.hasArray()).isTrue();
    SocketUtils.readFromSocket(socket, buffer, stream);
    // the channel was used to read the bytes
    verify(channel, times(1)).read(buffer);
    // the buffer was filled
    assertThat(buffer.position()).isEqualTo(bytes.length);
    // the stream was not used
    assertThat(stream.available()).isEqualTo(bytes.length);
  }


  @Test
  public void readFromSocketWithDirectBuffer() throws IOException {
    Socket socket = mock(Socket.class);
    SocketChannel channel = mock(SocketChannel.class);
    when(socket.getChannel()).thenReturn(channel);
    final ByteBuffer buffer = ByteBuffer.allocateDirect(100); // non-heap buffer
    byte[] bytes = new byte[100];
    InputStream stream = new ByteArrayInputStream(bytes);
    when(channel.read(buffer)).thenAnswer((answer) -> {
      buffer.put(bytes);
      return buffer.position();
    });
    assertThat(buffer.hasArray()).isFalse();
    SocketUtils.readFromSocket(socket, buffer, stream);
    // the channel was used to read the bytes
    verify(channel, times(1)).read(buffer);
    // the buffer was filled
    assertThat(buffer.position()).isEqualTo(bytes.length);
    // the stream was not used
    assertThat(stream.available()).isEqualTo(bytes.length);
  }

  @Test
  public void readFromSSLSocketWithHeapBuffer() throws IOException {
    Socket socket = mock(SSLSocket.class);
    SocketChannel channel = mock(SocketChannel.class);
    when(socket.getChannel()).thenReturn(channel);
    final ByteBuffer buffer = ByteBuffer.allocate(100); // heap buffer
    byte[] bytes = new byte[100];
    InputStream stream = new ByteArrayInputStream(bytes);
    assertThat(stream.available()).isEqualTo(bytes.length);
    SocketUtils.readFromSocket(socket, buffer, stream);
    // the channel was not used to read the bytes
    verify(channel, times(0)).read(buffer);
    // the buffer was filled
    assertThat(buffer.position()).isEqualTo(bytes.length);
    // the stream was used to read the bytes
    assertThat(stream.available()).isZero();
  }


  @Test
  public void readFromSSLSocketWithDirectBuffer() throws IOException {
    Socket socket = mock(SSLSocket.class);
    SocketChannel channel = mock(SocketChannel.class);
    when(socket.getChannel()).thenReturn(channel);
    final ByteBuffer buffer = ByteBuffer.allocateDirect(100); // non-heap buffer
    byte[] bytes = new byte[100];
    InputStream stream = new ByteArrayInputStream(bytes);
    assertThat(stream.available()).isEqualTo(bytes.length);
    SocketUtils.readFromSocket(socket, buffer, stream);
    // the channel was not used
    verify(channel, times(0)).read(buffer);
    // the buffer was filled
    assertThat(buffer.position()).isEqualTo(bytes.length);
    // the stream was used to fill the buffer
    assertThat(stream.available()).isZero();
  }
}
