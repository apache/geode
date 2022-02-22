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

package org.apache.geode.distributed.internal.tcpserver;

import static org.apache.geode.distributed.internal.tcpserver.TcpClient.assertNotSslAlert;
import static org.apache.geode.distributed.internal.tcpserver.TcpClient.createDataInputStream;
import static org.apache.geode.distributed.internal.tcpserver.TcpClient.createDataOutputStream;
import static org.apache.geode.distributed.internal.tcpserver.TcpClient.createEOFException;
import static org.apache.geode.distributed.internal.tcpserver.TcpClient.resetSocketAndLogExceptions;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketException;

import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.ObjectSerializer;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;

class TcpClientTest {

  final TcpSocketCreator socketCreator = mock(TcpSocketCreator.class);
  final ObjectSerializer objectSerializer = mock(ObjectSerializer.class);
  final ObjectDeserializer objectDeserializer = mock(ObjectDeserializer.class);
  final TcpSocketFactory socketFactory = mock(TcpSocketFactory.class);

  @Test
  void resetSocketAndLogExceptionsIgnoresExceptionsOnSetSoLinger() throws IOException {
    final Socket socket = mock(Socket.class);
    when(socket.isClosed()).thenReturn(false);
    doThrow(new SocketException()).when(socket).setSoLinger(eq(true), eq(0));

    assertDoesNotThrow(() -> resetSocketAndLogExceptions(socket));

    final InOrder order = inOrder(socket);
    order.verify(socket).isClosed();
    order.verify(socket).setSoLinger(eq(true), eq(0));
    order.verify(socket).close();
    verifyNoMoreInteractions(socket);
  }

  @Test
  void resetSocketAndLogExceptionsIgnoresExceptionsOnClose() throws IOException {
    final Socket socket = mock(Socket.class);
    when(socket.isClosed()).thenReturn(false);
    doThrow(new IOException()).when(socket).close();

    assertDoesNotThrow(() -> resetSocketAndLogExceptions(socket));

    final InOrder order = inOrder(socket);
    order.verify(socket).isClosed();
    order.verify(socket).setSoLinger(eq(true), eq(0));
    order.verify(socket).close();
    verifyNoMoreInteractions(socket);
  }

  @Test
  void resetSocketAndLogExceptionsDoesNothingWhenSocketIsClosed() {
    final Socket socket = mock(Socket.class);
    when(socket.isClosed()).thenReturn(true);

    assertDoesNotThrow(() -> resetSocketAndLogExceptions(socket));

    verify(socket).isClosed();
    verifyNoMoreInteractions(socket);
  }

  @Test
  void sendRequestWritesHeaderRequestAndFlushes() throws IOException {
    final DataOutputStream out = mock(DataOutputStream.class);

    final short ordinalVersion = 42;
    final Object request = new Serializable() {};

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertDoesNotThrow(() -> client.sendRequest(out, ordinalVersion, request));

    final InOrder order = inOrder(out, objectSerializer);
    order.verify(out).writeInt(eq(TcpServer.GOSSIPVERSION));
    order.verify(out).writeShort(eq((int) ordinalVersion));
    order.verify(objectSerializer).writeObject(same(request), same(out));
    order.verify(out).flush();

    verifyNoMoreInteractions(out, objectSerializer);
  }

  @Test
  void getServerVersionReturnsVersionOrdinal() throws IOException, ClassNotFoundException {
    final DataInputStream in = mock(DataInputStream.class);
    final DataOutputStream out = mock(DataOutputStream.class);
    final short versionOrdinal = (short) 42;
    final VersionResponse versionResponse = new VersionResponse();
    versionResponse.setVersionOrdinal(versionOrdinal);

    when(objectDeserializer.readObject(same(in))).thenReturn(versionResponse);

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThat(client.getServerVersion(in, out))
        .isEqualTo(versionOrdinal);

    final InOrder order = inOrder(objectSerializer, objectDeserializer);
    order.verify(objectSerializer).writeObject(any(VersionRequest.class), same(out));
    order.verify(objectDeserializer).readObject(same(in));

    verifyNoMoreInteractions(objectDeserializer, objectSerializer);
  }

  @Test
  void getServerVersionThrowsIOExceptionWhenReadingWrongResponse()
      throws IOException, ClassNotFoundException {
    final DataInputStream in = mock(DataInputStream.class);
    final DataOutputStream out = mock(DataOutputStream.class);

    when(objectDeserializer.readObject(same(in))).thenReturn(new Object());

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThatThrownBy(() -> client.getServerVersion(in, out))
        .isInstanceOf(IOException.class).hasCauseInstanceOf(ClassCastException.class);

    final InOrder order = inOrder(objectSerializer, objectDeserializer);
    order.verify(objectSerializer).writeObject(any(VersionRequest.class), same(out));
    order.verify(objectDeserializer).readObject(same(in));

    verifyNoMoreInteractions(objectDeserializer, objectSerializer);
  }

  @Test
  void getServerVersionThrowsIOExceptionWhenClassNotFoundException()
      throws IOException, ClassNotFoundException {
    final DataInputStream in = mock(DataInputStream.class);
    final DataOutputStream out = mock(DataOutputStream.class);

    when(objectDeserializer.readObject(same(in))).thenThrow(new ClassNotFoundException());

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThatThrownBy(() -> client.getServerVersion(in, out))
        .isInstanceOf(IOException.class).hasCauseInstanceOf(ClassNotFoundException.class);

    final InOrder order = inOrder(objectSerializer, objectDeserializer);
    order.verify(objectSerializer).writeObject(any(VersionRequest.class), same(out));
    order.verify(objectDeserializer).readObject(same(in));

    verifyNoMoreInteractions(objectDeserializer, objectSerializer);
  }

  @Test
  void getServerVersionReturnOldestVersionOnEndOfFile() throws IOException, ClassNotFoundException {
    final DataInputStream in = mock(DataInputStream.class);
    final DataOutputStream out = mock(DataOutputStream.class);

    when(objectDeserializer.readObject(same(in))).thenThrow(new EOFException());

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThat(client.getServerVersion(in, out)).isEqualTo(KnownVersion.OLDEST.ordinal());

    final InOrder order = inOrder(objectSerializer, objectDeserializer);
    order.verify(objectSerializer).writeObject(any(VersionRequest.class), same(out));
    order.verify(objectDeserializer).readObject(same(in));

    verifyNoMoreInteractions(objectDeserializer, objectSerializer);
  }

  @Test
  void getServerVersionResetsSocketOnSetSoTimeoutException() throws IOException {
    final int timeout = 42;

    final ClusterSocketCreator clusterSocketCreator = mock(ClusterSocketCreator.class);
    final HostAndPort address = mock(HostAndPort.class);
    final Socket socket = mock(Socket.class);

    when(socketCreator.forCluster()).thenReturn(clusterSocketCreator);
    when(clusterSocketCreator.connect(eq(address), eq(timeout), any(), same(socketFactory)))
        .thenReturn(socket);
    doThrow(new SocketException()).when(socket).setSoTimeout(eq(timeout));

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThatThrownBy(() -> client.getServerVersion(address, timeout))
        .isInstanceOf(IOException.class);

    final InOrder order = inOrder(socket);
    order.verify(socket).setSoTimeout(eq(timeout));
    order.verify(socket).isClosed();
    order.verify(socket).setSoLinger(eq(true), eq(0));
    order.verify(socket).close();

    verifyNoMoreInteractions(socket);
    verifyNoInteractions(objectDeserializer, objectSerializer);
  }

  @Test
  void getServerVersionDoesNotCatchSSLHandshakeException() throws IOException {
    final int timeout = 42;
    final SSLHandshakeException sslHandshakeException = new SSLHandshakeException("");

    final ClusterSocketCreator clusterSocketCreator = mock(ClusterSocketCreator.class);
    final HostAndPort address = mock(HostAndPort.class);

    when(socketCreator.forCluster()).thenReturn(clusterSocketCreator);
    when(clusterSocketCreator.connect(eq(address), eq(timeout), any(), same(socketFactory)))
        .thenThrow(sslHandshakeException);

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThatThrownBy(() -> client.getServerVersion(address, timeout))
        .isSameAs(sslHandshakeException);

    verifyNoInteractions(objectDeserializer, objectSerializer);
  }

  @Test
  void getServerVersionResetsSocketOnIOException() throws IOException {
    final int timeout = 42;

    final ClusterSocketCreator clusterSocketCreator = mock(ClusterSocketCreator.class);
    final HostAndPort address = mock(HostAndPort.class);
    final Socket socket = mock(Socket.class);

    when(socketCreator.forCluster()).thenReturn(clusterSocketCreator);
    when(clusterSocketCreator.connect(eq(address), eq(timeout), any(), same(socketFactory)))
        .thenReturn(socket);
    doThrow(new IOException()).when(objectSerializer).writeObject(any(), any());

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThatThrownBy(() -> client.getServerVersion(address, timeout))
        .isInstanceOf(IOException.class);

    final InOrder order = inOrder(objectSerializer, socket);
    order.verify(socket).setSoTimeout(eq(timeout));
    verify(socket).getInputStream();
    verify(socket).getOutputStream();
    order.verify(objectSerializer).writeObject(any(), any());
    order.verify(socket).isClosed();
    order.verify(socket).setSoLinger(eq(true), eq(0));
    order.verify(socket).close();

    verifyNoMoreInteractions(socket, objectSerializer);
    verifyNoInteractions(objectDeserializer);
  }

  @Test
  void getServerVersionResetsSocketOnReturn() throws IOException, ClassNotFoundException {
    final int timeout = 42;

    final ClusterSocketCreator clusterSocketCreator = mock(ClusterSocketCreator.class);
    final HostAndPort address = mock(HostAndPort.class);
    final Socket socket = mock(Socket.class);
    final InputStream in = mock(InputStream.class);
    final OutputStream out = mock(OutputStream.class);

    when(socketCreator.forCluster()).thenReturn(clusterSocketCreator);
    when(clusterSocketCreator.connect(eq(address), eq(timeout), any(), same(socketFactory)))
        .thenReturn(socket);
    when(socket.getInputStream()).thenReturn(in);
    when(socket.getOutputStream()).thenReturn(out);
    when(objectDeserializer.readObject(any())).thenReturn(new VersionResponse());

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertDoesNotThrow(() -> client.getServerVersion(address, timeout));

    final InOrder order = inOrder(objectSerializer, objectDeserializer, socket);
    order.verify(socket).setSoTimeout(eq(timeout));
    verify(socket).getInputStream();
    verify(socket).getOutputStream();
    order.verify(objectSerializer).writeObject(any(VersionRequest.class), any());
    order.verify(objectDeserializer).readObject(any());
    order.verify(socket).isClosed();
    order.verify(socket).setSoLinger(eq(true), eq(0));
    order.verify(socket).close();

    verifyNoMoreInteractions(socket, objectSerializer, objectDeserializer);
  }

  @Test
  void getServerVersionReturnsCachedVersion() throws IOException, ClassNotFoundException {
    final int timeout = 0;
    final short ordinalVersion = 42;
    final VersionResponse versionResponse = new VersionResponse();
    versionResponse.setVersionOrdinal(ordinalVersion);

    final ClusterSocketCreator clusterSocketCreator = mock(ClusterSocketCreator.class);
    final HostAndPort address = mock(HostAndPort.class);
    final Socket socket = mock(Socket.class);

    when(socketCreator.forCluster()).thenReturn(clusterSocketCreator);
    when(clusterSocketCreator.connect(eq(address), eq(timeout), any(), same(socketFactory)))
        .thenReturn(socket);
    when(socket.getInputStream()).thenReturn(mock(InputStream.class));
    when(socket.getOutputStream()).thenReturn(mock(OutputStream.class));
    when(objectDeserializer.readObject(any())).thenReturn(versionResponse);

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThat(client.getServerVersion(address, timeout)).isEqualTo(ordinalVersion);
    // second invocation should not open sockets, as verified below.
    assertThat(client.getServerVersion(address, timeout)).isEqualTo(ordinalVersion);

    verify(socketCreator).forCluster();
    verify(clusterSocketCreator).connect(same(address), eq(timeout), any(), same(socketFactory));
    verify(objectSerializer).writeObject(any(VersionRequest.class), any());
    verify(objectDeserializer).readObject(any());

    verifyNoMoreInteractions(socketCreator, objectSerializer, objectDeserializer, socketFactory,
        clusterSocketCreator, address);
  }

  @Test
  void createDataOutputStreamIsVersionedWhenVersionOlderThanCurrent() throws IOException {
    final Socket socket = mock(Socket.class);
    final KnownVersion version = KnownVersion.GEODE_1_1_0;

    assertThat(createDataOutputStream(socket, version))
        .isInstanceOf(VersionedDataOutputStream.class)
        .satisfies(
            v -> assertThat(((VersionedDataOutputStream) v).getVersion()).isEqualTo(version));
  }

  @Test
  void createDataOutputStreamIsNotVersionedWhenVersionIsCurrent() throws IOException {
    final Socket socket = mock(Socket.class);
    final KnownVersion version = KnownVersion.CURRENT;

    assertThat(createDataOutputStream(socket, version))
        .isNotInstanceOf(VersionedDataOutputStream.class);
  }

  @Test
  void createDataInputStreamIsVersionedWhenVersionOlderThanCurrent() throws IOException {
    final Socket socket = mock(Socket.class);
    final KnownVersion version = KnownVersion.GEODE_1_1_0;

    assertThat(createDataInputStream(socket, version))
        .isInstanceOf(VersionedDataInputStream.class)
        .satisfies(v -> assertThat(((VersionedDataInputStream) v).getVersion()).isEqualTo(version));
  }

  @Test
  void createDataInputStreamIsNotVersionedWhenVersionIsCurrent() throws IOException {
    final Socket socket = mock(Socket.class);
    final KnownVersion version = KnownVersion.CURRENT;

    assertThat(createDataInputStream(socket, version))
        .isNotInstanceOf(VersionedDataInputStream.class);
  }


  @Test
  void createEOFExceptionAddsCause() {
    final HostAndPort address = new HostAndPort("localhost", 1234);
    final Exception cause = new Exception("ouch");

    assertThat(createEOFException(address, cause))
        .isInstanceOf(EOFException.class)
        .hasRootCause(cause);
  }

  @Test
  void receiveResponseThrowsEOFException() throws IOException, ClassNotFoundException {
    final DataInputStream in = mock(DataInputStream.class);

    final HostAndPort address = new HostAndPort("localhost", 1234);
    final EOFException exception = new EOFException("ouch");

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    when(objectDeserializer.readObject(same(in))).thenThrow(exception);

    assertThatThrownBy(() -> client.receiveResponse(in, address))
        .isInstanceOf(EOFException.class)
        .hasRootCause(exception);

    verify(objectDeserializer).readObject(same(in));

    verifyNoMoreInteractions(objectDeserializer);
    verifyNoInteractions(socketCreator, objectSerializer, socketFactory);
  }

  @Test
  void requestToServerReturnsNullIfNoReplyExpected() throws IOException, ClassNotFoundException {
    final Object request = new Object();
    final int timeout = 42;
    final short serverVersionOrdinal = KnownVersion.CURRENT.ordinal();
    final KnownVersion serverVersion = KnownVersion.CURRENT;

    final ClusterSocketCreator clusterSocketCreator = mock(ClusterSocketCreator.class);
    final HostAndPort address = mock(HostAndPort.class);
    final Socket socket = mock(Socket.class);

    when(socketCreator.forCluster()).thenReturn(clusterSocketCreator);
    when(clusterSocketCreator.connect(eq(address), eq(timeout), any(), same(socketFactory)))
        .thenReturn(socket);
    when(socket.getOutputStream()).thenReturn(mock(OutputStream.class));

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThat(client.requestToServer(address, request, timeout, false, serverVersionOrdinal,
        serverVersion, null)).isNull();

    final InOrder order = inOrder(socketCreator, clusterSocketCreator, socket, objectSerializer);
    order.verify(socketCreator).forCluster();
    order.verify(clusterSocketCreator).connect(eq(address), eq(timeout), any(),
        same(socketFactory));
    order.verify(socket).setSoTimeout(eq(timeout));
    order.verify(socket).getOutputStream();
    order.verify(objectSerializer).writeObject(same(request), any());
    order.verify(socket).close();

    verifyNoMoreInteractions(socketCreator, clusterSocketCreator, socket, objectSerializer);
    verifyNoInteractions(objectDeserializer, socketFactory);
  }

  @Test
  void requestToServerReturnsResponseIfReplyExpected() throws IOException, ClassNotFoundException {
    final Object request = new Object();
    final Object response = new Object();
    final int timeout = 42;
    final short serverVersionOrdinal = KnownVersion.CURRENT.ordinal();
    final KnownVersion serverVersion = KnownVersion.CURRENT;

    final ClusterSocketCreator clusterSocketCreator = mock(ClusterSocketCreator.class);
    final HostAndPort address = mock(HostAndPort.class);
    final Socket socket = mock(Socket.class);

    when(socketCreator.forCluster()).thenReturn(clusterSocketCreator);
    when(clusterSocketCreator.connect(eq(address), eq(timeout), any(), same(socketFactory)))
        .thenReturn(socket);
    when(socket.getInputStream()).thenReturn(mock(InputStream.class));
    when(socket.getOutputStream()).thenReturn(mock(OutputStream.class));
    when(objectDeserializer.readObject(any())).thenReturn(response);

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThat(client.requestToServer(address, request, timeout, true, serverVersionOrdinal,
        serverVersion, null)).isSameAs(response);

    final InOrder order =
        inOrder(socketCreator, clusterSocketCreator, socket, objectSerializer, objectDeserializer);
    order.verify(socketCreator).forCluster();
    order.verify(clusterSocketCreator).connect(eq(address), eq(timeout), any(),
        same(socketFactory));
    order.verify(socket).setSoTimeout(eq(timeout));
    order.verify(socket).getOutputStream();
    order.verify(objectSerializer).writeObject(same(request), any());
    order.verify(socket).getInputStream();
    order.verify(objectDeserializer).readObject(any());
    verify(socketCreator, atLeast(0)).forCluster();
    verify(socket, atLeast(0)).isClosed();
    verify(clusterSocketCreator, atLeast(0)).useSSL();
    order.verify(socket).setSoLinger(eq(true), eq(0));
    order.verify(socket).close();

    verifyNoMoreInteractions(socketCreator, clusterSocketCreator, socket, objectSerializer,
        objectDeserializer);
    verifyNoInteractions(socketFactory);
  }

  @Test
  void requestToServerDoesNotSetSoLingerWhenUsingSsl() throws IOException, ClassNotFoundException {
    final Object request = new Object();
    final Object response = new Object();
    final int timeout = 42;
    final short serverVersionOrdinal = KnownVersion.CURRENT.ordinal();
    final KnownVersion serverVersion = KnownVersion.CURRENT;

    final ClusterSocketCreator clusterSocketCreator = mock(ClusterSocketCreator.class);
    final HostAndPort address = mock(HostAndPort.class);
    final Socket socket = mock(Socket.class);

    when(socketCreator.forCluster()).thenReturn(clusterSocketCreator);
    when(clusterSocketCreator.connect(eq(address), eq(timeout), any(), same(socketFactory)))
        .thenReturn(socket);
    when(clusterSocketCreator.useSSL()).thenReturn(true);
    when(socket.getInputStream()).thenReturn(mock(InputStream.class));
    when(socket.getOutputStream()).thenReturn(mock(OutputStream.class));
    when(objectDeserializer.readObject(any())).thenReturn(response);

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThat(client.requestToServer(address, request, timeout, true, serverVersionOrdinal,
        serverVersion, null)).isSameAs(response);

    final InOrder order =
        inOrder(socketCreator, clusterSocketCreator, socket, objectSerializer, objectDeserializer);
    order.verify(socketCreator).forCluster();
    order.verify(clusterSocketCreator).connect(eq(address), eq(timeout), any(),
        same(socketFactory));
    order.verify(socket).setSoTimeout(eq(timeout));
    order.verify(socket).getOutputStream();
    order.verify(objectSerializer).writeObject(same(request), any());
    order.verify(socket).getInputStream();
    order.verify(objectDeserializer).readObject(any());
    verify(socketCreator, atLeast(0)).forCluster();
    verify(socket, atLeast(0)).isClosed();
    verify(clusterSocketCreator, atLeast(0)).useSSL();
    order.verify(socket).close();

    verifyNoMoreInteractions(socketCreator, clusterSocketCreator, socket, objectSerializer,
        objectDeserializer);
    verifyNoInteractions(socketFactory);
  }

  @Test
  void requestToServerPassesSSLHandshakeExceptionCausedByEOFException() throws Exception {
    final ClusterSocketCreator clusterSocketCreator = Mockito.mock(ClusterSocketCreator.class);
    final HostAndPort address = mock(HostAndPort.class);

    final SSLHandshakeException sslHandshakeException =
        new SSLHandshakeException("Remote host terminated the handshake");
    sslHandshakeException.initCause(new EOFException("SSL peer shut down incorrectly"));

    when(socketCreator.forCluster()).thenReturn(clusterSocketCreator);
    when(clusterSocketCreator.connect(any(), anyInt(), any(), any()))
        .thenThrow(sslHandshakeException);

    final TcpClient client =
        new TcpClient(socketCreator, objectSerializer, objectDeserializer, socketFactory);

    assertThatExceptionOfType(SSLHandshakeException.class)
        .isThrownBy(() -> client.requestToServer(address, new Object(), 42, false))
        .withRootCauseInstanceOf(EOFException.class);
  }

  @Test
  void assertNotSslAlertThrowSSLHandshakeExceptionWhenAlertIsDetected() throws IOException {
    final DataInputStream in = mock(DataInputStream.class);
    when(in.read()).thenReturn(0x15);

    assertThatThrownBy(() -> assertNotSslAlert(in))
        .isInstanceOf(SSLHandshakeException.class)
        .hasMessage("Server expecting SSL handshake.");

    final InOrder order = inOrder(in);
    order.verify(in).mark(eq(1));
    // noinspection ResultOfMethodCallIgnored
    order.verify(in).read();
    order.verify(in).reset();

    verifyNoMoreInteractions(in);
  }

  @Test
  void assertNotSslAlertDoesNotThrowWhenAlertIsNotDetectedAndStreamIsReset() throws IOException {
    final DataInputStream in = mock(DataInputStream.class);
    when(in.read()).thenReturn(0);

    assertDoesNotThrow(() -> assertNotSslAlert(in));

    final InOrder order = inOrder(in);
    order.verify(in).mark(eq(1));
    // noinspection ResultOfMethodCallIgnored
    order.verify(in).read();
    order.verify(in).reset();

    verifyNoMoreInteractions(in);
  }

  @Test
  void assertNotSslAlertDoesNotThrowOnIOExceptionAndStreamIsReset() throws IOException {
    final DataInputStream in = mock(DataInputStream.class);
    when(in.read()).thenReturn(-1);

    assertDoesNotThrow(() -> assertNotSslAlert(in));

    final InOrder order = inOrder(in);
    order.verify(in).mark(eq(1));
    // noinspection ResultOfMethodCallIgnored
    order.verify(in).read();
    order.verify(in).reset();

    verifyNoMoreInteractions(in);
  }

}
