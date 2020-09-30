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

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_TASK;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_WRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_OVERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;
import static javax.net.ssl.SSLEngineResult.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.GemFireIOException;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class NioSslEngineTest {
  private static final int netBufferSize = 10000;
  private static final int appBufferSize = 20000;

  private SSLEngine mockEngine;
  private DMStats mockStats;
  private NioSslEngine nioSslEngine;
  private NioSslEngine spyNioSslEngine;

  @Before
  public void setUp() throws Exception {
    mockEngine = mock(SSLEngine.class);

    SSLSession mockSession = mock(SSLSession.class);
    when(mockEngine.getSession()).thenReturn(mockSession);
    when(mockSession.getPacketBufferSize()).thenReturn(netBufferSize);
    when(mockSession.getApplicationBufferSize()).thenReturn(appBufferSize);

    mockStats = mock(DMStats.class);

    nioSslEngine = new NioSslEngine(mockEngine, new BufferPool(mockStats));
    spyNioSslEngine = spy(nioSslEngine);
  }

  @Test
  public void handshake() throws Exception {
    SocketChannel mockChannel = mock(SocketChannel.class);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(100, 100, 100, 0);
    Socket mockSocket = mock(Socket.class);
    when(mockChannel.socket()).thenReturn(mockSocket);
    when(mockSocket.isClosed()).thenReturn(false);

    // initial read of handshake status followed by read of handshake status after task execution
    when(mockEngine.getHandshakeStatus()).thenReturn(NEED_UNWRAP, NEED_WRAP);

    // interleaved wraps/unwraps/task-execution
    when(mockEngine.unwrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(OK, NEED_WRAP, 100, 100),
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_UNWRAP, 0, 0),
        new SSLEngineResult(OK, NEED_TASK, 100, 0));

    when(mockEngine.getDelegatedTask()).thenReturn(() -> {
    }, (Runnable) null);

    when(mockEngine.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(OK, NEED_UNWRAP, 100, 100),
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_WRAP, 0, 0),
        new SSLEngineResult(CLOSED, FINISHED, 100, 0));

    spyNioSslEngine.handshake(mockChannel, 10000, ByteBuffer.allocate(netBufferSize));
    verify(mockEngine, atLeast(2)).getHandshakeStatus();
    verify(mockEngine, times(3)).wrap(any(ByteBuffer.class), any(ByteBuffer.class));
    verify(mockEngine, times(3)).unwrap(any(ByteBuffer.class), any(ByteBuffer.class));
    verify(spyNioSslEngine, times(2)).expandWriteBuffer(any(BufferPool.BufferType.class),
        any(ByteBuffer.class), any(Integer.class));
    verify(spyNioSslEngine, times(1)).handleBlockingTasks();
    verify(mockChannel, times(3)).read(any(ByteBuffer.class));
  }

  @Test
  public void handshakeWithInsufficientBufferSize() throws Exception {
    SocketChannel mockChannel = mock(SocketChannel.class);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(100, 100, 100, 0);
    Socket mockSocket = mock(Socket.class);
    when(mockChannel.socket()).thenReturn(mockSocket);
    when(mockSocket.isClosed()).thenReturn(false);

    // // initial read of handshake status followed by read of handshake status after task execution
    // when(mockEngine.getHandshakeStatus()).thenReturn(NEED_UNWRAP, NEED_WRAP);
    //
    // // interleaved wraps/unwraps/task-execution
    // when(mockEngine.unwrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
    // new SSLEngineResult(OK, NEED_WRAP, 100, 100),
    // new SSLEngineResult(BUFFER_OVERFLOW, NEED_UNWRAP, 0, 0),
    // new SSLEngineResult(OK, NEED_TASK, 100, 0));
    //
    // when(mockEngine.getDelegatedTask()).thenReturn(() -> {
    // }, (Runnable) null);
    //
    // when(mockEngine.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
    // new SSLEngineResult(OK, NEED_UNWRAP, 100, 100),
    // new SSLEngineResult(BUFFER_OVERFLOW, NEED_WRAP, 0, 0),
    // new SSLEngineResult(CLOSED, FINISHED, 100, 0));
    //
    assertThatThrownBy(() -> spyNioSslEngine.handshake(mockChannel, 10000,
        ByteBuffer.allocate(netBufferSize / 2))).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Provided buffer is too small");
  }

  @Test
  public void handshakeDetectsClosedSocket() throws Exception {
    SocketChannel mockChannel = mock(SocketChannel.class);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(100, 100, 100, 0);
    Socket mockSocket = mock(Socket.class);
    when(mockChannel.socket()).thenReturn(mockSocket);
    when(mockSocket.isClosed()).thenReturn(true);

    // initial read of handshake status followed by read of handshake status after task execution
    when(mockEngine.getHandshakeStatus()).thenReturn(NEED_UNWRAP);

    ByteBuffer byteBuffer = ByteBuffer.allocate(netBufferSize);

    assertThatThrownBy(() -> spyNioSslEngine.handshake(mockChannel, 10000, byteBuffer))
        .isInstanceOf(
            SocketException.class)
        .hasMessageContaining("handshake terminated");
  }

  @Test
  public void handshakeDoesNotTerminateWithFinished() throws Exception {
    SocketChannel mockChannel = mock(SocketChannel.class);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(100, 100, 100, 0);
    Socket mockSocket = mock(Socket.class);
    when(mockChannel.socket()).thenReturn(mockSocket);
    when(mockSocket.isClosed()).thenReturn(false);

    // initial read of handshake status followed by read of handshake status after task execution
    when(mockEngine.getHandshakeStatus()).thenReturn(NEED_UNWRAP);

    // interleaved wraps/unwraps/task-execution
    when(mockEngine.unwrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(OK, NEED_WRAP, 100, 100));

    when(mockEngine.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(CLOSED, NOT_HANDSHAKING, 100, 0));

    ByteBuffer byteBuffer = ByteBuffer.allocate(netBufferSize);

    assertThatThrownBy(() -> spyNioSslEngine.handshake(mockChannel, 10000, byteBuffer))
        .isInstanceOf(
            SSLHandshakeException.class)
        .hasMessageContaining("SSL Handshake terminated with status");
  }


  @Test
  public void checkClosed() throws Exception {
    nioSslEngine.checkClosed();
  }

  @Test(expected = IOException.class)
  public void checkClosedThrows() throws Exception {
    when(mockEngine.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(CLOSED, FINISHED, 0, 100));
    nioSslEngine.close(mock(SocketChannel.class));
    nioSslEngine.checkClosed();
  }

  @Test
  public void wrap() throws Exception {
    // make the application data too big to fit into the engine's encryption buffer
    ByteBuffer appData = ByteBuffer.allocate(nioSslEngine.myNetData.capacity() + 100);
    byte[] appBytes = new byte[appData.capacity()];
    Arrays.fill(appBytes, (byte) 0x1F);
    appData.put(appBytes);
    appData.flip();

    // create an engine that will transfer bytes from the application buffer to the encrypted buffer
    TestSSLEngine testEngine = new TestSSLEngine();
    testEngine.addReturnResult(
        new SSLEngineResult(OK, NEED_TASK, appData.remaining(), appData.remaining()));
    spyNioSslEngine.engine = testEngine;

    ByteBuffer wrappedBuffer = spyNioSslEngine.wrap(appData);

    verify(spyNioSslEngine, times(1)).expandWriteBuffer(any(BufferPool.BufferType.class),
        any(ByteBuffer.class), any(Integer.class));
    appData.flip();
    assertThat(wrappedBuffer).isEqualTo(appData);
    verify(spyNioSslEngine, times(1)).handleBlockingTasks();
  }

  @Test
  public void wrapFails() {
    // make the application data too big to fit into the engine's encryption buffer
    ByteBuffer appData = ByteBuffer.allocate(nioSslEngine.myNetData.capacity() + 100);
    byte[] appBytes = new byte[appData.capacity()];
    Arrays.fill(appBytes, (byte) 0x1F);
    appData.put(appBytes);
    appData.flip();

    // create an engine that will transfer bytes from the application buffer to the encrypted buffer
    TestSSLEngine testEngine = new TestSSLEngine();
    testEngine.addReturnResult(
        new SSLEngineResult(CLOSED, NEED_TASK, appData.remaining(), appData.remaining()));
    spyNioSslEngine.engine = testEngine;

    assertThatThrownBy(() -> spyNioSslEngine.wrap(appData)).isInstanceOf(SSLException.class)
        .hasMessageContaining("Error encrypting data");
  }

  @Test
  public void unwrapWithBufferOverflow() throws Exception {
    // make the application data too big to fit into the engine's encryption buffer
    int originalPeerAppDataCapacity = nioSslEngine.peerAppData.capacity();
    int originalPeerAppDataPosition = originalPeerAppDataCapacity / 2;
    nioSslEngine.peerAppData.position(originalPeerAppDataPosition);
    ByteBuffer wrappedData = ByteBuffer.allocate(originalPeerAppDataCapacity + 100);
    byte[] netBytes = new byte[wrappedData.capacity()];
    Arrays.fill(netBytes, (byte) 0x1F);
    wrappedData.put(netBytes);
    wrappedData.flip();

    // create an engine that will transfer bytes from the application buffer to the encrypted buffer
    TestSSLEngine testEngine = new TestSSLEngine();
    spyNioSslEngine.engine = testEngine;

    testEngine.addReturnResult(
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_UNWRAP, 0, 0), // results in 30,000 byte buffer
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_UNWRAP, 0, 0), // 50,000 bytes
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_UNWRAP, 0, 0), // 90,000 bytes
        new SSLEngineResult(OK, FINISHED, netBytes.length, netBytes.length));

    int expectedCapacity = 2 * originalPeerAppDataCapacity - originalPeerAppDataPosition;
    expectedCapacity =
        2 * (expectedCapacity - originalPeerAppDataPosition) + originalPeerAppDataPosition;
    expectedCapacity =
        2 * (expectedCapacity - originalPeerAppDataPosition) + originalPeerAppDataPosition;
    ByteBuffer unwrappedBuffer = spyNioSslEngine.unwrap(wrappedData);
    unwrappedBuffer.flip();
    assertThat(unwrappedBuffer.capacity()).isEqualTo(expectedCapacity);
  }


  @Test
  public void unwrapWithBufferUnderflow() throws Exception {
    ByteBuffer wrappedData = ByteBuffer.allocate(nioSslEngine.peerAppData.capacity());
    byte[] netBytes = new byte[wrappedData.capacity() / 2];
    Arrays.fill(netBytes, (byte) 0x1F);
    wrappedData.put(netBytes);
    wrappedData.flip();

    // create an engine that will transfer bytes from the application buffer to the encrypted buffer
    TestSSLEngine testEngine = new TestSSLEngine();
    testEngine.addReturnResult(new SSLEngineResult(BUFFER_UNDERFLOW, NEED_TASK, 0, 0));
    spyNioSslEngine.engine = testEngine;

    ByteBuffer unwrappedBuffer = spyNioSslEngine.unwrap(wrappedData);
    unwrappedBuffer.flip();
    assertThat(unwrappedBuffer.remaining()).isEqualTo(0);
    assertThat(wrappedData.position()).isEqualTo(netBytes.length);
  }

  @Test
  public void unwrapWithDecryptionError() {
    // make the application data too big to fit into the engine's encryption buffer
    ByteBuffer wrappedData = ByteBuffer.allocate(nioSslEngine.peerAppData.capacity());
    byte[] netBytes = new byte[wrappedData.capacity() / 2];
    Arrays.fill(netBytes, (byte) 0x1F);
    wrappedData.put(netBytes);
    wrappedData.flip();

    // create an engine that will transfer bytes from the application buffer to the encrypted buffer
    TestSSLEngine testEngine = new TestSSLEngine();
    testEngine.addReturnResult(new SSLEngineResult(CLOSED, FINISHED, 0, 0));
    spyNioSslEngine.engine = testEngine;

    assertThatThrownBy(() -> spyNioSslEngine.unwrap(wrappedData)).isInstanceOf(SSLException.class)
        .hasMessageContaining("Error decrypting data");
  }

  @Test
  public void ensureUnwrappedCapacity() {
    ByteBuffer wrappedBuffer = ByteBuffer.allocate(netBufferSize);
    int requestedCapacity = nioSslEngine.getUnwrappedBuffer(wrappedBuffer).capacity() * 2;
    ByteBuffer unwrappedBuffer = nioSslEngine.ensureUnwrappedCapacity(requestedCapacity);
    assertThat(unwrappedBuffer.capacity()).isGreaterThanOrEqualTo(requestedCapacity);
  }

  @Test
  public void close() throws Exception {
    SocketChannel mockChannel = mock(SocketChannel.class);
    Socket mockSocket = mock(Socket.class);
    when(mockChannel.socket()).thenReturn(mockSocket);
    when(mockSocket.isClosed()).thenReturn(false);

    when(mockEngine.isOutboundDone()).thenReturn(Boolean.FALSE);
    when(mockEngine.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(CLOSED, FINISHED, 0, 0));
    nioSslEngine.close(mockChannel);
    assertThatThrownBy(() -> nioSslEngine.checkClosed()).isInstanceOf(IOException.class)
        .hasMessageContaining("NioSslEngine has been closed");
    nioSslEngine.close(mockChannel);
  }

  @Test
  public void closeWhenUnwrapError() throws Exception {
    SocketChannel mockChannel = mock(SocketChannel.class);
    Socket mockSocket = mock(Socket.class);
    when(mockChannel.socket()).thenReturn(mockSocket);
    when(mockSocket.isClosed()).thenReturn(true);

    when(mockEngine.isOutboundDone()).thenReturn(Boolean.FALSE);
    when(mockEngine.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(BUFFER_OVERFLOW, FINISHED, 0, 0));
    assertThatThrownBy(() -> nioSslEngine.close(mockChannel)).isInstanceOf(GemFireIOException.class)
        .hasMessageContaining("exception closing SSL session")
        .hasCauseInstanceOf(SSLException.class);
  }

  @Test
  public void closeWhenSocketWriteError() throws Exception {
    SocketChannel mockChannel = mock(SocketChannel.class);
    Socket mockSocket = mock(Socket.class);
    when(mockChannel.socket()).thenReturn(mockSocket);
    when(mockSocket.isClosed()).thenReturn(true);

    when(mockEngine.isOutboundDone()).thenReturn(Boolean.FALSE);
    when(mockEngine.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenAnswer((x) -> {
      // give the NioSslEngine something to write on its socket channel, simulating a TLS close
      // message
      nioSslEngine.myNetData.put("Goodbye cruel world".getBytes());
      return new SSLEngineResult(CLOSED, FINISHED, 0, 0);
    });
    when(mockChannel.write(any(ByteBuffer.class))).thenThrow(new ClosedChannelException());
    nioSslEngine.close(mockChannel);
    verify(mockChannel, times(1)).write(any(ByteBuffer.class));
  }

  @Test
  public void ensureWrappedCapacityOfSmallMessage() {
    ByteBuffer buffer = ByteBuffer.allocate(netBufferSize);
    assertThat(
        nioSslEngine.ensureWrappedCapacity(10, buffer, BufferPool.BufferType.UNTRACKED))
            .isEqualTo(buffer);
  }

  @Test
  public void ensureWrappedCapacityWithNoBuffer() {
    assertThat(
        nioSslEngine.ensureWrappedCapacity(10, null, BufferPool.BufferType.UNTRACKED)
            .capacity())
                .isEqualTo(netBufferSize);
  }

  @Test
  public void readAtLeast() throws Exception {
    final int amountToRead = 150;
    final int individualRead = 60;
    final int preexistingBytes = 10;
    ByteBuffer wrappedBuffer = ByteBuffer.allocate(1000);
    SocketChannel mockChannel = mock(SocketChannel.class);

    // force a compaction by making the decoded buffer appear near to being full
    ByteBuffer unwrappedBuffer = nioSslEngine.peerAppData;
    unwrappedBuffer.position(unwrappedBuffer.capacity() - individualRead);
    unwrappedBuffer.limit(unwrappedBuffer.position() + preexistingBytes);

    // simulate some socket reads
    when(mockChannel.read(any(ByteBuffer.class))).thenAnswer(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) throws Throwable {
        ByteBuffer buffer = invocation.getArgument(0);
        buffer.position(buffer.position() + individualRead);
        return individualRead;
      }
    });

    TestSSLEngine testSSLEngine = new TestSSLEngine();
    testSSLEngine.addReturnResult(new SSLEngineResult(OK, NEED_UNWRAP, 0, 0));
    nioSslEngine.engine = testSSLEngine;

    ByteBuffer data = nioSslEngine.readAtLeast(mockChannel, amountToRead, wrappedBuffer);
    verify(mockChannel, times(3)).read(isA(ByteBuffer.class));
    assertThat(data.position()).isEqualTo(0);
    assertThat(data.limit()).isEqualTo(individualRead * 3 + preexistingBytes);
  }


  /**
   * This tests the case where a message header has been read and part of a message has been
   * read, but the decoded buffer is too small to hold all of the message. In this case
   * the readAtLeast method will have to expand the capacity of the decoded buffer and return
   * the new, expanded, buffer as the method result.
   */
  @Test
  public void readAtLeastUsingSmallAppBuffer() throws Exception {
    final int amountToRead = 150;
    final int individualRead = 60;
    final int preexistingBytes = 10;
    ByteBuffer wrappedBuffer = ByteBuffer.allocate(1000);
    SocketChannel mockChannel = mock(SocketChannel.class);

    // force buffer expansion by making a small decoded buffer appear near to being full
    int initialUnwrappedBufferSize = 100;
    ByteBuffer unwrappedBuffer = ByteBuffer.allocate(initialUnwrappedBufferSize);
    unwrappedBuffer.position(7).limit(preexistingBytes + 7); // 7 bytes of message header - ignored
    nioSslEngine.peerAppData = unwrappedBuffer;

    // simulate some socket reads
    when(mockChannel.read(any(ByteBuffer.class))).thenAnswer(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) throws Throwable {
        ByteBuffer buffer = invocation.getArgument(0);
        buffer.position(buffer.position() + individualRead);
        return individualRead;
      }
    });

    TestSSLEngine testSSLEngine = new TestSSLEngine();
    testSSLEngine.addReturnResult(
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_UNWRAP, 0, 0), // expand 100 bytes to 93*2+7
        new SSLEngineResult(OK, NEED_UNWRAP, 0, 0), // 10 + 60 bytes = 70
        new SSLEngineResult(OK, NEED_UNWRAP, 0, 0), // 70 + 60 bytes = 130
        new SSLEngineResult(OK, NEED_UNWRAP, 0, 0)); // 130 + 60 bytes = 190
    nioSslEngine.engine = testSSLEngine;

    ByteBuffer data = nioSslEngine.readAtLeast(mockChannel, amountToRead, wrappedBuffer);
    verify(mockChannel, times(3)).read(isA(ByteBuffer.class));
    assertThat(data.position()).isEqualTo(0);
    assertThat(data.limit()).isEqualTo(individualRead * 3 + preexistingBytes);
    // The initial available space in the unwrapped buffer should have doubled
    int initialFreeSpace = initialUnwrappedBufferSize - preexistingBytes;
    assertThat(nioSslEngine.peerAppData.capacity())
        .isEqualTo(2 * initialFreeSpace + preexistingBytes);
  }


  /**
   * This tests the case where a message header has been read and part of a message has been
   * read, but the decoded buffer is too small to hold all of the message. In this case
   * the buffer is completely full and should only take one overflow response to resolve
   * the problem.
   */
  @Test
  public void readAtLeastUsingSmallAppBufferAtWriteLimit() throws Exception {
    final int amountToRead = 150;
    final int individualRead = 150;

    int initialUnwrappedBufferSize = 100;
    final int preexistingBytes = initialUnwrappedBufferSize - 7;
    ByteBuffer wrappedBuffer = ByteBuffer.allocate(1000);
    SocketChannel mockChannel = mock(SocketChannel.class);

    // force buffer expansion by making a small decoded buffer appear near to being full
    ByteBuffer unwrappedBuffer = ByteBuffer.allocate(initialUnwrappedBufferSize);
    unwrappedBuffer.position(7).limit(preexistingBytes + 7); // 7 bytes of message header - ignored
    nioSslEngine.peerAppData = unwrappedBuffer;

    // simulate some socket reads
    when(mockChannel.read(any(ByteBuffer.class))).thenAnswer(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) throws Throwable {
        ByteBuffer buffer = invocation.getArgument(0);
        buffer.position(buffer.position() + individualRead);
        return individualRead;
      }
    });

    TestSSLEngine testSSLEngine = new TestSSLEngine();
    testSSLEngine.addReturnResult(
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_UNWRAP, 0, 0),
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_UNWRAP, 0, 0),
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_UNWRAP, 0, 0),
        new SSLEngineResult(OK, NEED_UNWRAP, 0, 0));
    nioSslEngine.engine = testSSLEngine;

    ByteBuffer data = nioSslEngine.readAtLeast(mockChannel, amountToRead, wrappedBuffer);
    verify(mockChannel, times(1)).read(isA(ByteBuffer.class));
    assertThat(data.position()).isEqualTo(0);
    assertThat(data.limit())
        .isEqualTo(individualRead * testSSLEngine.getNumberOfUnwraps() + preexistingBytes);
  }


  // TestSSLEngine holds a stack of SSLEngineResults and always copies the
  // input buffer to the output buffer byte-for-byte in wrap() and unwrap() operations.
  // We use it in some tests where we need the byte-copying behavior because it's
  // pretty difficult & cumbersome to implement with Mockito.
  static class TestSSLEngine extends SSLEngine {

    private List<SSLEngineResult> returnResults = new ArrayList<>();

    private int numberOfUnwrapsPerformed;

    private SSLEngineResult nextResult() {
      SSLEngineResult result = returnResults.remove(0);
      if (returnResults.isEmpty()) {
        returnResults.add(result);
      }
      return result;
    }

    public int getNumberOfUnwraps() {
      return numberOfUnwrapsPerformed;
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer[] sources, int i, int i1, ByteBuffer destination) {
      for (ByteBuffer source : sources) {
        destination.put(source);
      }
      return nextResult();
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer source, ByteBuffer[] destinations, int i, int i1) {
      SSLEngineResult sslEngineResult = nextResult();
      if (sslEngineResult.getStatus() != BUFFER_UNDERFLOW
          && sslEngineResult.getStatus() != BUFFER_OVERFLOW) {
        destinations[0].put(source);
        numberOfUnwrapsPerformed++;
      }
      return sslEngineResult;
    }

    @Override
    public Runnable getDelegatedTask() {
      return null;
    }

    @Override
    public void closeInbound() {}

    @Override
    public boolean isInboundDone() {
      return false;
    }

    @Override
    public void closeOutbound() {}

    @Override
    public boolean isOutboundDone() {
      return false;
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return new String[0];
    }

    @Override
    public String[] getEnabledCipherSuites() {
      return new String[0];
    }

    @Override
    public void setEnabledCipherSuites(String[] strings) {}

    @Override
    public String[] getSupportedProtocols() {
      return new String[0];
    }

    @Override
    public String[] getEnabledProtocols() {
      return new String[0];
    }

    @Override
    public void setEnabledProtocols(String[] strings) {}

    @Override
    public SSLSession getSession() {
      return null;
    }

    @Override
    public void beginHandshake() {}

    @Override
    public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
      return null;
    }

    @Override
    public void setUseClientMode(boolean b) {}

    @Override
    public boolean getUseClientMode() {
      return false;
    }

    @Override
    public void setNeedClientAuth(boolean b) {}

    @Override
    public boolean getNeedClientAuth() {
      return false;
    }

    @Override
    public void setWantClientAuth(boolean b) {}

    @Override
    public boolean getWantClientAuth() {
      return false;
    }

    @Override
    public void setEnableSessionCreation(boolean b) {}

    @Override
    public boolean getEnableSessionCreation() {
      return false;
    }

    /**
     * add an engine operation result to be returned by wrap or unwrap.
     * Like Mockito's thenReturn(), the last return result will repeat forever
     */
    void addReturnResult(SSLEngineResult... sslEngineResult) {
      for (SSLEngineResult result : sslEngineResult) {
        returnResults.add(result);
      }
    }
  }
}
