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

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_TASK;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_OVERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.OK;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireIOException;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.internal.logging.LogService;


/**
 * NioSslEngine uses an SSLEngine to bind SSL logic to a data source
 */
public class NioSslEngine implements NioFilter {
  private static final Logger logger = LogService.getLogger();

  private final DMStats stats;

  private boolean closed;

  /**
   * Buffers allocated by NioSslEngine may be acquired from the Buffers pool
   * or they may be allocated using Buffer.allocate(). This enum is used
   * to note the different types. Tracked buffers come from the Buffers pool
   * and need to be released when we're done using them.
   */
  enum BufferType {
    UNTRACKED, TRACKED_SENDER, TRACKED_RECEIVER
  }

  private final SSLEngine engine;
  private final SocketChannel socketChannel;

  /**
   * myNetData holds bytes wrapped by the SSLEngine
   */
  private ByteBuffer myNetData;

  /**
   * peerAppData holds the last unwrapped data from a peer
   */
  private ByteBuffer peerAppData;

  public NioSslEngine(SocketChannel channel, SSLEngine engine,
      DMStats stats) {
    this.stats = stats;
    SSLSession session = engine.getSession();
    int appBufferSize = session.getApplicationBufferSize();
    int packetBufferSize = engine.getSession().getPacketBufferSize();
    // if (peerNetBuffer.isDirect()) {
    // this.myNetData = ByteBuffer.allocateDirect(packetBufferSize);
    // this.useDirectBuffers = true;
    // } else {
    this.myNetData = ByteBuffer.allocate(packetBufferSize);
    // }
    this.peerAppData = ByteBuffer.allocate(appBufferSize);
    this.engine = engine;
    this.socketChannel = channel;
  }

  /**
   * This will throw an SSLHandshakeException if the handshake doesn't terminate in
   * a FINISHED state. It may throw other IOExceptions caused by I/O operations
   */
  public synchronized NioSslEngine handshake(int timeout, ByteBuffer peerNetData)
      throws IOException {
    checkClosed();

    ByteBuffer myAppData = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
    peerNetData.clear();
    myAppData.clear();

    if (logger.isDebugEnabled()) {
      logger.debug("Starting TLS handshake with {}", socketChannel.socket());
    }

    long timeoutNanos = -1;
    if (timeout > 0) {
      timeoutNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
    }

    // Begin handshake
    engine.beginHandshake();
    SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();

    // Process handshaking message
    while (status != SSLEngineResult.HandshakeStatus.FINISHED &&
        status != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
      if (socketChannel.socket().isClosed()) {
        throw new SocketException("handshake terminated - socket is closed");
      }

      if (timeoutNanos > 0) {
        if (timeoutNanos < System.nanoTime()) {
          throw new SocketTimeoutException("handshake timed out");
        }
      }

      int dataRead = 0;

      switch (status) {
        case NEED_UNWRAP:
          // Receive handshaking data from peer
          if (dataRead >= 0) {
            dataRead = socketChannel.read(peerNetData);
          }

          // Process incoming handshaking data
          peerNetData.flip();
          SSLEngineResult engineResult = engine.unwrap(peerNetData, peerAppData);
          peerNetData.compact();
          status = engineResult.getHandshakeStatus();

          // if we're not finished, there's nothing to process and no data was read let's hang out
          // for a little
          if (peerAppData.remaining() == 0 && dataRead == 0 && status == NEED_UNWRAP) {
            LockSupport.parkNanos(100000L);
          }

          if (engineResult.getStatus() == BUFFER_OVERFLOW) {
            peerAppData =
                expandBuffer(BufferType.UNTRACKED, peerAppData, peerAppData.capacity() * 2);
          }
          break;

        case NEED_WRAP:
          // Empty the local network packet buffer.
          myNetData.clear();

          // Generate handshaking data
          engineResult = engine.wrap(myAppData, myNetData);
          status = engineResult.getHandshakeStatus();

          // Check status
          switch (engineResult.getStatus()) {
            case BUFFER_UNDERFLOW:
              break;
            case BUFFER_OVERFLOW:
              myNetData =
                  expandBuffer(BufferType.TRACKED_SENDER, myNetData, myNetData.capacity() * 2);
              break;
            case OK:
              myNetData.flip();
              // Send the handshaking data to peer
              while (myNetData.hasRemaining()) {
                socketChannel.write(myNetData);
              }
              break;
            case CLOSED:
              break;
            default:
              throw new IllegalStateException(
                  "Unknown SSLEngineResult status: " + engineResult.getStatus());
          }
          break;

        case NOT_HANDSHAKING:
          break;
        case FINISHED:
          break;
        case NEED_TASK:
          // Handle blocking tasks
          handleBlockingTasks();
          status = engine.getHandshakeStatus();
          break;
        default:
          throw new IllegalStateException("Unknown SSL Handshake state: " + status);
      }
      LockSupport.parkNanos(1000L);
    }
    if (!Objects.equals(SSLEngineResult.HandshakeStatus.FINISHED, status)) {
      throw new SSLHandshakeException("SSL Handshake terminated with status " + status);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("TLS handshake successfull");
    }
    return this;
  }

  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("NioSslEngine has been closed");
    }
  }

  private void handleBlockingTasks() {
    Runnable task;
    while ((task = engine.getDelegatedTask()) != null) {
      // these tasks could be run in other threads but the SSLEngine will block until they finish
      task.run();
    }
  }

  @Override
  public synchronized ByteBuffer wrap(ByteBuffer appData) throws IOException {
    checkClosed();

    myNetData.clear();

    while (appData.hasRemaining()) {
      // ensure we have lots of capacity since encrypted data might
      // be larger than the app data
      int remaining = myNetData.capacity() - myNetData.position();

      if (remaining < (appData.remaining() * 2)) {
        int newCapacity = expandedCapacity(appData, myNetData);
        myNetData = expandBuffer(BufferType.TRACKED_SENDER, myNetData, newCapacity);
      }

      SSLEngineResult wrapResult = engine.wrap(appData, myNetData);

      if (wrapResult.getHandshakeStatus() == NEED_TASK) {
        handleBlockingTasks();
      }

      if (wrapResult.getStatus() != OK) {
        throw new SSLException("Error encrypting data: " + wrapResult);
      }
    }

    myNetData.flip();

    return myNetData;
  }

  @Override
  public synchronized ByteBuffer unwrap(ByteBuffer wrappedBuffer) throws IOException {
    checkClosed();

    // note that we do not clear peerAppData as it may hold a partial
    // message. TcpConduit, for instance, uses message chunking to
    // transmit large payloads and we may have read a partial chunk
    // during the previous unwrap

    while (wrappedBuffer.hasRemaining()) {
      int remaining = peerAppData.capacity() - peerAppData.position();
      if (remaining < wrappedBuffer.remaining() * 2) {
        int newCapacity = expandedCapacity(peerAppData, wrappedBuffer);
        peerAppData = expandBuffer(BufferType.UNTRACKED, peerAppData, newCapacity);
      }
      SSLEngineResult unwrapResult = engine.unwrap(wrappedBuffer, peerAppData);
      if (unwrapResult.getHandshakeStatus() == NEED_TASK) {
        handleBlockingTasks();
      }

      if (unwrapResult.getStatus() != OK) {
        throw new SSLException("Error decrypting data: " + unwrapResult);
      }
    }
    wrappedBuffer.clear();
    peerAppData.flip();
    return peerAppData;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    engine.closeOutbound();
    try {
      ByteBuffer empty = ByteBuffer.allocate(0);

      while (!engine.isOutboundDone()) {
        // Get close message
        engine.wrap(empty, myNetData);

        // Send close message to peer
        while (myNetData.hasRemaining()) {
          socketChannel.write(myNetData);
          myNetData.compact();
        }
      }
    } catch (IOException e) {
      throw new GemFireIOException("exception closing SSL session", e);
    } finally {
      releaseBuffer(BufferType.TRACKED_SENDER, myNetData);
      releaseBuffer(BufferType.UNTRACKED, peerAppData);
      this.closed = true;
    }
  }

  private ByteBuffer acquireBuffer(BufferType type, int capacity) {
    switch (type) {
      case UNTRACKED:
        return ByteBuffer.allocate(capacity);
      case TRACKED_SENDER:
        return Buffers.acquireSenderBuffer(capacity, stats);
      case TRACKED_RECEIVER:
        return Buffers.acquireReceiveBuffer(capacity, stats);
    }
    throw new IllegalArgumentException("Unexpected buffer type " + type.toString());
  }

  private void releaseBuffer(BufferType type, ByteBuffer buffer) {
    switch (type) {
      case UNTRACKED:
        return;
      case TRACKED_SENDER:
        Buffers.releaseSenderBuffer(buffer, stats);
        return;
      case TRACKED_RECEIVER:
        Buffers.releaseReceiveBuffer(buffer, stats);
        return;
    }
    throw new IllegalArgumentException("Unexpected buffer type " + type.toString());
  }

  private int expandedCapacity(ByteBuffer sourceBuffer, ByteBuffer targetBuffer) {
    return Math.max(targetBuffer.position() + sourceBuffer.remaining() * 2,
        targetBuffer.capacity() * 2);
  }

  ByteBuffer expandBuffer(BufferType type, ByteBuffer existing, int desiredCapacity) {
    ByteBuffer newBuffer = acquireBuffer(type, desiredCapacity);
    newBuffer.clear();
    existing.flip();
    newBuffer.put(existing);
    releaseBuffer(type, existing);
    return newBuffer;
  }

}
