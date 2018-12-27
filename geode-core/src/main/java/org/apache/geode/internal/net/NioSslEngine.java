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
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.OK;
import static org.apache.geode.internal.net.Buffers.expandBuffer;
import static org.apache.geode.internal.net.Buffers.releaseBuffer;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

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

  /**
   * buffer used to receive data during TLS handshake
   */
  private ByteBuffer handshakeBuffer;

  public NioSslEngine(SocketChannel channel, SSLEngine engine,
      DMStats stats) {
    this.stats = stats;
    SSLSession session = engine.getSession();
    int appBufferSize = session.getApplicationBufferSize();
    int packetBufferSize = engine.getSession().getPacketBufferSize();
    this.myNetData = ByteBuffer.allocate(packetBufferSize);
    this.peerAppData = ByteBuffer.allocate(appBufferSize);
    this.engine = engine;
    this.socketChannel = channel;
  }

  /**
   * This will throw an SSLHandshakeException if the handshake doesn't terminate in a FINISHED
   * state. It may throw other IOExceptions caused by I/O operations
   */
  public synchronized NioSslEngine handshake(int timeout, ByteBuffer peerNetData)
      throws IOException, InterruptedException {

    peerNetData.clear();
    if (peerNetData.capacity() < engine.getSession().getPacketBufferSize()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Allocating new buffer for SSL handshake");
      }
      this.handshakeBuffer =
          Buffers.acquireReceiveBuffer(engine.getSession().getPacketBufferSize(), stats);
    } else {
      this.handshakeBuffer = peerNetData;
    }

    ByteBuffer myAppData = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
    handshakeBuffer.clear();
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
    SSLEngineResult engineResult = null;

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
            dataRead = socketChannel.read(handshakeBuffer);
          }

          // Process incoming handshaking data
          handshakeBuffer.flip();
          engineResult = engine.unwrap(handshakeBuffer, peerAppData);
          handshakeBuffer.compact();
          status = engineResult.getHandshakeStatus();

          // if we're not finished, there's nothing to process and no data was read let's hang out
          // for a little
          if (peerAppData.remaining() == 0 && dataRead == 0 && status == NEED_UNWRAP) {
            Thread.sleep(10);
          }

          if (engineResult.getStatus() == BUFFER_OVERFLOW) {
            peerAppData =
                expandBuffer(Buffers.BufferType.UNTRACKED, peerAppData, peerAppData.capacity() * 2,
                    stats);
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
                  expandBuffer(Buffers.BufferType.TRACKED_SENDER, myNetData,
                      myNetData.capacity() * 2, stats);
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
        case NEED_TASK:
          // Handle blocking tasks
          handleBlockingTasks();
          status = engine.getHandshakeStatus();
          break;
        default:
          throw new IllegalStateException("Unknown SSL Handshake state: " + status);
      }
      Thread.sleep(10);
    }
    if (!Objects.equals(SSLEngineResult.HandshakeStatus.FINISHED, status)) {
      throw new SSLHandshakeException("SSL Handshake terminated with status " + status);
    }
    if (logger.isDebugEnabled()) {
      if (engineResult != null) {
        logger.debug("TLS handshake successfull.  result={} and handshakeResult={}",
            engineResult.getStatus(), engine.getHandshakeStatus());
      } else {
        logger.debug("TLS handshake successfull.  handshakeResult={}",
            engine.getHandshakeStatus());
      }
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
        myNetData = expandBuffer(Buffers.BufferType.TRACKED_SENDER, myNetData, newCapacity, stats);
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
        peerAppData = expandBuffer(Buffers.BufferType.UNTRACKED, peerAppData, newCapacity, stats);
      }
      SSLEngineResult unwrapResult = engine.unwrap(wrappedBuffer, peerAppData);
      if (unwrapResult.getStatus() == BUFFER_UNDERFLOW) {
        // partial data - need to read more. When this happens the SSLEngine will not have
        // changed the buffer position
        if (wrappedBuffer.position() == wrappedBuffer.limit()) {
          wrappedBuffer.clear();
        } else {
          wrappedBuffer.compact();
        }
        return peerAppData;
      }

      if (unwrapResult.getStatus() != OK) {
        throw new SSLException("Error decrypting data: " + unwrapResult);
      }
    }
    wrappedBuffer.clear();
    return peerAppData;
  }

  @Override
  public ByteBuffer getUnwrappedBuffer(ByteBuffer wrappedBuffer) {
    return peerAppData;
  }

  @Override
  public ByteBuffer ensureUnwrappedCapacity(int amount, ByteBuffer wrappedBuffer,
      Buffers.BufferType bufferType,
      DMStats stats) {
    peerAppData = Buffers.expandBuffer(bufferType, peerAppData, amount, this.stats);
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
      releaseBuffer(Buffers.BufferType.TRACKED_SENDER, myNetData, stats);
      releaseBuffer(Buffers.BufferType.UNTRACKED, peerAppData, stats);
      this.closed = true;
    }
  }

  private int expandedCapacity(ByteBuffer sourceBuffer, ByteBuffer targetBuffer) {
    return Math.max(targetBuffer.position() + sourceBuffer.remaining() * 2,
        targetBuffer.capacity() * 2);
  }

}
