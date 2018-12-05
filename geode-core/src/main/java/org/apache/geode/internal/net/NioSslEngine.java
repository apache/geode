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

import java.io.IOException;
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

import org.apache.geode.GemFireIOException;


/**
 * NioSslEngine uses an SSLEngine to bind SSL logic to a data source
 */
public class NioSslEngine implements NioFilter {
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

  ByteBuffer myAppData;

  private boolean useDirectBuffers;

  public NioSslEngine(SocketChannel channel, SSLEngine engine, boolean useDirectBuffers) {
    SSLSession session = engine.getSession();
    int appBufferSize = session.getApplicationBufferSize();
    int packetBufferSize = engine.getSession().getPacketBufferSize();
    if (useDirectBuffers) {
      this.myNetData = ByteBuffer.allocateDirect(packetBufferSize);
      this.peerAppData = ByteBuffer.allocateDirect(appBufferSize);
      this.myAppData = ByteBuffer.allocateDirect(appBufferSize);
      this.useDirectBuffers = true;
    } else {
      this.myNetData = ByteBuffer.allocate(packetBufferSize);
      this.peerAppData = ByteBuffer.allocate(appBufferSize);
      this.myAppData = ByteBuffer.allocate(appBufferSize);
    }
    this.engine = engine;
    this.socketChannel = channel;
  }

  /**
   * This will throw an SSLHandshakeException if the handshake doesn't terminate in
   * a FINISHED state. It may throw other IOExceptions caused by I/O operations
   */
  public NioSslEngine handshake(int timeout) throws IOException {
    ByteBuffer peerNetData = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());

    long timeoutNanos = -1;
    if (timeout > 0) {
      timeoutNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
    }

    // Begin handshake
    engine.beginHandshake();
    SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();

    // Process handshaking message
    while (hs != SSLEngineResult.HandshakeStatus.FINISHED &&
        hs != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {

      if (timeoutNanos > 0) {
        if (timeoutNanos < System.nanoTime()) {
          throw new SocketTimeoutException("handshake timed out");
        }
      }

      switch (hs) {

        case NEED_UNWRAP:
          // Receive handshaking data from peer
          if (socketChannel.read(peerNetData) < 0) {
            // The channel has reached end-of-stream
          }

          // Process incoming handshaking data
          peerNetData.flip();
          SSLEngineResult res = engine.unwrap(peerNetData, peerAppData);
          peerNetData.compact();
          hs = res.getHandshakeStatus();

          // Check status
          switch (res.getStatus()) {
            case BUFFER_UNDERFLOW:
              break;
            case BUFFER_OVERFLOW:
              break;
            case OK:
              // Handle OK status
              break;
            case CLOSED:
              break;
            default:
              throw new IllegalStateException("Unknown SSLEngineResult status: " + res.getStatus());
          }
          break;

        case NEED_WRAP:
          // Empty the local network packet buffer.
          myNetData.clear();

          // Generate handshaking data
          res = engine.wrap(myAppData, myNetData);
          hs = res.getHandshakeStatus();

          // Check status
          switch (res.getStatus()) {
            case BUFFER_UNDERFLOW:
              break;
            case BUFFER_OVERFLOW:
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
              throw new IllegalStateException("Unknown SSLEngineResult status: " + res.getStatus());
          }
          break;

        case NOT_HANDSHAKING:
          break;
        case FINISHED:
          break;
        case NEED_TASK:
          // Handle blocking tasks
          handleBlockingTasks();
          hs = engine.getHandshakeStatus();
          break;
        default:
          throw new IllegalStateException("Unknown SSL Handshake state: " + hs);
      }
    }
    if (!Objects.equals(SSLEngineResult.HandshakeStatus.FINISHED, hs)) {
      throw new SSLHandshakeException("SSL Handshake terminated with status " + hs);
    }
    return this;
  }

  private void handleBlockingTasks() {
    Runnable task;
    while ((task = engine.getDelegatedTask()) != null) {
      // these tasks could be run in other threads but the SSLEngine will block until they finish
      task.run();
    }
  }

  @Override
  public ByteBuffer wrap(ByteBuffer appData) throws IOException {
    myNetData.clear();

    while (appData.hasRemaining()) {
      // ensure we have lots of capacity since encrypted data might
      // be larger than the app data
      int remaining = myNetData.capacity() - myNetData.position();

      if (remaining < (appData.remaining() * 2)) {
        int newCapacity = newBufferCapacity(appData, myNetData);
        myNetData = newBuffer(myNetData, newCapacity);
      }

      SSLEngineResult wrapResult = engine.wrap(appData, myNetData);

      if (wrapResult.getHandshakeStatus() == NEED_TASK) {
        handleBlockingTasks();
      }

      if (wrapResult.getStatus() != SSLEngineResult.Status.OK) {
        throw new SSLException("Error encrypting data: " + wrapResult);
      }
    }

    myNetData.flip();

    return myNetData;
  }

  @Override
  public ByteBuffer unwrap(ByteBuffer wrappedBuffer) throws IOException {
    // note that we do not clear peerAppData as it may hold a partial
    // message. TcpConduit, for instance, uses message chunking to
    // transmit large payloads and we may have read a partial chunk
    // during the previous unwrap

    while (wrappedBuffer.hasRemaining()) {
      int remaining = peerAppData.capacity() - peerAppData.position();
      if (remaining < wrappedBuffer.remaining() * 2) {
        int newCapacity = newBufferCapacity(peerAppData, wrappedBuffer);
        peerAppData = newBuffer(peerAppData, newCapacity);
      }
      SSLEngineResult unwrapResult = engine.unwrap(wrappedBuffer, peerAppData);
      if (unwrapResult.getHandshakeStatus() == NEED_TASK) {
        handleBlockingTasks();
      }

      if (unwrapResult.getStatus() != SSLEngineResult.Status.OK) {
        throw new SSLException("Error decrypting data: " + unwrapResult);
      }
    }
    wrappedBuffer.clear();
    peerAppData.flip();
    return peerAppData;
  }

  @Override
  public void close() {
    engine.closeOutbound();
    try {
      ByteBuffer empty = ByteBuffer.allocate(0);

      while (!engine.isOutboundDone()) {
        // Get close message
        SSLEngineResult res = engine.wrap(empty, myNetData);

        // Send close message to peer
        while (myNetData.hasRemaining()) {
          socketChannel.write(myNetData);
          myNetData.compact();
        }
      }
    } catch (IOException e) {
      throw new GemFireIOException("exception closing SSL session", e);
    }
  }

  private int newBufferCapacity(ByteBuffer sourceBuffer, ByteBuffer targetBuffer) {
    return Math.max(targetBuffer.position() + sourceBuffer.remaining() * 2,
        targetBuffer.capacity() * 2);
  }

  private ByteBuffer newBuffer(ByteBuffer existing, int desiredCapacity) {
    ByteBuffer newBuffer;
    if (useDirectBuffers) {
      newBuffer = ByteBuffer.allocateDirect(desiredCapacity);
    } else {
      newBuffer = ByteBuffer.allocate(desiredCapacity);
    }
    newBuffer.clear();
    existing.flip();
    newBuffer.put(existing);
    return newBuffer;
  }

}
