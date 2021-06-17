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
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_OVERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.OK;
import static org.apache.geode.internal.net.BufferPool.BufferType.TRACKED_RECEIVER;
import static org.apache.geode.internal.net.BufferPool.BufferType.TRACKED_SENDER;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireIOException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.net.BufferPool.BufferType;
import org.apache.geode.internal.net.ByteBufferVendor.OpenAttemptTimedOut;
import org.apache.geode.logging.internal.log4j.api.LogService;


/**
 * NioSslEngine uses an SSLEngine to bind SSL logic to a data source. This class is not thread safe.
 * Its use should be confined to one thread or should be protected by external synchronization.
 */
public class NioSslEngine implements NioFilter {
  private static final Logger logger = LogService.getLogger();

  private final BufferPool bufferPool;

  private boolean closed;

  SSLEngine engine;

  /**
   * holds bytes wrapped by the SSLEngine; a.k.a. myNetData
   */
  private final ByteBufferVendor outputSharing;

  /**
   * holds the last unwrapped data from a peer; a.k.a. peerAppData
   */
  private final ByteBufferVendor inputSharing;

  NioSslEngine(SSLEngine engine, BufferPool bufferPool) {
    SSLSession session = engine.getSession();
    int appBufferSize = session.getApplicationBufferSize();
    int packetBufferSize = engine.getSession().getPacketBufferSize();
    closed = false;
    this.engine = engine;
    this.bufferPool = bufferPool;
    outputSharing =
        new ByteBufferVendor(bufferPool.acquireDirectSenderBuffer(packetBufferSize),
            TRACKED_SENDER, bufferPool);
    inputSharing =
        new ByteBufferVendor(bufferPool.acquireNonDirectReceiveBuffer(appBufferSize),
            TRACKED_RECEIVER, bufferPool);
  }

  /**
   * This will throw an SSLHandshakeException if the handshake doesn't terminate in a FINISHED
   * state. It may throw other IOExceptions caused by I/O operations
   */
  public boolean handshake(SocketChannel socketChannel, int timeout,
      ByteBuffer peerNetData)
      throws IOException, InterruptedException {

    if (peerNetData.capacity() < engine.getSession().getPacketBufferSize()) {
      throw new IllegalArgumentException(String.format("Provided buffer is too small to perform "
          + "SSL handshake.  Buffer capacity is %s but need %s",
          peerNetData.capacity(), engine.getSession().getPacketBufferSize()));
    }

    ByteBuffer handshakeBuffer = peerNetData;
    handshakeBuffer.clear();

    ByteBuffer myAppData = ByteBuffer.wrap(new byte[0]);

    if (logger.isDebugEnabled()) {
      logger.debug("Starting TLS handshake with {}.  Timeout is {}ms", socketChannel.socket(),
          timeout);
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
    while (status != FINISHED &&
        status != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
      if (socketChannel.socket().isClosed()) {
        logger.info("Handshake terminated because socket is closed");
        throw new SocketException("handshake terminated - socket is closed");
      }

      if (timeoutNanos > 0) {
        if (timeoutNanos < System.nanoTime()) {
          logger.info("TLS handshake is timing out");
          throw new SocketTimeoutException("handshake timed out");
        }
      }

      switch (status) {
        case NEED_UNWRAP:
          try (final ByteBufferSharing inputSharing = shareInputBuffer()) {
            final ByteBuffer peerAppData = inputSharing.getBuffer();

            // Receive handshaking data from peer
            int dataRead = socketChannel.read(handshakeBuffer);

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
              inputSharing.expandWriteBufferIfNeeded(peerAppData.capacity() * 2);
            }
            break;
          }

        case NEED_WRAP:
          try (final ByteBufferSharing outputSharing = shareOutputBuffer()) {
            final ByteBuffer myNetData = outputSharing.getBuffer();

            // Empty the local network packet buffer.
            myNetData.clear();

            // Generate handshaking data
            engineResult = engine.wrap(myAppData, myNetData);
            status = engineResult.getHandshakeStatus();

            // Check status
            switch (engineResult.getStatus()) {
              case BUFFER_OVERFLOW:
                // no need to assign return value because we will never reference it
                outputSharing.expandWriteBufferIfNeeded(myNetData.capacity() * 2);
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
                logger.info("handshake terminated with illegal state due to {}", status);
                throw new IllegalStateException(
                    "Unknown SSLEngineResult status: " + engineResult.getStatus());
            }
            break;
          }
        case NEED_TASK:
          // Handle blocking tasks
          handleBlockingTasks();
          status = engine.getHandshakeStatus();
          break;
        default:
          logger.info("handshake terminated with illegal state due to {}", status);
          throw new IllegalStateException("Unknown SSL Handshake state: " + status);
      }
      Thread.sleep(10);
    }
    if (status != FINISHED) {
      logger.info("handshake terminated with exception due to {}", status);
      throw new SSLHandshakeException("SSL Handshake terminated with status " + status);
    }
    if (logger.isDebugEnabled()) {
      if (engineResult != null) {
        logger.debug("TLS handshake successful.  result={} and handshakeResult={}",
            engineResult.getStatus(), engine.getHandshakeStatus());
      } else {
        logger.debug("TLS handshake successful.  handshakeResult={}",
            engine.getHandshakeStatus());
      }
    }
    return true;
  }

  void handleBlockingTasks() {
    Runnable task;
    while ((task = engine.getDelegatedTask()) != null) {
      // these tasks could be run in other threads but the SSLEngine will block until they finish
      task.run();
    }
  }

  @Override
  public ByteBufferSharing wrap(ByteBuffer appData) throws IOException {
    try (final ByteBufferSharing outputSharing = shareOutputBuffer()) {

      ByteBuffer myNetData = outputSharing.getBuffer();

      myNetData.clear();

      while (appData.hasRemaining()) {
        // ensure we have lots of capacity since encrypted data might
        // be larger than the app data
        int remaining = myNetData.capacity() - myNetData.position();

        if (remaining < (appData.remaining() * 2)) {
          int newCapacity = expandedCapacity(appData, myNetData);
          myNetData = outputSharing.expandWriteBufferIfNeeded(newCapacity);
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

      return shareOutputBuffer();
    }
  }

  @Override
  public ByteBufferSharing unwrap(ByteBuffer wrappedBuffer) throws IOException {
    try (final ByteBufferSharing inputSharing = shareInputBuffer()) {

      ByteBuffer peerAppData = inputSharing.getBuffer();

      // note that we do not clear peerAppData as it may hold a partial
      // message. TcpConduit, for instance, uses message chunking to
      // transmit large payloads and we may have read a partial chunk
      // during the previous unwrap

      peerAppData.limit(peerAppData.capacity());
      boolean stopDecryption = false;
      while (wrappedBuffer.hasRemaining() && !stopDecryption) {
        SSLEngineResult unwrapResult = engine.unwrap(wrappedBuffer, peerAppData);
        switch (unwrapResult.getStatus()) {
          case BUFFER_OVERFLOW:
            // buffer overflow expand and try again - double the available decryption space
            int newCapacity =
                (peerAppData.capacity() - peerAppData.position()) * 2 + peerAppData.position();
            newCapacity = Math.max(newCapacity, peerAppData.capacity() / 2 * 3);
            peerAppData = inputSharing.expandWriteBufferIfNeeded(newCapacity);
            peerAppData.limit(peerAppData.capacity());
            break;
          case BUFFER_UNDERFLOW:
            // partial data - need to read more. When this happens the SSLEngine will not have
            // changed the buffer position
            wrappedBuffer.compact();
            return shareInputBuffer();
          case OK:
            break;
          default:// if there is data in the decrypted buffer return it. Otherwise signal that we're
            // having trouble
            if (peerAppData.position() <= 0) {
              throw new SSLException("Error decrypting data: " + unwrapResult);
            }
            stopDecryption = true;
            break;
        }
      }
      wrappedBuffer.clear();
      return shareInputBuffer();
    }
  }

  @Override
  public ByteBuffer ensureWrappedCapacity(int amount, ByteBuffer wrappedBuffer,
      BufferType bufferType) {
    ByteBuffer buffer = wrappedBuffer;
    int requiredSize = engine.getSession().getPacketBufferSize();
    if (buffer == null) {
      buffer = bufferPool.acquireDirectBuffer(bufferType, requiredSize);
    } else if (buffer.capacity() < requiredSize) {
      buffer = bufferPool.expandWriteBufferIfNeeded(bufferType, buffer, requiredSize);
    }
    return buffer;
  }

  @Override
  public ByteBufferSharing readAtLeast(SocketChannel channel, int bytes,
      ByteBuffer wrappedBuffer) throws IOException {
    try (final ByteBufferSharing inputSharing = shareInputBuffer()) {

      ByteBuffer peerAppData = inputSharing.getBuffer();

      if (peerAppData.capacity() > bytes) {
        // we already have a buffer that's big enough
        if (peerAppData.capacity() - peerAppData.position() < bytes) {
          peerAppData.compact();
          peerAppData.flip();
        }
      }

      while (peerAppData.remaining() < bytes) {
        wrappedBuffer.limit(wrappedBuffer.capacity());
        int amountRead = channel.read(wrappedBuffer);
        if (amountRead < 0) {
          throw new EOFException();
        }
        if (amountRead > 0) {
          wrappedBuffer.flip();
          // prep the decoded buffer for writing
          peerAppData.compact();
          try (final ByteBufferSharing inputSharing2 = unwrap(wrappedBuffer)) {
            // done writing to the decoded buffer - prep it for reading again
            final ByteBuffer peerAppDataNew = inputSharing2.getBuffer();
            peerAppDataNew.flip();
            peerAppData = peerAppDataNew; // loop needs new reference!
          }
        }
      }
      return shareInputBuffer();
    }
  }

  @Override
  public ByteBufferSharing getUnwrappedBuffer() throws IOException {
    return shareInputBuffer();
  }

  @Override
  public void doneReadingDirectAck(ByteBuffer unwrappedBuffer) {
    // nothing needs to be done - the next direct-ack message will be
    // read into the same buffer and compaction will be done during
    // read-operations
  }

  @Override
  public synchronized void close(SocketChannel socketChannel) {
    if (closed) {
      return;
    }
    closed = true;
    inputSharing.destruct();
    try (final ByteBufferSharing outputSharing = shareOutputBuffer(1, TimeUnit.MINUTES)) {
      final ByteBuffer myNetData = outputSharing.getBuffer();

      if (!engine.isOutboundDone()) {
        ByteBuffer empty = ByteBuffer.wrap(new byte[0]);
        engine.closeOutbound();

        // clear the buffer to receive a CLOSE message from the SSLEngine
        myNetData.clear();

        // Get close message
        SSLEngineResult result = engine.wrap(empty, myNetData);

        if (result.getStatus() != SSLEngineResult.Status.CLOSED) {
          throw new SSLHandshakeException(
              "Error closing SSL session.  Status=" + result.getStatus());
        }

        // Send close message to peer
        myNetData.flip();
        while (myNetData.hasRemaining()) {
          socketChannel.write(myNetData);
        }
      }
    } catch (ClosedChannelException e) {
      // we can't send a close message if the channel is closed
    } catch (IOException e) {
      throw new GemFireIOException("exception closing SSL session", e);
    } catch (final OpenAttemptTimedOut _unused) {
      logger.info(String.format("Couldn't get output lock in time, eliding TLS close message"));
      if (!engine.isOutboundDone()) {
        engine.closeOutbound();
      }
    } finally {
      outputSharing.destruct();
    }
  }

  private int expandedCapacity(ByteBuffer sourceBuffer, ByteBuffer targetBuffer) {
    return Math.max(targetBuffer.position() + sourceBuffer.remaining() * 2,
        targetBuffer.capacity() * 2);
  }

  @VisibleForTesting
  public ByteBufferSharing shareOutputBuffer() throws IOException {
    return outputSharing.open();
  }

  private ByteBufferSharing shareOutputBuffer(final long time, final TimeUnit unit)
      throws OpenAttemptTimedOut, IOException {
    return outputSharing.open(time, unit);
  }

  public ByteBufferSharing shareInputBuffer() throws IOException {
    return inputSharing.open();
  }
}
