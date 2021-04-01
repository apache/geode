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

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.tcp.Connection;
import org.apache.geode.unsafe.internal.sun.nio.ch.DirectBuffer;
import org.apache.geode.util.internal.GeodeGlossary;

public class BufferPool {
  private final DMStats stats;

  /**
   * Buffers may be acquired from the Buffers pool
   * or they may be allocated using Buffer.allocate(). This enum is used
   * to note the different types. Tracked buffers come from the Buffers pool
   * and need to be released when we're done using them.
   */
  public enum BufferType {
    UNTRACKED, TRACKED_SENDER, TRACKED_RECEIVER
  }


  public BufferPool(DMStats stats) {
    this.stats = stats;
  }

  /**
   * A list of soft references to small byte buffers.
   */
  private final ConcurrentLinkedQueue<BBSoftReference> bufferSmallQueue =
      new ConcurrentLinkedQueue<>();

  /**
   * A list of soft references to middle byte buffers.
   */
  private final ConcurrentLinkedQueue<BBSoftReference> bufferMiddleQueue =
      new ConcurrentLinkedQueue<>();

  /**
   * A list of soft references to large byte buffers.
   */
  private final ConcurrentLinkedQueue<BBSoftReference> bufferLargeQueue =
      new ConcurrentLinkedQueue<>();

  static final int SMALL_BUFFER_SIZE = Connection.SMALL_BUFFER_SIZE;


  static final int MEDIUM_BUFFER_SIZE = DistributionConfig.DEFAULT_SOCKET_BUFFER_SIZE;


  /**
   * use direct ByteBuffers instead of heap ByteBuffers for NIO operations
   */
  public static final boolean useDirectBuffers = !Boolean.getBoolean("p2p.nodirectBuffers")
      || Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "BufferPool.useHeapBuffers");

  /**
   * Should only be called by threads that have currently acquired send permission.
   *
   * @return a byte buffer to be used for sending on this connection.
   */
  public ByteBuffer acquireDirectSenderBuffer(int size) {
    return acquireDirectBuffer(size, true);
  }

  public ByteBuffer acquireDirectReceiveBuffer(int size) {
    return acquireDirectBuffer(size, false);
  }

  /**
   * try to acquire direct buffer, if enabled by configuration
   */
  private ByteBuffer acquireDirectBuffer(int size, boolean send) {
    ByteBuffer result;

    if (useDirectBuffers) {
      if (size <= MEDIUM_BUFFER_SIZE) {
        result = acquirePredefinedFixedBuffer(send, size);
      } else {
        result = acquireLargeBuffer(send, size);
      }
      if (result.capacity() > size) {
        result.position(0).limit(size);
        result = result.slice();
      }
      return result;
    }
    // if we are using heap buffers then don't bother with keeping them around
    result = ByteBuffer.allocate(size);
    updateBufferStats(size, send, false);
    return result;
  }

  public ByteBuffer acquireNonDirectSenderBuffer(int size) {
    ByteBuffer result = ByteBuffer.allocate(size);
    stats.incSenderBufferSize(size, false);
    return result;
  }

  public ByteBuffer acquireNonDirectReceiveBuffer(int size) {
    ByteBuffer result = ByteBuffer.allocate(size);
    stats.incReceiverBufferSize(size, false);
    return result;
  }

  /**
   * Acquire direct buffer with predefined default capacity (SMALL_BUFFER_SIZE or
   * MEDIUM_BUFFER_SIZE)
   */
  private ByteBuffer acquirePredefinedFixedBuffer(boolean send, int size) {
    // set
    int defaultSize;
    ConcurrentLinkedQueue<BBSoftReference> bufferTempQueue;
    ByteBuffer result;

    if (size <= SMALL_BUFFER_SIZE) {
      defaultSize = SMALL_BUFFER_SIZE;
      bufferTempQueue = bufferSmallQueue;
    } else {
      defaultSize = MEDIUM_BUFFER_SIZE;
      bufferTempQueue = bufferMiddleQueue;
    }

    BBSoftReference ref = bufferTempQueue.poll();
    while (ref != null) {
      ByteBuffer bb = ref.getBB();
      if (bb == null) {
        // it was garbage collected
        updateBufferStats(-defaultSize, ref.getSend(), true);
      } else {
        bb.clear();
        if (defaultSize > size) {
          bb.limit(size);
        }
        return bb;
      }
      ref = bufferTempQueue.poll();
    }
    result = ByteBuffer.allocateDirect(defaultSize);
    updateBufferStats(defaultSize, send, true);
    if (defaultSize > size) {
      result.limit(size);
    }
    return result;
  }

  private ByteBuffer acquireLargeBuffer(boolean send, int size) {
    // set
    ByteBuffer result;
    IdentityHashMap<BBSoftReference, BBSoftReference> alreadySeen = null; // keys are used like a
    // set
    BBSoftReference ref = bufferLargeQueue.poll();
    while (ref != null) {
      ByteBuffer bb = ref.getBB();
      if (bb == null) {
        // it was garbage collected
        int refSize = ref.consumeSize();
        if (refSize > 0) {
          updateBufferStats(-refSize, ref.getSend(), true);
        }
      } else if (bb.capacity() >= size) {
        bb.clear();
        if (bb.capacity() > size) {
          bb.limit(size);
        }
        return bb;
      } else {
        // wasn't big enough so put it back in the queue
        Assert.assertTrue(bufferLargeQueue.offer(ref));
        if (alreadySeen == null) {
          alreadySeen = new IdentityHashMap<>();
        }
        if (alreadySeen.put(ref, ref) != null) {
          break;
        }
      }
      ref = bufferLargeQueue.poll();
    }
    result = ByteBuffer.allocateDirect(size);
    updateBufferStats(size, send, true);
    return result;
  }

  public void releaseSenderBuffer(ByteBuffer bb) {
    releaseBuffer(bb, true);
  }

  public void releaseReceiveBuffer(ByteBuffer bb) {
    releaseBuffer(bb, false);
  }

  /**
   * expand a buffer that's currently being read from
   */
  ByteBuffer expandReadBufferIfNeeded(BufferType type, ByteBuffer existing,
      int desiredCapacity) {
    if (existing.capacity() >= desiredCapacity) {
      if (existing.position() > 0) {
        existing.compact();
        existing.flip();
      }
      return existing;
    }
    ByteBuffer newBuffer;
    if (existing.isDirect()) {
      newBuffer = acquireDirectBuffer(type, desiredCapacity);
    } else {
      newBuffer = acquireNonDirectBuffer(type, desiredCapacity);
    }
    newBuffer.clear();
    newBuffer.put(existing);
    newBuffer.flip();
    releaseBuffer(type, existing);
    return newBuffer;
  }

  /**
   * expand a buffer that's currently being written to
   */
  ByteBuffer expandWriteBufferIfNeeded(BufferType type, ByteBuffer existing,
      int desiredCapacity) {
    if (existing.capacity() >= desiredCapacity) {
      return existing;
    }
    ByteBuffer newBuffer;
    if (existing.isDirect()) {
      newBuffer = acquireDirectBuffer(type, desiredCapacity);
    } else {
      newBuffer = acquireNonDirectBuffer(type, desiredCapacity);
    }
    newBuffer.clear();
    existing.flip();
    newBuffer.put(existing);
    releaseBuffer(type, existing);
    return newBuffer;
  }

  ByteBuffer acquireDirectBuffer(BufferPool.BufferType type, int capacity) {
    switch (type) {
      case UNTRACKED:
        return ByteBuffer.allocate(capacity);
      case TRACKED_SENDER:
        return acquireDirectSenderBuffer(capacity);
      case TRACKED_RECEIVER:
        return acquireDirectReceiveBuffer(capacity);
    }
    throw new IllegalArgumentException("Unexpected buffer type " + type.toString());
  }

  ByteBuffer acquireNonDirectBuffer(BufferPool.BufferType type, int capacity) {
    switch (type) {
      case UNTRACKED:
        return ByteBuffer.allocate(capacity);
      case TRACKED_SENDER:
        return acquireNonDirectSenderBuffer(capacity);
      case TRACKED_RECEIVER:
        return acquireNonDirectReceiveBuffer(capacity);
    }
    throw new IllegalArgumentException("Unexpected buffer type " + type.toString());
  }

  void releaseBuffer(BufferPool.BufferType type, ByteBuffer buffer) {
    switch (type) {
      case UNTRACKED:
        return;
      case TRACKED_SENDER:
        releaseSenderBuffer(buffer);
        return;
      case TRACKED_RECEIVER:
        releaseReceiveBuffer(buffer);
        return;
    }
    throw new IllegalArgumentException("Unexpected buffer type " + type.toString());
  }


  /**
   * Releases a previously acquired buffer.
   */
  private void releaseBuffer(ByteBuffer buffer, boolean send) {
    if (buffer.isDirect()) {
      buffer = getPoolableBuffer(buffer);
      BBSoftReference bbRef = new BBSoftReference(buffer, send);
      if (buffer.capacity() <= SMALL_BUFFER_SIZE) {
        bufferSmallQueue.offer(bbRef);
      } else if (buffer.capacity() <= MEDIUM_BUFFER_SIZE) {
        bufferMiddleQueue.offer(bbRef);
      } else {
        bufferLargeQueue.offer(bbRef);
      }
    } else {
      updateBufferStats(-buffer.capacity(), send, false);
    }
  }

  /**
   * If we hand out a buffer that is larger than the requested size we create a
   * "slice" of the buffer having the requested capacity and hand that out instead.
   * When we put the buffer back in the pool we need to find the original, non-sliced,
   * buffer. This is held in DirectBuffer in its "attachment" field.
   *
   * This method is visible for use in debugging and testing. For debugging, invoke this method if
   * you need to see the non-sliced buffer for some reason, such as logging its hashcode.
   */
  @VisibleForTesting
  ByteBuffer getPoolableBuffer(final ByteBuffer buffer) {
    final Object attachment = DirectBuffer.attachment(buffer);

    if (null == attachment) {
      return buffer;
    }

    if (attachment instanceof ByteBuffer) {
      return (ByteBuffer) attachment;
    }

    throw new InternalGemFireException("direct byte buffer attachment was not a byte buffer but a "
        + attachment.getClass().getName());
  }

  /**
   * Update buffer stats.
   */
  private void updateBufferStats(int size, boolean send, boolean direct) {
    if (send) {
      stats.incSenderBufferSize(size, direct);
    } else {
      stats.incReceiverBufferSize(size, direct);
    }
  }

  /**
   * A soft reference that remembers the size of the byte buffer it refers to. TODO Dan - I really
   * think this should be a weak reference. The JVM doesn't seem to clear soft references if it is
   * getting low on direct memory.
   */
  private static class BBSoftReference extends SoftReference<ByteBuffer> {
    private int size;
    private final boolean send;

    BBSoftReference(ByteBuffer bb, boolean send) {
      super(bb);
      size = bb.capacity();
      this.send = send;
    }

    public int getSize() {
      return size;
    }

    synchronized int consumeSize() {
      int result = size;
      size = 0;
      return result;
    }

    public boolean getSend() {
      return send;
    }

    public ByteBuffer getBB() {
      return super.get();
    }
  }

}
