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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.internal.Assert;

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
   * A list of soft references to byte buffers.
   */
  private final ConcurrentLinkedDeque<ByteBufferSoftReference> bufferQueue =
      new ConcurrentLinkedDeque<>();

  /**
   * use direct ByteBuffers instead of heap ByteBuffers for NIO operations
   */
  public static final boolean useDirectBuffers = !Boolean.getBoolean("p2p.nodirectBuffers");

  /**
   * Should only be called by threads that have currently acquired send permission.
   *
   * @return a byte buffer to be used for sending on this connection.
   */
  public ByteBuffer acquireSenderBuffer(int size) {
    return acquireBuffer(size, true);
  }

  public ByteBuffer acquireReceiveBuffer(int size) {
    return acquireBuffer(size, false);
  }

  private ByteBuffer acquireBuffer(int size, boolean send) {
    ByteBuffer result;
    if (useDirectBuffers) {
      for(ByteBufferSoftReference ref : bufferQueue) {
        bufferQueue.remove(ref);

        if(garbageCollected(ref)){
          removeFromQueuePermanently(ref);
        }
        else if(bigEnough(ref, size)){
          return borrowFromQueue(ref, size);
        }
        else {
          bufferQueue.offerFirst(ref);
        }
      }
      result = ByteBuffer.allocateDirect(size);
    } else {
      // if we are using heap buffers then don't bother with keeping them around
      result = ByteBuffer.allocate(size);
    }
    if (send) {
      stats.incSenderBufferSize(size, useDirectBuffers);
    } else {
      stats.incReceiverBufferSize(size, useDirectBuffers);
    }
    return result;
  }

  private ByteBuffer borrowFromQueue(ByteBufferSoftReference softReference, int size) {
    ByteBuffer buffer = softReference.getByteBuffer();
    buffer.rewind();
    buffer.limit(size);

    return buffer;
  }

  private boolean bigEnough(ByteBufferSoftReference softReference, int size) {
    return softReference.getByteBuffer().capacity() >= size;
  }

  private void removeFromQueuePermanently(ByteBufferSoftReference softReference) {
    int refSize = softReference.consumeSize();
    if(refSize > 0) {
      if (softReference.getSend()) {
        stats.incSenderBufferSize(-refSize, true);
      } else {
        stats.incReceiverBufferSize(-refSize, true);
      }
    }
  }

  private boolean garbageCollected(ByteBufferSoftReference softReference) {
    return softReference.getByteBuffer() == null;
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
    ByteBuffer newBuffer = acquireBuffer(type, desiredCapacity);
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
    ByteBuffer newBuffer = acquireBuffer(type, desiredCapacity);
    newBuffer.clear();
    existing.flip();
    newBuffer.put(existing);
    releaseBuffer(type, existing);
    return newBuffer;
  }

  ByteBuffer acquireBuffer(BufferPool.BufferType type, int capacity) {
    switch (type) {
      case UNTRACKED:
        return ByteBuffer.allocate(capacity);
      case TRACKED_SENDER:
        return acquireSenderBuffer(capacity);
      case TRACKED_RECEIVER:
        return acquireReceiveBuffer(capacity);
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
  private void releaseBuffer(ByteBuffer bb, boolean send) {
    if (useDirectBuffers) {
      ByteBufferSoftReference bbRef = new ByteBufferSoftReference(bb, send);
      bufferQueue.offer(bbRef);
    } else {
      if (send) {
        stats.incSenderBufferSize(-bb.capacity(), false);
      } else {
        stats.incReceiverBufferSize(-bb.capacity(), false);
      }
    }
  }

  /**
   * A soft reference that remembers the size of the byte buffer it refers to. TODO Dan - I really
   * think this should be a weak reference. The JVM doesn't seem to clear soft references if it is
   * getting low on direct memory.
   */
  private static class ByteBufferSoftReference extends SoftReference<ByteBuffer> {
    private int size;
    private final boolean send;

    ByteBufferSoftReference(ByteBuffer bb, boolean send) {
      super(bb);
      this.size = bb.capacity();
      this.send = send;
    }

    public int getSize() {
      return this.size;
    }

    synchronized int consumeSize() {
      int result = this.size;
      this.size = 0;
      return result;
    }

    public boolean getSend() {
      return this.send;
    }

    public ByteBuffer getByteBuffer() {
      return super.get();
    }
  }

}
