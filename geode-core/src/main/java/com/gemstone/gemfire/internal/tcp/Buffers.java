/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.tcp;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.internal.Assert;

/**
 *
 */
public class Buffers {
  /**
   * A list of soft references to byte buffers.
   */
  private static final ConcurrentLinkedQueue bufferQueue = new ConcurrentLinkedQueue();
  
  /**
   * Should only be called by threads that have currently acquired send permission.
   * @return a byte buffer to be used for sending on this connection.
   */
  static ByteBuffer acquireSenderBuffer(int size, DMStats stats) {
    return acquireBuffer(size, stats, true);
  }
  
  static ByteBuffer acquireReceiveBuffer(int size, DMStats stats) {
    return acquireBuffer(size, stats, false);
  }

  static ByteBuffer acquireBuffer(int size, DMStats stats, boolean send) {
    ByteBuffer result;
    if (TCPConduit.useDirectBuffers) {
      IdentityHashMap<BBSoftReference, BBSoftReference> alreadySeen = null; // keys are used like a set
      BBSoftReference ref = (BBSoftReference)bufferQueue.poll();
      while (ref != null) {
        ByteBuffer bb = ref.getBB();
        if (bb == null) {
          // it was garbage collected
          int refSize = ref.consumeSize();
          if (refSize > 0) {
            if (ref.getSend()) { // fix bug 46773
              stats.incSenderBufferSize(-refSize, true);
            } else {
              stats.incReceiverBufferSize(-refSize, true);
            }
          }
        } else if (bb.capacity() >= size) {
          bb.rewind();
          bb.limit(size);
          return bb;
        } else {
          // wasn't big enough so put it back in the queue
          Assert.assertTrue(bufferQueue.offer(ref));
          if (alreadySeen == null) {
            alreadySeen = new IdentityHashMap<BBSoftReference, BBSoftReference>();
          }
          if (alreadySeen.put(ref, ref) != null) {
            // if it returns non-null then we have already seen this item
            // so we have worked all the way through the queue once.
            // So it is time to give up and allocate a new buffer.
            break;
          }
        }
        ref = (BBSoftReference)bufferQueue.poll();
      }
      result = ByteBuffer.allocateDirect(size);
    } else {
      // if we are using heap buffers then don't bother with keeping them around
      result = ByteBuffer.allocate(size);
    }
    if(send) {
      stats.incSenderBufferSize(size, TCPConduit.useDirectBuffers);
    } else {
      stats.incReceiverBufferSize(size, TCPConduit.useDirectBuffers);
    }
    return result;
  }
  
  static void releaseSenderBuffer(ByteBuffer bb, DMStats stats) {
    releaseBuffer(bb, stats, true);
  }
  
  static void releaseReceiveBuffer(ByteBuffer bb, DMStats stats) {
    releaseBuffer(bb, stats, false);
  }
  
  /**
   * Releases a previously acquired buffer.
   */
  static void releaseBuffer(ByteBuffer bb, DMStats stats, boolean send) {
    if (TCPConduit.useDirectBuffers) {
      BBSoftReference bbRef = new BBSoftReference(bb, send);
      bufferQueue.offer(bbRef);
    } else {
      if(send) {
        stats.incSenderBufferSize(-bb.capacity(), false);
      } else {
        stats.incReceiverBufferSize(-bb.capacity(), false);
      }
    }
  }
  
  public static void initBufferStats(DMStats stats) { // fixes 46773
    if (TCPConduit.useDirectBuffers) {
      @SuppressWarnings("unchecked")
      Iterator<BBSoftReference> it = (Iterator<BBSoftReference>)bufferQueue.iterator();
      while (it.hasNext()) {
        BBSoftReference ref = it.next();
        if (ref.getBB() != null) {
          if(ref.getSend()) { // fix bug 46773
            stats.incSenderBufferSize(ref.getSize(), true);
          } else {
            stats.incReceiverBufferSize(ref.getSize(), true);
          }
        }
      }
    }
  }

  /**
   * A soft reference that remembers the size of the byte buffer it refers to.
   * TODO Dan - I really think this should be a weak reference. The JVM
   * doesn't seem to clear soft references if it is getting low on direct
   * memory.
   */
  private static class BBSoftReference extends SoftReference<ByteBuffer> {
    private int size;
    private final boolean send;
    public BBSoftReference(ByteBuffer bb, boolean send) {
      super(bb);
      this.size = bb.capacity();
      this.send = send;
    }
    public int getSize() {
      return this.size;
    }
    public synchronized int consumeSize() {
      int result = this.size;
      this.size = 0;
      return result;
    }
    public boolean getSend() {
      return this.send;
    }
    public ByteBuffer getBB() {
      return (ByteBuffer)super.get();
    }
  }

}
