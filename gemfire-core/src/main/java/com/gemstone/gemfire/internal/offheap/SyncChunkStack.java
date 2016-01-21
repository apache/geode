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
package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.LogWriter;

/**
 * A "stack" of "chunk" instances. The chunks are not kept
 * in java object form but instead each "chunk" is just an
 * off-heap address.
 * This class is used for each "tiny" free-list of the off-heap memory allocator.
 */
public class SyncChunkStack {
  // Ok to read without sync but must be synced on write
  private volatile long topAddr;
  
  public SyncChunkStack(long addr) {
    if (addr != 0L) SimpleMemoryAllocatorImpl.validateAddress(addr);
    this.topAddr = addr;
  }
  public SyncChunkStack() {
    this.topAddr = 0L;
  }
  public boolean isEmpty() {
    return this.topAddr == 0L;
  }
  public void offer(long e) {
    assert e != 0;
    SimpleMemoryAllocatorImpl.validateAddress(e);
    synchronized (this) {
      Chunk.setNext(e, this.topAddr);
      this.topAddr = e;
    }
  }
  public long poll() {
    long result;
    synchronized (this) {
      result = this.topAddr;
      if (result != 0L) {
        this.topAddr = Chunk.getNext(result);
      }
    }
    return result;
  }
  /**
   * Returns the address of the "top" item in this stack.
   */
  public long getTopAddress() {
    return this.topAddr;
  }
  /**
   * Removes all the Chunks from this stack
   * and returns the address of the first chunk.
   * The caller owns all the Chunks after this call.
   */
  public long clear() {
    long result;
    synchronized (this) {
      result = this.topAddr;
      if (result != 0L) {
        this.topAddr = 0L;
      }
    }
    return result;
  }
  public void logSizes(LogWriter lw, String msg) {
    long headAddr = this.topAddr;
    long addr;
    boolean concurrentModDetected;
    do {
      concurrentModDetected = false;
      addr = headAddr;
      while (addr != 0L) {
        int curSize = Chunk.getSize(addr);
        addr = Chunk.getNext(addr);
        long curHead = this.topAddr;
        if (curHead != headAddr) {
          headAddr = curHead;
          concurrentModDetected = true;
          // Someone added or removed from the stack.
          // So we break out of the inner loop and start
          // again at the new head.
          break;
        }
        // TODO construct a single log msg
        // that gets reset on the concurrent mad.
        lw.info(msg + curSize);
      }
    } while (concurrentModDetected);
  }
  public long computeTotalSize() {
    long result;
    long headAddr = this.topAddr;
    long addr;
    boolean concurrentModDetected;
    do {
      concurrentModDetected = false;
      result = 0;
      addr = headAddr;
      while (addr != 0L) {
        result += Chunk.getSize(addr);
        addr = Chunk.getNext(addr);
        long curHead = this.topAddr;
        if (curHead != headAddr) {
          headAddr = curHead;
          concurrentModDetected = true;
          // Someone added or removed from the stack.
          // So we break out of the inner loop and start
          // again at the new head.
          break;
        }
      }
    } while (concurrentModDetected);
    return result;
  }
}