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
package org.apache.geode.internal.offheap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.offheap.FreeListManager.LongStack;

/**
 * A "stack" of addresses of OffHeapStoredObject instances. The stored objects are not kept in java
 * object form but instead each one is just an off-heap address. This class is used for each "tiny"
 * free-list of the FreeListManager. This class is thread safe.
 */
public class OffHeapStoredObjectAddressStack implements LongStack {
  // Ok to read without sync but must be synced on write
  private volatile long topAddr;

  public OffHeapStoredObjectAddressStack(long addr) {
    if (addr != 0L) {
      MemoryAllocatorImpl.validateAddress(addr);
    }
    topAddr = addr;
  }

  public OffHeapStoredObjectAddressStack() {
    topAddr = 0L;
  }

  public boolean isEmpty() {
    return topAddr == 0L;
  }

  public void offer(long e) {
    assert e != 0;
    MemoryAllocatorImpl.validateAddress(e);
    synchronized (this) {
      OffHeapStoredObject.setNext(e, topAddr);
      topAddr = e;
    }
  }

  @Override
  public long poll() {
    long result;
    synchronized (this) {
      result = topAddr;
      if (result != 0L) {
        topAddr = OffHeapStoredObject.getNext(result);
      }
    }
    return result;
  }

  /**
   * Returns the address of the "top" item in this stack.
   */
  public long getTopAddress() {
    return topAddr;
  }

  /**
   * Removes all the addresses from this stack and returns the top address. The caller owns all the
   * addresses after this call.
   */
  public long clear() {
    long result;
    synchronized (this) {
      result = topAddr;
      if (result != 0L) {
        topAddr = 0L;
      }
    }
    return result;
  }

  public void logSizes(Logger logger, String msg) {
    long headAddr = topAddr;
    long addr;
    boolean concurrentModDetected;
    do {
      concurrentModDetected = false;
      addr = headAddr;
      while (addr != 0L) {
        int curSize = OffHeapStoredObject.getSize(addr);
        addr = OffHeapStoredObject.getNext(addr);
        testHookDoConcurrentModification();
        long curHead = topAddr;
        if (curHead != headAddr) {
          headAddr = curHead;
          concurrentModDetected = true;
          // Someone added or removed from the stack.
          // So we break out of the inner loop and start
          // again at the new head.
          break;
        }
        // TODO construct a single log msg
        // that gets reset when concurrentModDetected.
        logger.info(msg + curSize);
      }
    } while (concurrentModDetected);
  }

  public long computeTotalSize() {
    long result;
    long headAddr = topAddr;
    long addr;
    boolean concurrentModDetected;
    do {
      concurrentModDetected = false;
      result = 0;
      addr = headAddr;
      while (addr != 0L) {
        result += OffHeapStoredObject.getSize(addr);
        addr = OffHeapStoredObject.getNext(addr);
        testHookDoConcurrentModification();
        long curHead = topAddr;
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

  /**
   * This method allows tests to override it and do a concurrent modification to the stack. For
   * production code it will be a noop.
   */
  protected void testHookDoConcurrentModification() {
    // nothing needed in production code
  }
}
