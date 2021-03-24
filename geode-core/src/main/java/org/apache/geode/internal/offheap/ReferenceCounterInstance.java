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

class ReferenceCounterInstance {

  int getRefCount(long memAddr) {
    return AddressableMemoryManager.readInt(memAddr + OffHeapStoredObject.REF_COUNT_OFFSET)
        & OffHeapStoredObject.REF_COUNT_MASK;
  }

  boolean retain(long memAddr) {
    MemoryAllocatorImpl.validateAddress(memAddr);
    int uc;
    int rawBits;
    int retryCount = 0;
    do {
      rawBits =
          AddressableMemoryManager.readIntVolatile(memAddr + OffHeapStoredObject.REF_COUNT_OFFSET);
      if ((rawBits & OffHeapStoredObject.MAGIC_MASK) != OffHeapStoredObject.MAGIC_NUMBER) {
        // same as uc == 0
        // TODO MAGIC_NUMBER rethink its use and interaction with compactor fragments
        return false;
      }
      uc = rawBits & OffHeapStoredObject.REF_COUNT_MASK;
      if (uc == OffHeapStoredObject.MAX_REF_COUNT) {
        throw new IllegalStateException(
            "Maximum use count exceeded. rawBits=" + Integer.toHexString(rawBits));
      } else if (uc == 0) {
        return false;
      }
      retryCount++;
      if (retryCount > 1000) {
        throw new IllegalStateException("tried to write " + (rawBits + 1) + " to @"
            + Long.toHexString(memAddr) + " 1,000 times.");
      }
    } while (!AddressableMemoryManager.writeIntVolatile(
        memAddr + OffHeapStoredObject.REF_COUNT_OFFSET, rawBits,
        rawBits + 1));
    // debugLog("use inced ref count " + (uc+1) + " @" + Long.toHexString(memAddr), true);
    if (ReferenceCountHelper.trackReferenceCounts()) {
      ReferenceCountHelper.refCountChanged(memAddr, false, uc + 1);
    }

    return true;
  }

  void release(final long memAddr) {
    release(memAddr, null);
  }

  void release(final long memAddr, FreeListManager freeListManager) {
    MemoryAllocatorImpl.validateAddress(memAddr);
    int newCount;
    int rawBits;
    boolean returnToAllocator;
    do {
      returnToAllocator = false;
      rawBits =
          AddressableMemoryManager.readIntVolatile(memAddr + OffHeapStoredObject.REF_COUNT_OFFSET);
      if ((rawBits & OffHeapStoredObject.MAGIC_MASK) != OffHeapStoredObject.MAGIC_NUMBER) {
        String msg = "It looks like off heap memory @" + Long.toHexString(memAddr)
            + " was already freed. rawBits=" + Integer.toHexString(rawBits) + " history="
            + ReferenceCountHelper.getFreeRefCountInfo(memAddr);
        // debugLog(msg, true);
        throw new IllegalStateException(msg);
      }
      int curCount = rawBits & OffHeapStoredObject.REF_COUNT_MASK;
      if ((curCount) == 0) {
        // debugLog("too many frees @" + Long.toHexString(memAddr), true);
        throw new IllegalStateException(
            "Memory has already been freed." + " history=" + ReferenceCountHelper
                .getFreeRefCountInfo(memAddr) /* + System.identityHashCode(this) */);
      }
      if (curCount == 1) {
        newCount = 0; // clear the use count, bits, and the delta size since it will be freed.
        returnToAllocator = true;
      } else {
        newCount = rawBits - 1;
      }
    } while (!AddressableMemoryManager.writeIntVolatile(
        memAddr + OffHeapStoredObject.REF_COUNT_OFFSET, rawBits,
        newCount));
    if (returnToAllocator) {
      if (ReferenceCountHelper.trackReferenceCounts()) {
        if (ReferenceCountHelper.trackFreedReferenceCounts()) {
          ReferenceCountHelper.refCountChanged(memAddr, true,
              newCount & OffHeapStoredObject.REF_COUNT_MASK);
        }
        ReferenceCountHelper.freeRefCountInfo(memAddr);
      }
      if (freeListManager == null) {
        freeListManager = MemoryAllocatorImpl.getAllocator().getFreeListManager();
      }
      freeListManager.free(memAddr);
    } else {
      if (ReferenceCountHelper.trackReferenceCounts()) {
        ReferenceCountHelper.refCountChanged(memAddr, true,
            newCount & OffHeapStoredObject.REF_COUNT_MASK);
      }
    }
  }
}
