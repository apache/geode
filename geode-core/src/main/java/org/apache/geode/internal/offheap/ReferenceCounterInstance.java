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

  int getRefCount(long address) {
    return AddressableMemoryManager.readInt(address + OffHeapStoredObject.REF_COUNT_OFFSET)
        & OffHeapStoredObject.REF_COUNT_MASK;
  }

  boolean retain(long address) {
    MemoryAllocatorImpl.validateAddress(address);
    int usageCount;
    int rawBits;
    int retryCount = 0;

    do {
      rawBits =
          AddressableMemoryManager.readIntVolatile(address + OffHeapStoredObject.REF_COUNT_OFFSET);
      if ((rawBits & OffHeapStoredObject.MAGIC_MASK) != OffHeapStoredObject.MAGIC_NUMBER) {
        return false;
      }
      usageCount = rawBits & OffHeapStoredObject.REF_COUNT_MASK;
      if (usageCount == OffHeapStoredObject.MAX_REF_COUNT) {
        throw new IllegalStateException(
            "Maximum use count exceeded. rawBits=" + Integer.toHexString(rawBits));
      }
      if (usageCount == 0) {
        return false;
      }
      retryCount++;
      if (retryCount > 1000) {
        throw new IllegalStateException("tried to write " + (rawBits + 1) + " to @"
            + Long.toHexString(address) + " 1,000 times.");
      }
    } while (!AddressableMemoryManager
        .writeIntVolatile(address + OffHeapStoredObject.REF_COUNT_OFFSET, rawBits, rawBits + 1));

    if (ReferenceCountHelper.trackReferenceCounts()) {
      ReferenceCountHelper.refCountChanged(address, false, usageCount + 1);
    }

    return true;
  }

  void release(final long address) {
    release(address, null);
  }

  void release(final long address, FreeListManager freeListManager) {
    MemoryAllocatorImpl.validateAddress(address);
    int newCount;
    int rawBits;
    boolean returnToAllocator;

    do {
      returnToAllocator = false;
      rawBits =
          AddressableMemoryManager.readIntVolatile(address + OffHeapStoredObject.REF_COUNT_OFFSET);
      if ((rawBits & OffHeapStoredObject.MAGIC_MASK) != OffHeapStoredObject.MAGIC_NUMBER) {
        String message = "It looks like off heap memory @" + Long.toHexString(address)
            + " was already freed. rawBits=" + Integer.toHexString(rawBits) + " history="
            + ReferenceCountHelper.getFreeRefCountInfo(address);
        // debugLog(msg, true);
        throw new IllegalStateException(message);
      }

      int currentCount = rawBits & OffHeapStoredObject.REF_COUNT_MASK;
      if (currentCount == 0) {
        throw new IllegalStateException("Memory has already been freed. history="
            + ReferenceCountHelper.getFreeRefCountInfo(address));
      }

      if (currentCount == 1) {
        // clear the use count, bits, and the delta size since it will be freed.
        newCount = 0;
        returnToAllocator = true;
      } else {
        newCount = rawBits - 1;
      }
    } while (!AddressableMemoryManager
        .writeIntVolatile(address + OffHeapStoredObject.REF_COUNT_OFFSET, rawBits, newCount));

    if (returnToAllocator) {
      if (ReferenceCountHelper.trackReferenceCounts()) {
        if (ReferenceCountHelper.trackFreedReferenceCounts()) {
          ReferenceCountHelper.refCountChanged(address, true,
              newCount & OffHeapStoredObject.REF_COUNT_MASK);
        }
        ReferenceCountHelper.freeRefCountInfo(address);
      }

      if (freeListManager == null) {
        freeListManager = MemoryAllocatorImpl.getAllocator().getFreeListManager();
      }

      freeListManager.free(address);
    } else {
      if (ReferenceCountHelper.trackReferenceCounts()) {
        ReferenceCountHelper.refCountChanged(address, true,
            newCount & OffHeapStoredObject.REF_COUNT_MASK);
      }
    }
  }
}
