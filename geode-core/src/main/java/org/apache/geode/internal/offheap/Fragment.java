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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A fragment is a block of memory that can have chunks allocated from it. The allocations are
 * always from the front so the free memory is always at the end. The freeIdx keeps track of the
 * first byte of free memory in the fragment. The base memory address and the total size of a
 * fragment never change. During defragmentation fragments go away and are recreated.
 *
 *
 */
public class Fragment implements MemoryBlock {
  private static final byte FILL_BYTE = OffHeapStoredObject.FILL_BYTE;
  private final long baseAddr;
  private final int size;
  @SuppressWarnings("unused")
  private volatile int freeIdx;
  private static final AtomicIntegerFieldUpdater<Fragment> freeIdxUpdater =
      AtomicIntegerFieldUpdater.newUpdater(Fragment.class, "freeIdx");

  public Fragment(long addr, int size) {
    MemoryAllocatorImpl.validateAddress(addr);
    baseAddr = addr;
    this.size = size;
    freeIdxUpdater.set(this, 0);
  }

  public int freeSpace() {
    return getSize() - getFreeIndex();
  }

  public boolean allocate(int oldOffset, int newOffset) {
    return freeIdxUpdater.compareAndSet(this, oldOffset, newOffset);
  }

  public int getFreeIndex() {
    return freeIdxUpdater.get(this);
  }

  public int getSize() {
    return size;
  }

  @Override
  public long getAddress() {
    return baseAddr;
  }

  @Override
  public State getState() {
    return State.UNUSED;
  }

  @Override
  public MemoryBlock getNextBlock() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBlockSize() {
    return freeSpace();
  }

  @Override
  public int getSlabId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFreeListId() {
    return -1;
  }

  @Override
  public int getRefCount() {
    return 0;
  }

  @Override
  public String getDataType() {
    return "N/A";
  }

  @Override
  public boolean isSerialized() {
    return false;
  }

  @Override
  public boolean isCompressed() {
    return false;
  }

  @Override
  public Object getDataValue() {
    return null;
  }

  public void fill() {
    AddressableMemoryManager.fill(baseAddr, size, FILL_BYTE);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Fragment) {
      return getAddress() == ((Fragment) o).getAddress();
    }
    return false;
  }

  @Override
  public int hashCode() {
    long value = getAddress();
    return (int) (value ^ (value >>> 32));
  }

  @Override
  public String toString() {
    return "Fragment [baseAddr=" + baseAddr + ", size=" + size + ", freeIdx=" + freeIdx + "]";
  }

}
