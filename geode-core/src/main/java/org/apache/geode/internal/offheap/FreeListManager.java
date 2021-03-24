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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.logging.log4j.Logger;

import org.apache.geode.OutOfOffHeapMemoryException;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Manages the free lists and slabs for a MemoryAllocator
 */
public class FreeListManager {
  static final Logger logger = LogService.getLogger();

  /**
   * The MemoryChunks that this allocator is managing by allocating smaller chunks of them. The
   * contents of this array never change.
   */
  private final Slab[] slabs;
  private final long totalSlabSize;

  private final AtomicReferenceArray<OffHeapStoredObjectAddressStack> tinyFreeLists =
      new AtomicReferenceArray<OffHeapStoredObjectAddressStack>(TINY_FREE_LIST_COUNT);
  // hugeChunkSet is sorted by chunk size in ascending order. It will only contain chunks larger
  // than MAX_TINY.
  private final ConcurrentSkipListSet<OffHeapStoredObject> hugeChunkSet =
      new ConcurrentSkipListSet<OffHeapStoredObject>();
  private final AtomicLong allocatedSize = new AtomicLong(0L);

  private int getNearestTinyMultiple(int size) {
    return (size - 1) / TINY_MULTIPLE;
  }

  List<OffHeapStoredObject> getLiveChunks() {
    ArrayList<OffHeapStoredObject> result = new ArrayList<OffHeapStoredObject>();
    for (int i = 0; i < slabs.length; i++) {
      getLiveChunks(slabs[i], result);
    }
    return result;
  }

  private void getLiveChunks(Slab slab, List<OffHeapStoredObject> result) {
    long addr = slab.getMemoryAddress();
    while (addr <= (slab.getMemoryAddress() + slab.getSize()
        - OffHeapStoredObject.MIN_CHUNK_SIZE)) {
      Fragment f = isAddrInFragmentFreeSpace(addr);
      if (f != null) {
        addr = f.getAddress() + f.getSize();
      } else {
        int curChunkSize = OffHeapStoredObject.getSize(addr);
        int refCount = ReferenceCounter.getRefCount(addr);
        if (refCount > 0) {
          result.add(new OffHeapStoredObject(addr));
        }
        addr += curChunkSize;
      }
    }
  }

  /**
   * If addr is in the free space of a fragment then return that fragment; otherwise return null.
   */
  private Fragment isAddrInFragmentFreeSpace(long addr) {
    for (Fragment f : this.fragmentList) {
      if (addr >= (f.getAddress() + f.getFreeIndex()) && addr < (f.getAddress() + f.getSize())) {
        return f;
      }
    }
    return null;
  }

  public long getUsedMemory() {
    return this.allocatedSize.get();
  }

  public long getFreeMemory() {
    return getTotalMemory() - getUsedMemory();
  }

  long getFreeFragmentMemory() {
    long result = 0;
    for (Fragment f : this.fragmentList) {
      int freeSpace = f.freeSpace();
      if (freeSpace >= OffHeapStoredObject.MIN_CHUNK_SIZE) {
        result += freeSpace;
      }
    }
    return result;
  }

  long getFreeTinyMemory() {
    long tinyFree = 0;
    for (int i = 0; i < this.tinyFreeLists.length(); i++) {
      OffHeapStoredObjectAddressStack cl = this.tinyFreeLists.get(i);
      if (cl != null) {
        tinyFree += cl.computeTotalSize();
      }
    }
    return tinyFree;
  }

  long getFreeHugeMemory() {
    long hugeFree = 0;
    for (OffHeapStoredObject c : this.hugeChunkSet) {
      hugeFree += c.getSize();
    }
    return hugeFree;
  }

  /**
   * The id of the last fragment we allocated from.
   */
  private final AtomicInteger lastFragmentAllocation = new AtomicInteger(0);
  private final CopyOnWriteArrayList<Fragment> fragmentList;
  private final MemoryAllocatorImpl ma;

  public FreeListManager(MemoryAllocatorImpl ma, final Slab[] slabs) {
    this.ma = ma;
    this.slabs = slabs;
    long total = 0;
    Fragment[] tmp = new Fragment[slabs.length];
    for (int i = 0; i < slabs.length; i++) {
      tmp[i] = createFragment(slabs[i].getMemoryAddress(), slabs[i].getSize());
      total += slabs[i].getSize();
    }
    this.fragmentList = new CopyOnWriteArrayList<Fragment>(tmp);
    this.totalSlabSize = total;

    fillFragments();
  }

  /**
   * Create and return a Fragment. This method exists so that tests can override it.
   */
  protected Fragment createFragment(long addr, int size) {
    return new Fragment(addr, size);
  }

  /**
   * Fills all fragments with a fill used for data integrity validation if fill validation is
   * enabled.
   */
  private void fillFragments() {
    if (!this.validateMemoryWithFill) {
      return;
    }
    for (Fragment fragment : this.fragmentList) {
      fragment.fill();
    }
  }

  /**
   * Allocate a chunk of memory of at least the given size. The basic algorithm is: 1. Look for a
   * previously allocated and freed chunk close to the size requested. 2. See if the original chunk
   * is big enough to split. If so do so. 3. Look for a previously allocated and freed chunk of any
   * size larger than the one requested. If we find one split it.
   * <p>
   * It might be better not to include step 3 since we expect and freed chunk to be reallocated in
   * the future. Maybe it would be better for 3 to look for adjacent free blocks that can be merged
   * together. For now we will just try 1 and 2 and then report out of mem.
   *
   * @param size minimum bytes the returned chunk must have.
   * @return the allocated chunk
   * @throws IllegalStateException if a chunk can not be allocated.
   */
  @SuppressWarnings("synthetic-access")
  public OffHeapStoredObject allocate(int size) {
    assert size > 0;

    OffHeapStoredObject result = basicAllocate(size, true);

    result.setDataSize(size);
    this.allocatedSize.addAndGet(result.getSize());
    result.initializeUseCount();

    return result;
  }

  private OffHeapStoredObject basicAllocate(int size, boolean useSlabs) {
    if (useSlabs) {
      // Every object stored off heap has a header so we need
      // to adjust the size so that the header gets allocated.
      // If useSlabs is false then the incoming size has already
      // been adjusted.
      size += OffHeapStoredObject.HEADER_SIZE;
    }
    if (size <= MAX_TINY) {
      return allocateTiny(size, useSlabs);
    } else {
      return allocateHuge(size, useSlabs);
    }
  }

  private OffHeapStoredObject allocateFromFragments(int chunkSize) {
    do {
      final int lastAllocationId = this.lastFragmentAllocation.get();
      for (int i = lastAllocationId; i < this.fragmentList.size(); i++) {
        OffHeapStoredObject result = allocateFromFragment(i, chunkSize);
        if (result != null) {
          return result;
        }
      }
      for (int i = 0; i < lastAllocationId; i++) {
        OffHeapStoredObject result = allocateFromFragment(i, chunkSize);
        if (result != null) {
          return result;
        }
      }
    } while (defragment(chunkSize));
    // We tried all the fragments and didn't find any free memory.
    logOffHeapState(chunkSize);
    final OutOfOffHeapMemoryException failure = new OutOfOffHeapMemoryException(
        "Out of off-heap memory. Could not allocate size of " + chunkSize);
    try {
      throw failure;
    } finally {
      this.ma.getOutOfOffHeapMemoryListener().outOfOffHeapMemory(failure);
    }
  }

  private void logOffHeapState(int chunkSize) {
    logOffHeapState(logger, chunkSize);
  }

  void logOffHeapState(Logger lw, int chunkSize) {
    OffHeapMemoryStats stats = this.ma.getStats();
    lw.info("OutOfOffHeapMemory allocating size of " + chunkSize + ". allocated="
        + this.allocatedSize.get() + " defragmentations=" + this.defragmentationCount.get()
        + " objects=" + stats.getObjects() + " free=" + stats.getFreeMemory() + " fragments="
        + stats.getFragments() + " largestFragment=" + stats.getLargestFragment()
        + " fragmentation=" + stats.getFragmentation());
    logFragmentState(lw);
    logTinyState(lw);
    logHugeState(lw);
  }

  private void logHugeState(Logger lw) {
    for (OffHeapStoredObject c : this.hugeChunkSet) {
      lw.info("Free huge of size " + c.getSize());
    }
  }

  private void logTinyState(Logger lw) {
    for (int i = 0; i < this.tinyFreeLists.length(); i++) {
      OffHeapStoredObjectAddressStack cl = this.tinyFreeLists.get(i);
      if (cl != null) {
        cl.logSizes(lw, "Free tiny of size ");
      }
    }
  }

  private void logFragmentState(Logger lw) {
    for (Fragment f : this.fragmentList) {
      int freeSpace = f.freeSpace();
      if (freeSpace > 0) {
        lw.info("Fragment at " + f.getAddress() + " of size " + f.getSize() + " has " + freeSpace
            + " bytes free.");
      }
    }
  }

  protected final AtomicInteger defragmentationCount = new AtomicInteger();
  /*
   * Set this to "true" to perform data integrity checks on allocated and reused Chunks. This may
   * clobber performance so turn on only when necessary.
   */
  final boolean validateMemoryWithFill =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "validateOffHeapWithFill");
  /**
   * Every allocated chunk smaller than TINY_MULTIPLE*TINY_FREE_LIST_COUNT will allocate a chunk of
   * memory that is a multiple of this value. Sizes are always rounded up to the next multiple of
   * this constant so internal fragmentation will be limited to TINY_MULTIPLE-1 bytes per allocation
   * and on average will be TINY_MULTIPLE/2 given a random distribution of size requests. This does
   * not account for the additional internal fragmentation caused by the off-heap header which
   * currently is always 8 bytes.
   */
  public static final int TINY_MULTIPLE =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_ALIGNMENT", 8);
  static {
    verifyOffHeapAlignment(TINY_MULTIPLE);
  }
  /**
   * Number of free lists to keep for tiny allocations.
   */
  public static final int TINY_FREE_LIST_COUNT =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_FREE_LIST_COUNT", 65536);
  static {
    verifyOffHeapFreeListCount(TINY_FREE_LIST_COUNT);
  }
  /**
   * How many unused bytes are allowed in a huge memory allocation.
   */
  public static final int HUGE_MULTIPLE = 256;
  static {
    verifyHugeMultiple(HUGE_MULTIPLE);
  }
  public static final int MAX_TINY = TINY_MULTIPLE * TINY_FREE_LIST_COUNT;

  /**
   * Return true if the two chunks have been combined into one. If low and high are adjacent to each
   * other and the combined size is small enough (see isSmallEnough) then low's size will be
   * increased by the size of high and true will be returned.
   */
  boolean combineIfAdjacentAndSmallEnough(long lowAddr, long highAddr) {
    assert lowAddr <= highAddr;
    int lowSize = OffHeapStoredObject.getSize(lowAddr);
    if (isAdjacent(lowAddr, lowSize, highAddr)) {
      int highSize = OffHeapStoredObject.getSize(highAddr);
      int combinedSize = lowSize + highSize;
      if (isSmallEnough(combinedSize)) {
        // append the highAddr chunk to lowAddr
        OffHeapStoredObject.setSize(lowAddr, (int) combinedSize);
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the area if memory (starting at lowAddr and extending to lowAddr+lowSize) is
   * right before (i.e. adjacent) to highAddr.
   */
  boolean isAdjacent(long lowAddr, int lowSize, long highAddr) {
    return (lowAddr + lowSize) == highAddr;
  }

  /**
   * Return true if size is small enough to be set as the size of a OffHeapStoredObject.
   */
  boolean isSmallEnough(long size) {
    return size <= Integer.MAX_VALUE;
  }

  /**
   * Defragments memory and returns true if enough memory to allocate chunkSize is freed. Otherwise
   * returns false;
   */
  boolean defragment(int chunkSize) {
    final long startDefragmentationTime = this.ma.getStats().startDefragmentation();
    final int countPreSync = this.defragmentationCount.get();
    afterDefragmentationCountFetched();
    try {
      synchronized (this) {
        if (this.defragmentationCount.get() != countPreSync) {
          // someone else did a defragmentation while we waited on the sync.
          // So just return true causing the caller to retry the allocation.
          return true;
        }
        boolean result = doDefragment(chunkSize);

        // Signal any waiters that a defragmentation happened.
        this.defragmentationCount.incrementAndGet();

        return result;
      } // sync
    } finally {
      this.ma.getStats().endDefragmentation(startDefragmentationTime);
    }
  }

  /**
   * Simple interface the represents a "stack" of primitive longs. Currently this interface only
   * allows supports poll but more could be added if needed in the future. This interface was
   * introduced to aid unit testing. The only implementation of it is
   * OffHeapStoredObjectAddressStack.
   */
  public interface LongStack {
    /**
     * Retrieves and removes the top of this stack, or returns {@code 0L} if this stack is empty.
     */
    long poll();
  }
  /**
   * Manages an array of primitive longs. The array can grow.
   */
  public static class ResizableLongArray {
    private static final int SORT_ARRAY_BLOCK_SIZE = 128;
    long[] data = new long[SORT_ARRAY_BLOCK_SIZE];
    int size = 0;

    public int binarySearch(long l) {
      return Arrays.binarySearch(data, 0, size, l);
    }

    public int size() {
      return size;
    }

    public long get(int idx) {
      return data[idx];
    }

    public void set(int idx, long l) {
      data[idx] = l;
    }

    public void add(long l) {
      if (size >= data.length) {
        long[] newData = new long[data.length + SORT_ARRAY_BLOCK_SIZE];
        System.arraycopy(data, 0, newData, 0, data.length);
        data = newData;
      }
      data[size] = l;
      size++;
    }

    public void insert(int idx, long l) {
      if (size >= data.length) {
        long[] newData = new long[data.length + SORT_ARRAY_BLOCK_SIZE];
        System.arraycopy(data, 0, newData, 0, idx);
        newData[idx] = l;
        System.arraycopy(data, idx, newData, idx + 1, size - idx);
        data = newData;
      } else {
        System.arraycopy(data, idx, data, idx + 1, size - idx);
        data[idx] = l;
      }
      size++;
    }
  }

  /**
   * Defragments memory and returns true if enough memory to allocate chunkSize is freed. Otherwise
   * returns false; Unlike the defragment method this method is not thread safe and does not check
   * for a concurrent defragment. It should only be called by defragment and unit tests.
   */
  boolean doDefragment(int chunkSize) {
    boolean result = false;
    ArrayList<LongStack> freeChunks = new ArrayList<LongStack>();
    collectFreeChunks(freeChunks);
    ResizableLongArray sorted = new ResizableLongArray();
    for (LongStack l : freeChunks) {
      long addr = l.poll();
      while (addr != 0) {
        int idx = sorted.binarySearch(addr);
        idx = -idx;
        idx--;
        int sortedSize = sorted.size();
        if (idx == sortedSize) {
          // addr is > everything in the array
          if (sortedSize == 0) {
            // nothing was in the array
            sorted.add(addr);
          } else {
            if (!combineIfAdjacentAndSmallEnough(sorted.get(idx - 1), addr)) {
              sorted.add(addr);
            }
          }
        } else {
          if (combineIfAdjacentAndSmallEnough(addr, sorted.get(idx))) {
            sorted.set(idx, addr);
          } else {
            if (idx == 0 || !combineIfAdjacentAndSmallEnough(sorted.get(idx - 1), addr)) {
              sorted.insert(idx, addr);
            }
          }
        }
        addr = l.poll();
      }
    }
    for (int i = sorted.size() - 1; i > 0; i--) {
      if (combineIfAdjacentAndSmallEnough(sorted.get(i - 1), sorted.get(i))) {
        sorted.set(i, 0L);
      }
    }

    int largestFragment = 0;
    this.lastFragmentAllocation.set(0);
    ArrayList<Fragment> tmp = new ArrayList<Fragment>();
    for (int i = sorted.size() - 1; i >= 0; i--) {
      long addr = sorted.get(i);
      if (addr == 0L)
        continue;
      int addrSize = OffHeapStoredObject.getSize(addr);
      Fragment f = createFragment(addr, addrSize);
      if (addrSize >= chunkSize) {
        result = true;
      }
      if (addrSize > largestFragment) {
        largestFragment = addrSize;
        // TODO it might be better to sort them biggest first
        tmp.add(0, f);
      } else {
        tmp.add(f);
      }
    }
    this.fragmentList.addAll(tmp);

    fillFragments();

    this.ma.getStats().setLargestFragment(largestFragment);
    this.ma.getStats().setFragments(tmp.size());
    this.ma.getStats().setFragmentation(getFragmentation());

    return result;
  }

  /**
   * Unit tests override this method to get better test coverage
   */
  protected void afterDefragmentationCountFetched() {}

  static void verifyOffHeapAlignment(int tinyMultiple) {
    if (tinyMultiple <= 0 || (tinyMultiple & 3) != 0) {
      throw new IllegalStateException(
          GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_ALIGNMENT must be a multiple of 8.");
    }
    if (tinyMultiple > 256) {
      // this restriction exists because of the dataSize field in the object header.
      throw new IllegalStateException(GeodeGlossary.GEMFIRE_PREFIX
          + "OFF_HEAP_ALIGNMENT must be <= 256 and a multiple of 8.");
    }
  }

  static void verifyOffHeapFreeListCount(int tinyFreeListCount) {
    if (tinyFreeListCount <= 0) {
      throw new IllegalStateException(
          GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_FREE_LIST_COUNT must be >= 1.");
    }
  }

  static void verifyHugeMultiple(int hugeMultiple) {
    if (hugeMultiple > 256 || hugeMultiple < 0) {
      // this restriction exists because of the dataSize field in the object header.
      throw new IllegalStateException(
          "HUGE_MULTIPLE must be >= 0 and <= 256 but it was " + hugeMultiple);
    }
  }

  protected int getFragmentCount() {
    return this.fragmentList.size();
  }

  protected int getFragmentation() {
    if (getUsedMemory() == 0) {
      // when no memory is used then there is no fragmentation
      return 0;
    } else {
      int availableFragments = getFragmentCount();
      if (availableFragments == 0) {
        // zero fragments means no free memory then no fragmentation
        return 0;
      } else if (availableFragments == 1) {
        // free memory is available as one fragment, so no fragmentation
        return 0;
      } else {
        // more than 1 fragment is available so freeMemory is > ObjectChunk.MIN_CHUNK_SIZE
        long freeMemory = getFreeMemory();
        assert freeMemory > OffHeapStoredObject.MIN_CHUNK_SIZE;
        long maxPossibleFragments = freeMemory / OffHeapStoredObject.MIN_CHUNK_SIZE;
        double fragmentation = ((double) availableFragments / (double) maxPossibleFragments) * 100d;
        return (int) Math.rint(fragmentation);
      }
    }
  }

  private void collectFreeChunks(List<LongStack> l) {
    collectFreeFragmentChunks(l);
    collectFreeHugeChunks(l);
    collectFreeTinyChunks(l);
  }

  List<Fragment> getFragmentList() {
    return this.fragmentList;
  }

  private void collectFreeFragmentChunks(List<LongStack> l) {
    if (this.fragmentList.size() == 0)
      return;
    OffHeapStoredObjectAddressStack result = new OffHeapStoredObjectAddressStack();
    for (Fragment f : this.fragmentList) {
      int offset;
      int diff;
      do {
        offset = f.getFreeIndex();
        diff = f.getSize() - offset;
      } while (diff >= OffHeapStoredObject.MIN_CHUNK_SIZE && !f.allocate(offset, offset + diff));
      if (diff < OffHeapStoredObject.MIN_CHUNK_SIZE) {
        // If diff > 0 then that memory will be lost during defragmentation.
        // This should never happen since we keep the sizes rounded
        // based on MIN_CHUNK_SIZE.
        assert diff == 0;
        // The current fragment is completely allocated so just skip it.
        continue;
      }
      long chunkAddr = f.getAddress() + offset;
      OffHeapStoredObject.setSize(chunkAddr, diff);
      result.offer(chunkAddr);
    }
    // All the fragments have been turned in to chunks so now clear them
    // The defragmentation will create new fragments.
    this.fragmentList.clear();
    if (!result.isEmpty()) {
      l.add(result);
    }
  }

  private void collectFreeTinyChunks(List<LongStack> l) {
    for (int i = 0; i < this.tinyFreeLists.length(); i++) {
      OffHeapStoredObjectAddressStack cl = this.tinyFreeLists.get(i);
      if (cl != null) {
        long head = cl.clear();
        if (head != 0L) {
          l.add(new OffHeapStoredObjectAddressStack(head));
        }
      }
    }
  }

  private void collectFreeHugeChunks(List<LongStack> l) {
    OffHeapStoredObject c = this.hugeChunkSet.pollFirst();
    OffHeapStoredObjectAddressStack result = null;
    while (c != null) {
      if (result == null) {
        result = new OffHeapStoredObjectAddressStack();
        l.add(result);
      }
      result.offer(c.getAddress());
      c = this.hugeChunkSet.pollFirst();
    }
  }

  OffHeapStoredObject allocateFromFragment(final int fragIdx, final int chunkSize) {
    if (fragIdx >= this.fragmentList.size())
      return null;
    final Fragment fragment;
    try {
      fragment = this.fragmentList.get(fragIdx);
    } catch (IndexOutOfBoundsException ignore) {
      // A concurrent defragmentation can cause this.
      return null;
    }
    boolean retryFragment;
    do {
      retryFragment = false;
      int oldOffset = fragment.getFreeIndex();
      int fragmentSize = fragment.getSize();
      int fragmentFreeSize = fragmentSize - oldOffset;
      if (fragmentFreeSize >= chunkSize) {
        // this fragment has room
        int newOffset = oldOffset + chunkSize;
        int extraSize = fragmentSize - newOffset;
        if (extraSize < OffHeapStoredObject.MIN_CHUNK_SIZE) {
          // include these last few bytes of the fragment in the allocation.
          // If we don't then they will be lost forever.
          // The extraSize bytes only apply to the first chunk we allocate (not the batch ones).
          newOffset += extraSize;
        } else {
          extraSize = 0;
        }
        if (fragment.allocate(oldOffset, newOffset)) {
          // We did the allocate!
          this.lastFragmentAllocation.set(fragIdx);
          OffHeapStoredObject result =
              new OffHeapStoredObject(fragment.getAddress() + oldOffset, chunkSize + extraSize);
          checkDataIntegrity(result);
          return result;
        } else {
          OffHeapStoredObject result = basicAllocate(chunkSize, false);
          if (result != null) {
            return result;
          }
          retryFragment = true;
        }
      }
    } while (retryFragment);
    return null; // did not find enough free space in this fragment
  }

  private int round(int multiple, int value) {
    return (int) ((((long) value + (multiple - 1)) / multiple) * multiple);
  }

  private OffHeapStoredObject allocateTiny(int size, boolean useFragments) {
    return basicAllocate(getNearestTinyMultiple(size), TINY_MULTIPLE, 0, this.tinyFreeLists,
        useFragments);
  }

  private OffHeapStoredObject basicAllocate(int idx, int multiple, int offset,
      AtomicReferenceArray<OffHeapStoredObjectAddressStack> freeLists, boolean useFragments) {
    OffHeapStoredObjectAddressStack clq = freeLists.get(idx);
    if (clq != null) {
      long memAddr = clq.poll();
      if (memAddr != 0) {
        OffHeapStoredObject result = new OffHeapStoredObject(memAddr);
        checkDataIntegrity(result);
        result.readyForAllocation();
        return result;
      }
    }
    if (useFragments) {
      return allocateFromFragments(((idx + 1) * multiple) + offset);
    } else {
      return null;
    }
  }

  private OffHeapStoredObject allocateHuge(int size, boolean useFragments) {
    // sizeHolder is a fake Chunk used to search our sorted hugeChunkSet.
    OffHeapStoredObject sizeHolder = new SearchMarker(size);
    NavigableSet<OffHeapStoredObject> ts = this.hugeChunkSet.tailSet(sizeHolder);
    OffHeapStoredObject result = ts.pollFirst();
    if (result != null) {
      if (result.getSize() - (HUGE_MULTIPLE - OffHeapStoredObject.HEADER_SIZE) < size) {
        // close enough to the requested size; just return it.
        checkDataIntegrity(result);
        result.readyForAllocation();
        return result;
      } else {
        this.hugeChunkSet.add(result);
      }
    }
    if (useFragments) {
      // We round it up to the next multiple of TINY_MULTIPLE to make
      // sure we always have chunks allocated on an 8 byte boundary.
      return allocateFromFragments(round(TINY_MULTIPLE, size));
    } else {
      return null;
    }
  }

  private void checkDataIntegrity(OffHeapStoredObject data) {
    if (this.validateMemoryWithFill) {
      data.validateFill();
    }
  }

  /**
   * Used by the FreeListManager to easily search its ConcurrentSkipListSet. This is not a real
   * OffHeapStoredObject but only used for searching.
   */
  private static class SearchMarker extends OffHeapStoredObject {
    private final int size;

    public SearchMarker(int size) {
      super();
      this.size = size;
    }

    @Override
    public int getSize() {
      return this.size;
    }
  }

  @SuppressWarnings("synthetic-access")
  public void free(long addr) {
    if (this.validateMemoryWithFill) {
      OffHeapStoredObject.fill(addr);
    }

    free(addr, true);
  }

  private void free(long addr, boolean updateStats) {
    int cSize = OffHeapStoredObject.getSize(addr);
    if (updateStats) {
      OffHeapMemoryStats stats = this.ma.getStats();
      stats.incObjects(-1);
      this.allocatedSize.addAndGet(-cSize);
      stats.incUsedMemory(-cSize);
      stats.incFreeMemory(cSize);
      this.ma.notifyListeners();
    }
    if (cSize <= MAX_TINY) {
      freeTiny(addr, cSize);
    } else {
      freeHuge(addr, cSize);
    }
  }

  private void freeTiny(long addr, int cSize) {
    basicFree(addr, getNearestTinyMultiple(cSize), this.tinyFreeLists);
  }

  private void basicFree(long addr, int idx,
      AtomicReferenceArray<OffHeapStoredObjectAddressStack> freeLists) {
    OffHeapStoredObjectAddressStack clq = freeLists.get(idx);
    if (clq != null) {
      clq.offer(addr);
    } else {
      clq = createFreeListForEmptySlot(freeLists, idx);
      clq.offer(addr);
      if (!freeLists.compareAndSet(idx, null, clq)) {
        clq = freeLists.get(idx);
        clq.offer(addr);
      }
    }
  }

  /**
   * Tests override this method to simulate concurrent modification
   */
  protected OffHeapStoredObjectAddressStack createFreeListForEmptySlot(
      AtomicReferenceArray<OffHeapStoredObjectAddressStack> freeLists, int idx) {
    return new OffHeapStoredObjectAddressStack();
  }

  private void freeHuge(long addr, int cSize) {
    this.hugeChunkSet.add(new OffHeapStoredObject(addr)); // TODO make this a collection of longs
  }

  List<MemoryBlock> getOrderedBlocks() {
    final List<MemoryBlock> value = new ArrayList<MemoryBlock>();
    addBlocksFromFragments(this.fragmentList, value); // unused fragments
    addBlocksFromChunks(getLiveChunks(), value); // used chunks
    addBlocksFromChunks(this.hugeChunkSet, value); // huge free chunks
    addMemoryBlocks(getTinyFreeBlocks(), value); // tiny free chunks
    Collections.sort(value, new Comparator<MemoryBlock>() {
      @Override
      public int compare(MemoryBlock o1, MemoryBlock o2) {
        return Long.compare(o1.getAddress(), o2.getAddress());
      }
    });
    return value;
  }

  private void addBlocksFromFragments(Collection<Fragment> src, List<MemoryBlock> dest) {
    for (MemoryBlock block : src) {
      dest.add(new MemoryBlockNode(this.ma, block));
    }
  }

  private void addBlocksFromChunks(Collection<OffHeapStoredObject> src, List<MemoryBlock> dest) {
    for (OffHeapStoredObject chunk : src) {
      dest.add(new MemoryBlockNode(this.ma, chunk));
    }
  }

  private void addMemoryBlocks(Collection<MemoryBlock> src, List<MemoryBlock> dest) {
    for (MemoryBlock block : src) {
      dest.add(new MemoryBlockNode(this.ma, block));
    }
  }

  private List<MemoryBlock> getTinyFreeBlocks() {
    final List<MemoryBlock> value = new ArrayList<MemoryBlock>();
    final MemoryAllocatorImpl sma = this.ma;
    for (int i = 0; i < this.tinyFreeLists.length(); i++) {
      if (this.tinyFreeLists.get(i) == null)
        continue;
      long addr = this.tinyFreeLists.get(i).getTopAddress();
      while (addr != 0L) {
        value.add(new MemoryBlockNode(sma, new TinyMemoryBlock(addr, i)));
        addr = OffHeapStoredObject.getNext(addr);
      }
    }
    return value;
  }

  List<MemoryBlock> getAllocatedBlocks() {
    final List<MemoryBlock> value = new ArrayList<MemoryBlock>();
    addBlocksFromChunks(getLiveChunks(), value); // used chunks
    Collections.sort(value, new Comparator<MemoryBlock>() {
      @Override
      public int compare(MemoryBlock o1, MemoryBlock o2) {
        return Long.compare(o1.getAddress(), o2.getAddress());
      }
    });
    return value;
  }

  /**
   * Used to represent an address from a tiny free list as a MemoryBlock
   */
  protected static class TinyMemoryBlock implements MemoryBlock {
    private final long address;
    private final int freeListId;

    protected TinyMemoryBlock(long address, int freeListId) {
      this.address = address;
      this.freeListId = freeListId;
    }

    @Override
    public State getState() {
      return State.DEALLOCATED;
    }

    @Override
    public long getAddress() {
      return address;
    }

    @Override
    public int getBlockSize() {
      return OffHeapStoredObject.getSize(address);
    }

    @Override
    public MemoryBlock getNextBlock() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getSlabId() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getFreeListId() {
      return freeListId;
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

    @Override
    public boolean equals(Object o) {
      if (o instanceof TinyMemoryBlock) {
        return getAddress() == ((TinyMemoryBlock) o).getAddress();
      }
      return false;
    }

    @Override
    public int hashCode() {
      long value = this.getAddress();
      return (int) (value ^ (value >>> 32));
    }
  }

  long getTotalMemory() {
    return this.totalSlabSize;
  }

  void freeSlabs() {
    for (int i = 0; i < slabs.length; i++) {
      slabs[i].free();
    }
  }

  /**
   * newSlabs will be non-null in unit tests. If the unit test gave us a different array of slabs
   * then something is wrong because we are trying to reuse the old already allocated array which
   * means that the new one will never be used. Note that this code does not bother comparing the
   * contents of the arrays.
   */
  boolean okToReuse(Slab[] newSlabs) {
    return newSlabs == null || newSlabs == this.slabs;
  }

  int getLargestSlabSize() {
    return this.slabs[0].getSize();
  }

  int findSlab(long addr) {
    for (int i = 0; i < this.slabs.length; i++) {
      Slab slab = this.slabs[i];
      long slabAddr = slab.getMemoryAddress();
      if (addr >= slabAddr) {
        if (addr < slabAddr + slab.getSize()) {
          return i;
        }
      }
    }
    throw new IllegalStateException("could not find a slab for addr " + addr);
  }

  void getSlabDescriptions(StringBuilder sb) {
    for (int i = 0; i < slabs.length; i++) {
      long startAddr = slabs[i].getMemoryAddress();
      long endAddr = startAddr + slabs[i].getSize();
      sb.append("[").append(Long.toString(startAddr, 16)).append("..")
          .append(Long.toString(endAddr, 16)).append("] ");
    }
  }

  boolean validateAddressAndSizeWithinSlab(long addr, int size) {
    for (int i = 0; i < slabs.length; i++) {
      if (slabs[i].getMemoryAddress() <= addr
          && addr < (slabs[i].getMemoryAddress() + slabs[i].getSize())) {
        // validate addr + size is within the same slab
        if (size != -1) { // skip this check if size is -1
          if (!(slabs[i].getMemoryAddress() <= (addr + size - 1)
              && (addr + size - 1) < (slabs[i].getMemoryAddress() + slabs[i].getSize()))) {
            throw new IllegalStateException(" address 0x" + Long.toString(addr + size - 1, 16)
                + " does not address the original slab memory");
          }
        }
        return true;
      }
    }
    return false;
  }

}
