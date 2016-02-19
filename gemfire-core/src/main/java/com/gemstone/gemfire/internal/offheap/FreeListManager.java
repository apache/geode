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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.OutOfOffHeapMemoryException;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

/**
 * Manages the free lists for a SimpleMemoryAllocatorImpl
 */
public class FreeListManager {
  /** The MemoryChunks that this allocator is managing by allocating smaller chunks of them.
   * The contents of this array never change.
   */
  private final AddressableMemoryChunk[] slabs;
  private final long totalSlabSize;
  
  final private AtomicReferenceArray<SyncChunkStack> tinyFreeLists = new AtomicReferenceArray<SyncChunkStack>(TINY_FREE_LIST_COUNT);
  // hugeChunkSet is sorted by chunk size in ascending order. It will only contain chunks larger than MAX_TINY.
  private final ConcurrentSkipListSet<ObjectChunk> hugeChunkSet = new ConcurrentSkipListSet<ObjectChunk>();
  private final AtomicLong allocatedSize = new AtomicLong(0L);

  private int getNearestTinyMultiple(int size) {
    return (size-1)/TINY_MULTIPLE;
  }
  List<ObjectChunk> getLiveChunks() {
    ArrayList<ObjectChunk> result = new ArrayList<ObjectChunk>();
    for (int i=0; i < slabs.length; i++) {
      getLiveChunks(slabs[i], result);
    }
    return result;
  }
  private void getLiveChunks(AddressableMemoryChunk slab, List<ObjectChunk> result) {
    long addr = slab.getMemoryAddress();
    while (addr <= (slab.getMemoryAddress() + slab.getSize() - ObjectChunk.MIN_CHUNK_SIZE)) {
      Fragment f = isAddrInFragmentFreeSpace(addr);
      if (f != null) {
        addr = f.getMemoryAddress() + f.getSize();
      } else {
        int curChunkSize = ObjectChunk.getSize(addr);
        int refCount = ObjectChunk.getRefCount(addr);
        if (refCount > 0) {
          result.add(new ObjectChunk(addr));
        }
        addr += curChunkSize;
      }
    }
  }
  /**
   * If addr is in the free space of a fragment then return that fragment; otherwise return null.
   */
  private Fragment isAddrInFragmentFreeSpace(long addr) {
    for (Fragment f: this.fragmentList) {
      if (addr >= (f.getMemoryAddress() + f.getFreeIndex()) && addr < (f.getMemoryAddress() + f.getSize())) {
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
    for (Fragment f: this.fragmentList) {
      int freeSpace = f.freeSpace();
      if (freeSpace >= ObjectChunk.MIN_CHUNK_SIZE) {
        result += freeSpace;
      }
    }
    return result;
  }
  long getFreeTinyMemory() {
    long tinyFree = 0;
    for (int i=0; i < this.tinyFreeLists.length(); i++) {
      SyncChunkStack cl = this.tinyFreeLists.get(i);
      if (cl != null) {
        tinyFree += cl.computeTotalSize();
      }
    }
    return tinyFree;
  }
  long getFreeHugeMemory() {
    long hugeFree = 0;
    for (ObjectChunk c: this.hugeChunkSet) {
      hugeFree += c.getSize();
    }
    return hugeFree;
  }

  /**
   * The id of the last fragment we allocated from.
   */
  private final AtomicInteger lastFragmentAllocation = new AtomicInteger(0);
  private final CopyOnWriteArrayList<Fragment> fragmentList;
  private final SimpleMemoryAllocatorImpl ma;

  public FreeListManager(SimpleMemoryAllocatorImpl ma, final AddressableMemoryChunk[] slabs) {
    this.ma = ma;
    this.slabs = slabs;
    long total = 0;
    Fragment[] tmp = new Fragment[slabs.length];
    for (int i=0; i < slabs.length; i++) {
      tmp[i] = createFragment(slabs[i].getMemoryAddress(), slabs[i].getSize());
      total += slabs[i].getSize();
    }
    this.fragmentList = new CopyOnWriteArrayList<Fragment>(tmp);
    this.totalSlabSize = total;

    fillFragments();
  }

  /**
   * Create and return a Fragment.
   * This method exists so that tests can override it.
   */
  protected Fragment createFragment(long addr, int size) {
    return new Fragment(addr, size);
  }
  
  /**
   * Fills all fragments with a fill used for data integrity validation 
   * if fill validation is enabled.
   */
  private void fillFragments() {
    if (!this.validateMemoryWithFill) {
      return;
    }
    for(Fragment fragment : this.fragmentList) {
      fragment.fill();
    }
  }

  /**
   * Allocate a chunk of memory of at least the given size.
   * The basic algorithm is:
   * 1. Look for a previously allocated and freed chunk close to the size requested.
   * 2. See if the original chunk is big enough to split. If so do so.
   * 3. Look for a previously allocated and freed chunk of any size larger than the one requested.
   *    If we find one split it.
   * <p>
   * It might be better not to include step 3 since we expect and freed chunk to be reallocated in the future.
   * Maybe it would be better for 3 to look for adjacent free blocks that can be merged together.
   * For now we will just try 1 and 2 and then report out of mem.
   * @param size minimum bytes the returned chunk must have.
   * @return the allocated chunk
   * @throws IllegalStateException if a chunk can not be allocated.
   */
  @SuppressWarnings("synthetic-access")
  public ObjectChunk allocate(int size) {
    assert size > 0;
    
    ObjectChunk result = basicAllocate(size, true);

    result.setDataSize(size);
    this.allocatedSize.addAndGet(result.getSize());
    result.initializeUseCount();

    return result;
  }

  private ObjectChunk basicAllocate(int size, boolean useSlabs) {
    if (useSlabs) {
      // Every object stored off heap has a header so we need
      // to adjust the size so that the header gets allocated.
      // If useSlabs is false then the incoming size has already
      // been adjusted.
      size += ObjectChunk.OFF_HEAP_HEADER_SIZE;
    }
    if (size <= MAX_TINY) {
      return allocateTiny(size, useSlabs);
    } else {
      return allocateHuge(size, useSlabs);
    }
  }

  private ObjectChunk allocateFromFragments(int chunkSize) {
    do {
      final int lastAllocationId = this.lastFragmentAllocation.get();
      for (int i=lastAllocationId; i < this.fragmentList.size(); i++) {
        ObjectChunk result = allocateFromFragment(i, chunkSize);
        if (result != null) {
          return result;
        }
      }
      for (int i=0; i < lastAllocationId; i++) {
        ObjectChunk result = allocateFromFragment(i, chunkSize);
        if (result != null) {
          return result;
        }
      }
    } while (compact(chunkSize));
    // We tried all the fragments and didn't find any free memory.
    logOffHeapState(chunkSize);
    final OutOfOffHeapMemoryException failure = new OutOfOffHeapMemoryException("Out of off-heap memory. Could not allocate size of " + chunkSize);
    try {
      throw failure;
    } finally {
      this.ma.getOutOfOffHeapMemoryListener().outOfOffHeapMemory(failure);
    }
  }

  private void logOffHeapState(int chunkSize) {
    if (InternalDistributedSystem.getAnyInstance() != null) {
      LogWriter lw = InternalDistributedSystem.getAnyInstance().getLogWriter();
      logOffHeapState(lw, chunkSize);
    }
  }

  void logOffHeapState(LogWriter lw, int chunkSize) {
    OffHeapMemoryStats stats = this.ma.getStats();
    lw.info("OutOfOffHeapMemory allocating size of " + chunkSize + ". allocated=" + this.allocatedSize.get() + " compactions=" + this.compactCount.get() + " objects=" + stats.getObjects() + " free=" + stats.getFreeMemory() + " fragments=" + stats.getFragments() + " largestFragment=" + stats.getLargestFragment() + " fragmentation=" + stats.getFragmentation());
    logFragmentState(lw);
    logTinyState(lw);
    logHugeState(lw);
  }

  private void logHugeState(LogWriter lw) {
    for (ObjectChunk c: this.hugeChunkSet) {
      lw.info("Free huge of size " + c.getSize());
    }
  }
  private void logTinyState(LogWriter lw) {
    for (int i=0; i < this.tinyFreeLists.length(); i++) {
      SyncChunkStack cl = this.tinyFreeLists.get(i);
      if (cl != null) {
        cl.logSizes(lw, "Free tiny of size ");
      }
    }
  }
  private void logFragmentState(LogWriter lw) {
    for (Fragment f: this.fragmentList) {
      int freeSpace = f.freeSpace();
      if (freeSpace > 0) {
        lw.info("Fragment at " + f.getMemoryAddress() + " of size " + f.getSize() + " has " + freeSpace + " bytes free.");
      }
    }
  }

  protected final AtomicInteger compactCount = new AtomicInteger();
  /*
   * Set this to "true" to perform data integrity checks on allocated and reused Chunks.  This may clobber 
   * performance so turn on only when necessary.
   */
  final boolean validateMemoryWithFill = Boolean.getBoolean("gemfire.validateOffHeapWithFill");
  /**
   * Every allocated chunk smaller than TINY_MULTIPLE*TINY_FREE_LIST_COUNT will allocate a chunk of memory that is a multiple of this value.
   * Sizes are always rounded up to the next multiple of this constant
   * so internal fragmentation will be limited to TINY_MULTIPLE-1 bytes per allocation
   * and on average will be TINY_MULTIPLE/2 given a random distribution of size requests.
   * This does not account for the additional internal fragmentation caused by the off-heap header
   * which currently is always 8 bytes.
   */
  public final static int TINY_MULTIPLE = Integer.getInteger("gemfire.OFF_HEAP_ALIGNMENT", 8);
  static {
    verifyOffHeapAlignment(TINY_MULTIPLE);
  }
  /**
   * Number of free lists to keep for tiny allocations.
   */
  public final static int TINY_FREE_LIST_COUNT = Integer.getInteger("gemfire.OFF_HEAP_FREE_LIST_COUNT", 16384);
  static {
    verifyOffHeapFreeListCount(TINY_FREE_LIST_COUNT);
  }
  /**
   * How many unused bytes are allowed in a huge memory allocation.
   */
  public final static int HUGE_MULTIPLE = 256;
  static {
    verifyHugeMultiple(HUGE_MULTIPLE);
  }
  public final static int MAX_TINY = TINY_MULTIPLE*TINY_FREE_LIST_COUNT;
  /**
   * Compacts memory and returns true if enough memory to allocate chunkSize
   * is freed. Otherwise returns false;
   * TODO OFFHEAP: what should be done about contiguous chunks that end up being bigger than 2G?
   * Currently if we are given slabs bigger than 2G or that just happen to be contiguous and add
   * up to 2G then the compactor may unify them together into a single Chunk and our 32-bit chunkSize
   * field will overflow. This code needs to detect this and just create a chunk of 2G and then start
   * a new one.
   * Or to prevent it from happening we could just check the incoming slabs and throw away a few bytes
   * to keep them from being contiguous.
   */
  boolean compact(int chunkSize) {
    final long startCompactionTime = this.ma.getStats().startCompaction();
    final int countPreSync = this.compactCount.get();
    afterCompactCountFetched();
    try {
      synchronized (this) {
        if (this.compactCount.get() != countPreSync) {
          // someone else did a compaction while we waited on the sync.
          // So just return true causing the caller to retry the allocation.
          return true;
        }
        ArrayList<SyncChunkStack> freeChunks = new ArrayList<SyncChunkStack>();
        collectFreeChunks(freeChunks);
        final int SORT_ARRAY_BLOCK_SIZE = 128;
        long[] sorted = new long[SORT_ARRAY_BLOCK_SIZE];
        int sortedSize = 0;
        boolean result = false;
        int largestFragment = 0;
        for (SyncChunkStack l: freeChunks) {
          long addr = l.poll();
          while (addr != 0) {
            int idx = Arrays.binarySearch(sorted, 0, sortedSize, addr);
            idx = -idx;
            idx--;
            if (idx == sortedSize) {
              // addr is > everything in the array
              if (sortedSize == 0) {
                // nothing was in the array
                sorted[0] = addr;
                sortedSize++;
              } else {
                // see if we can conflate into sorted[idx]
                long lowAddr = sorted[idx-1];
                int lowSize = ObjectChunk.getSize(lowAddr);
                if (lowAddr + lowSize == addr) {
                  // append the addr chunk to lowAddr
                  ObjectChunk.setSize(lowAddr, lowSize + ObjectChunk.getSize(addr));
                } else {
                  if (sortedSize >= sorted.length) {
                    long[] newSorted = new long[sorted.length+SORT_ARRAY_BLOCK_SIZE];
                    System.arraycopy(sorted, 0, newSorted, 0, sorted.length);
                    sorted = newSorted;
                  }
                  sortedSize++;
                  sorted[idx] = addr;
                }
              }
            } else {
              int addrSize = ObjectChunk.getSize(addr);
              long highAddr = sorted[idx];
              if (addr + addrSize == highAddr) {
                // append highAddr chunk to addr
                ObjectChunk.setSize(addr, addrSize + ObjectChunk.getSize(highAddr));
                sorted[idx] = addr;
              } else {
                boolean insert = idx==0;
                if (!insert) {
                  long lowAddr = sorted[idx-1];
                  //                  if (lowAddr == 0L) {
                  //                    long[] tmp = Arrays.copyOf(sorted, sortedSize);
                  //                    throw new IllegalStateException("addr was zero at idx=" + (idx-1) + " sorted="+ Arrays.toString(tmp));
                  //                  }
                  int lowSize = ObjectChunk.getSize(lowAddr);
                  if (lowAddr + lowSize == addr) {
                    // append the addr chunk to lowAddr
                    ObjectChunk.setSize(lowAddr, lowSize + addrSize);
                  } else {
                    insert = true;
                  }
                }
                if (insert) {
                  if (sortedSize >= sorted.length) {
                    long[] newSorted = new long[sorted.length+SORT_ARRAY_BLOCK_SIZE];
                    System.arraycopy(sorted, 0, newSorted, 0, idx);
                    newSorted[idx] = addr;
                    System.arraycopy(sorted, idx, newSorted, idx+1, sortedSize-idx);
                    sorted = newSorted;
                  } else {
                    System.arraycopy(sorted, idx, sorted, idx+1, sortedSize-idx);
                    sorted[idx] = addr;
                  }
                  sortedSize++;
                }
              }
            }
            addr = l.poll();
          }
        }
        for (int i=sortedSize-1; i > 0; i--) {
          long addr = sorted[i];
          long lowAddr = sorted[i-1];
          int lowSize = ObjectChunk.getSize(lowAddr);
          if (lowAddr + lowSize == addr) {
            // append addr chunk to lowAddr
            ObjectChunk.setSize(lowAddr, lowSize + ObjectChunk.getSize(addr));
            sorted[i] = 0L;
          }
        }
        this.lastFragmentAllocation.set(0);
        ArrayList<Fragment> tmp = new ArrayList<Fragment>();
        for (int i=sortedSize-1; i >= 0; i--) {
          long addr = sorted[i];
          if (addr == 0L) continue;
          int addrSize = ObjectChunk.getSize(addr);
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

        // Signal any waiters that a compaction happened.
        this.compactCount.incrementAndGet();

        this.ma.getStats().setLargestFragment(largestFragment);
        this.ma.getStats().setFragments(tmp.size());        
        updateFragmentation(largestFragment);

        return result;
      } // sync
    } finally {
      this.ma.getStats().endCompaction(startCompactionTime);
    }
  }

  /**
   * Unit tests override this method to get better test coverage
   */
  protected void afterCompactCountFetched() {
  }
  
  static void verifyOffHeapAlignment(int tinyMultiple) {
    if (tinyMultiple <= 0 || (tinyMultiple & 3) != 0) {
      throw new IllegalStateException("gemfire.OFF_HEAP_ALIGNMENT must be a multiple of 8.");
    }
    if (tinyMultiple > 256) {
      // this restriction exists because of the dataSize field in the object header.
      throw new IllegalStateException("gemfire.OFF_HEAP_ALIGNMENT must be <= 256 and a multiple of 8.");
    }
  }
  static void verifyOffHeapFreeListCount(int tinyFreeListCount) {
    if (tinyFreeListCount <= 0) {
      throw new IllegalStateException("gemfire.OFF_HEAP_FREE_LIST_COUNT must be >= 1.");
    }
  }
  static void verifyHugeMultiple(int hugeMultiple) {
    if (hugeMultiple > 256 || hugeMultiple < 0) {
      // this restriction exists because of the dataSize field in the object header.
      throw new IllegalStateException("HUGE_MULTIPLE must be >= 0 and <= 256 but it was " + hugeMultiple);
    }
  }
  
  private void updateFragmentation(long largestFragment) {      
    long freeSize = getFreeMemory();

    // Calculate free space fragmentation only if there is free space available.
    if(freeSize > 0) {
      long numerator = freeSize - largestFragment;

      double percentage = (double) numerator / (double) freeSize;
      percentage *= 100d;

      int wholePercentage = (int) Math.rint(percentage);
      this.ma.getStats().setFragmentation(wholePercentage);
    } else {
      // No free space? Then we have no free space fragmentation.
      this.ma.getStats().setFragmentation(0);
    }
  }

  private void collectFreeChunks(List<SyncChunkStack> l) {
    collectFreeFragmentChunks(l);
    collectFreeHugeChunks(l);
    collectFreeTinyChunks(l);
  }
  List<Fragment> getFragmentList() {
    return this.fragmentList;
  }
  private void collectFreeFragmentChunks(List<SyncChunkStack> l) {
    if (this.fragmentList.size() == 0) return;
    SyncChunkStack result = new SyncChunkStack();
    for (Fragment f: this.fragmentList) {
      int offset;
      int diff;
      do {
        offset = f.getFreeIndex();
        diff = f.getSize() - offset;
      } while (diff >= ObjectChunk.MIN_CHUNK_SIZE && !f.allocate(offset, offset+diff));
      if (diff < ObjectChunk.MIN_CHUNK_SIZE) {
        // If diff > 0 then that memory will be lost during compaction.
        // This should never happen since we keep the sizes rounded
        // based on MIN_CHUNK_SIZE.
        assert diff == 0;
        // The current fragment is completely allocated so just skip it.
        continue;
      }
      long chunkAddr = f.getMemoryAddress()+offset;
      ObjectChunk.setSize(chunkAddr, diff);
      result.offer(chunkAddr);
    }
    // All the fragments have been turned in to chunks so now clear them
    // The compaction will create new fragments.
    this.fragmentList.clear();
    if (!result.isEmpty()) {
      l.add(result);
    }
  }
  private void collectFreeTinyChunks(List<SyncChunkStack> l) {
    for (int i=0; i < this.tinyFreeLists.length(); i++) {
      SyncChunkStack cl = this.tinyFreeLists.get(i);
      if (cl != null) {
        long head = cl.clear();
        if (head != 0L) {
          l.add(new SyncChunkStack(head));
        }
      }
    }
  }
  private void collectFreeHugeChunks(List<SyncChunkStack> l) {
    ObjectChunk c = this.hugeChunkSet.pollFirst();
    SyncChunkStack result = null;
    while (c != null) {
      if (result == null) {
        result = new SyncChunkStack();
        l.add(result);
      }
      result.offer(c.getMemoryAddress());
      c = this.hugeChunkSet.pollFirst();
    }
  }

  ObjectChunk allocateFromFragment(final int fragIdx, final int chunkSize) {
    if (fragIdx >= this.fragmentList.size()) return null;
    final Fragment fragment;
    try {
      fragment = this.fragmentList.get(fragIdx);
    } catch (IndexOutOfBoundsException ignore) {
      // A concurrent compaction can cause this.
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
        if (extraSize < ObjectChunk.MIN_CHUNK_SIZE) {
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
          ObjectChunk result = new ObjectChunk(fragment.getMemoryAddress()+oldOffset, chunkSize+extraSize);
          checkDataIntegrity(result);
          return result;
        } else {
          ObjectChunk result = basicAllocate(chunkSize, false);
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
    return (int) ((((long)value + (multiple-1)) / multiple) * multiple);
  }
  private ObjectChunk allocateTiny(int size, boolean useFragments) {
    return basicAllocate(getNearestTinyMultiple(size), TINY_MULTIPLE, 0, this.tinyFreeLists, useFragments);
  }
  private ObjectChunk basicAllocate(int idx, int multiple, int offset, AtomicReferenceArray<SyncChunkStack> freeLists, boolean useFragments) {
    SyncChunkStack clq = freeLists.get(idx);
    if (clq != null) {
      long memAddr = clq.poll();
      if (memAddr != 0) {
        ObjectChunk result = new ObjectChunk(memAddr);
        checkDataIntegrity(result);
        result.readyForAllocation();
        return result;
      }
    }
    if (useFragments) {
      return allocateFromFragments(((idx+1)*multiple)+offset);
    } else {
      return null;
    }
  }
  private ObjectChunk allocateHuge(int size, boolean useFragments) {
    // sizeHolder is a fake Chunk used to search our sorted hugeChunkSet.
    ObjectChunk sizeHolder = new FakeChunk(size);
    NavigableSet<ObjectChunk> ts = this.hugeChunkSet.tailSet(sizeHolder);
    ObjectChunk result = ts.pollFirst();
    if (result != null) {
      if (result.getSize() - (HUGE_MULTIPLE - ObjectChunk.OFF_HEAP_HEADER_SIZE) < size) {
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
  
  private void checkDataIntegrity(ObjectChunk data) {
    if (this.validateMemoryWithFill) {
      data.validateFill();
    }
  }
  /**
   * Used by the FreeListManager to easily search its
   * ConcurrentSkipListSet. This is not a real chunk
   * but only used for searching.
   */
  private static class FakeChunk extends ObjectChunk {
    private final int size;
    public FakeChunk(int size) {
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
      ObjectChunk.fill(addr);
    }
    
    free(addr, true);
  }

  private void free(long addr, boolean updateStats) {
    int cSize = ObjectChunk.getSize(addr);
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
  private void basicFree(long addr, int idx, AtomicReferenceArray<SyncChunkStack> freeLists) {
    SyncChunkStack clq = freeLists.get(idx);
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
  protected SyncChunkStack createFreeListForEmptySlot(AtomicReferenceArray<SyncChunkStack> freeLists, int idx) {
    return new SyncChunkStack();
  }
  
  private void freeHuge(long addr, int cSize) {
    this.hugeChunkSet.add(new ObjectChunk(addr)); // TODO make this a collection of longs
  }

  List<MemoryBlock> getOrderedBlocks() {
    final List<MemoryBlock> value = new ArrayList<MemoryBlock>();
    addBlocksFromFragments(this.fragmentList, value); // unused fragments
    addBlocksFromChunks(getLiveChunks(), value); // used chunks
    addBlocksFromChunks(this.hugeChunkSet, value);    // huge free chunks
    addMemoryBlocks(getTinyFreeBlocks(), value);           // tiny free chunks
    Collections.sort(value, 
        new Comparator<MemoryBlock>() {
          @Override
          public int compare(MemoryBlock o1, MemoryBlock o2) {
            return Long.valueOf(o1.getMemoryAddress()).compareTo(o2.getMemoryAddress());
          }
    });
    return value;
  }
  private void addBlocksFromFragments(Collection<Fragment> src, List<MemoryBlock> dest) {
    for (MemoryBlock block : src) {
      dest.add(new MemoryBlockNode(this.ma, block));
    }
  }
  
  private void addBlocksFromChunks(Collection<ObjectChunk> src, List<MemoryBlock> dest) {
    for (ObjectChunk chunk : src) {
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
    final SimpleMemoryAllocatorImpl sma = this.ma;
    for (int i = 0; i < this.tinyFreeLists.length(); i++) {
      if (this.tinyFreeLists.get(i) == null) continue;
      long addr = this.tinyFreeLists.get(i).getTopAddress();
      while (addr != 0L) {
        value.add(new MemoryBlockNode(sma, new TinyMemoryBlock(addr, i)));
        addr = ObjectChunk.getNext(addr);
      }
    }
    return value;
  }
  List<MemoryBlock> getAllocatedBlocks() {
    final List<MemoryBlock> value = new ArrayList<MemoryBlock>();
    addBlocksFromChunks(getLiveChunks(), value); // used chunks
    Collections.sort(value, 
        new Comparator<MemoryBlock>() {
          @Override
          public int compare(MemoryBlock o1, MemoryBlock o2) {
            return Long.valueOf(o1.getMemoryAddress()).compareTo(o2.getMemoryAddress());
          }
    });
    return value;
  }
  /**
   * Used to represent an address from a tiny free list as a MemoryBlock
   */
  private static final class TinyMemoryBlock implements MemoryBlock {
    private final long address;
    private final int freeListId;

    private TinyMemoryBlock(long address, int freeListId) {
      this.address = address;
      this.freeListId = freeListId;
    }

    @Override
    public State getState() {
      return State.DEALLOCATED;
    }

    @Override
    public long getMemoryAddress() {
      return address;
    }

    @Override
    public int getBlockSize() {
      return ObjectChunk.getSize(address);
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
        return getMemoryAddress() == ((TinyMemoryBlock) o).getMemoryAddress();
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      long value = this.getMemoryAddress();
      return (int)(value ^ (value >>> 32));
    }
  }

  long getTotalMemory() {
    return this.totalSlabSize;
  }
  
  void freeSlabs() {
    for (int i=0; i < slabs.length; i++) {
      slabs[i].release();
    }
  }
  /**
   * newSlabs will be non-null in unit tests.
   * If the unit test gave us a different array
   * of slabs then something is wrong because we
   * are trying to reuse the old already allocated
   * array which means that the new one will never
   * be used. Note that this code does not bother
   * comparing the contents of the arrays.
   */
  boolean okToReuse(AddressableMemoryChunk[] newSlabs) {
    return newSlabs == null || newSlabs == this.slabs;
  }
  
  int getLargestSlabSize() {
    return this.slabs[0].getSize();
  }
  int findSlab(long addr) {
    for (int i=0; i < this.slabs.length; i++) {
      AddressableMemoryChunk slab = this.slabs[i];
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
    for (int i=0; i < slabs.length; i++) {
      long startAddr = slabs[i].getMemoryAddress();
      long endAddr = startAddr + slabs[i].getSize();
      sb.append("[").append(Long.toString(startAddr, 16)).append("..").append(Long.toString(endAddr, 16)).append("] ");
    }
  }
  boolean validateAddressAndSizeWithinSlab(long addr, int size) {
    for (int i=0; i < slabs.length; i++) {
      if (slabs[i].getMemoryAddress() <= addr && addr < (slabs[i].getMemoryAddress() + slabs[i].getSize())) {
        // validate addr + size is within the same slab
        if (size != -1) { // skip this check if size is -1
          if (!(slabs[i].getMemoryAddress() <= (addr+size-1) && (addr+size-1) < (slabs[i].getMemoryAddress() + slabs[i].getSize()))) {
            throw new IllegalStateException(" address 0x" + Long.toString(addr+size-1, 16) + " does not address the original slab memory");
          }
        }
        return true;
      }
    }
    return false;
  }
  
}