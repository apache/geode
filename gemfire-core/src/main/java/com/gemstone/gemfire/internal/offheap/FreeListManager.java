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
import com.gemstone.gemfire.internal.offheap.MemoryBlock.State;

/**
 * Manages the free lists for a SimpleMemoryAllocatorImpl
 */
public class FreeListManager {
  final private AtomicReferenceArray<SyncChunkStack> tinyFreeLists = new AtomicReferenceArray<SyncChunkStack>(SimpleMemoryAllocatorImpl.TINY_FREE_LIST_COUNT);
  // hugeChunkSet is sorted by chunk size in ascending order. It will only contain chunks larger than MAX_TINY.
  private final ConcurrentSkipListSet<Chunk> hugeChunkSet = new ConcurrentSkipListSet<Chunk>();
  private final AtomicLong allocatedSize = new AtomicLong(0L);

  private int getNearestTinyMultiple(int size) {
    return (size-1)/SimpleMemoryAllocatorImpl.TINY_MULTIPLE;
  }
  List<Chunk> getLiveChunks() {
    ArrayList<Chunk> result = new ArrayList<Chunk>();
    UnsafeMemoryChunk[] slabs = this.ma.getSlabs();
    for (int i=0; i < slabs.length; i++) {
      getLiveChunks(slabs[i], result);
    }
    return result;
  }
  private void getLiveChunks(UnsafeMemoryChunk slab, List<Chunk> result) {
    long addr = slab.getMemoryAddress();
    while (addr <= (slab.getMemoryAddress() + slab.getSize() - Chunk.MIN_CHUNK_SIZE)) {
      Fragment f = isAddrInFragmentFreeSpace(addr);
      if (f != null) {
        addr = f.getMemoryAddress() + f.getSize();
      } else {
        int curChunkSize = Chunk.getSize(addr);
        int refCount = Chunk.getRefCount(addr);
        if (refCount > 0) {
          result.add(this.ma.chunkFactory.newChunk(addr));
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
    return this.ma.getTotalMemory() - getUsedMemory();
  }
  long getFreeFragmentMemory() {
    long result = 0;
    for (Fragment f: this.fragmentList) {
      int freeSpace = f.freeSpace();
      if (freeSpace >= Chunk.MIN_CHUNK_SIZE) {
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
    for (Chunk c: this.hugeChunkSet) {
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

  public FreeListManager(SimpleMemoryAllocatorImpl ma) {
    this.ma = ma;
    UnsafeMemoryChunk[] slabs = ma.getSlabs();
    Fragment[] tmp = new Fragment[slabs.length];
    for (int i=0; i < slabs.length; i++) {
      tmp[i] = new Fragment(slabs[i].getMemoryAddress(), slabs[i].getSize());
    }
    this.fragmentList = new CopyOnWriteArrayList<Fragment>(tmp);

    if(ma.validateMemoryWithFill) {
      fillFragments();
    }
  }

  /**
   * Fills all fragments with a fill used for data integrity validation.
   */
  private void fillFragments() {
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
   * @param chunkType TODO
   * @return the allocated chunk
   * @throws IllegalStateException if a chunk can not be allocated.
   */
  @SuppressWarnings("synthetic-access")
  public Chunk allocate(int size, ChunkType chunkType) {
    Chunk result = null;
    {
      assert size > 0;
      if (chunkType == null) {
        chunkType = GemFireChunk.TYPE;
      }
      result = basicAllocate(size, true, chunkType);
      result.setDataSize(size);
    }
    this.ma.stats.incObjects(1);
    int resultSize = result.getSize();
    this.allocatedSize.addAndGet(resultSize);
    this.ma.stats.incUsedMemory(resultSize);
    this.ma.stats.incFreeMemory(-resultSize);
    result.initializeUseCount();
    this.ma.notifyListeners();

    return result;
  }

  private Chunk basicAllocate(int size, boolean useSlabs, ChunkType chunkType) {
    if (useSlabs) {
      // Every object stored off heap has a header so we need
      // to adjust the size so that the header gets allocated.
      // If useSlabs is false then the incoming size has already
      // been adjusted.
      size += Chunk.OFF_HEAP_HEADER_SIZE;
    }
    if (size <= SimpleMemoryAllocatorImpl.MAX_TINY) {
      return allocateTiny(size, useSlabs, chunkType);
    } else {
      return allocateHuge(size, useSlabs, chunkType);
    }
  }

  private Chunk allocateFromFragments(int chunkSize, ChunkType chunkType) {
    do {
      final int lastAllocationId = this.lastFragmentAllocation.get();
      for (int i=lastAllocationId; i < this.fragmentList.size(); i++) {
        Chunk result = allocateFromFragment(i, chunkSize, chunkType);
        if (result != null) {
          return result;
        }
      }
      for (int i=0; i < lastAllocationId; i++) {
        Chunk result = allocateFromFragment(i, chunkSize, chunkType);
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
      this.ma.ooohml.outOfOffHeapMemory(failure);
    }
  }

  private void logOffHeapState(int chunkSize) {
    if (InternalDistributedSystem.getAnyInstance() != null) {
      LogWriter lw = InternalDistributedSystem.getAnyInstance().getLogWriter();
      lw.info("OutOfOffHeapMemory allocating size of " + chunkSize + ". allocated=" + this.allocatedSize.get() + " compactions=" + this.compactCount.get() + " objects=" + this.ma.stats.getObjects() + " free=" + this.ma.stats.getFreeMemory() + " fragments=" + this.ma.stats.getFragments() + " largestFragment=" + this.ma.stats.getLargestFragment() + " fragmentation=" + this.ma.stats.getFragmentation());
      logFragmentState(lw);
      logTinyState(lw);
      logHugeState(lw);
    }
  }

  private void logHugeState(LogWriter lw) {
    for (Chunk c: this.hugeChunkSet) {
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

  private final AtomicInteger compactCount = new AtomicInteger();
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
  private boolean compact(int chunkSize) {
    final long startCompactionTime = this.ma.getStats().startCompaction();
    final int countPreSync = this.compactCount.get();
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
            //System.out.println("DEBUG addr=" + addr + " size=" + Chunk.getSize(addr) + " idx="+idx + " sortedSize=" + sortedSize);
            if (idx >= 0) {
              throw new IllegalStateException("duplicate memory address found during compaction!");
            }
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
                int lowSize = Chunk.getSize(lowAddr);
                if (lowAddr + lowSize == addr) {
                  // append the addr chunk to lowAddr
                  Chunk.setSize(lowAddr, lowSize + Chunk.getSize(addr));
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
              int addrSize = Chunk.getSize(addr);
              long highAddr = sorted[idx];
              if (addr + addrSize == highAddr) {
                // append highAddr chunk to addr
                Chunk.setSize(addr, addrSize + Chunk.getSize(highAddr));
                sorted[idx] = addr;
              } else {
                boolean insert = idx==0;
                if (!insert) {
                  long lowAddr = sorted[idx-1];
                  //                  if (lowAddr == 0L) {
                  //                    long[] tmp = Arrays.copyOf(sorted, sortedSize);
                  //                    throw new IllegalStateException("addr was zero at idx=" + (idx-1) + " sorted="+ Arrays.toString(tmp));
                  //                  }
                  int lowSize = Chunk.getSize(lowAddr);
                  if (lowAddr + lowSize == addr) {
                    // append the addr chunk to lowAddr
                    Chunk.setSize(lowAddr, lowSize + addrSize);
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
          int lowSize = Chunk.getSize(lowAddr);
          if (lowAddr + lowSize == addr) {
            // append addr chunk to lowAddr
            Chunk.setSize(lowAddr, lowSize + Chunk.getSize(addr));
            sorted[i] = 0L;
          }
        }
        this.lastFragmentAllocation.set(0);
        ArrayList<Fragment> tmp = new ArrayList<Fragment>();
        for (int i=sortedSize-1; i >= 0; i--) {
          long addr = sorted[i];
          if (addr == 0L) continue;
          int addrSize = Chunk.getSize(addr);
          Fragment f = new Fragment(addr, addrSize);
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

        // Reinitialize fragments with fill pattern data
        if(this.ma.validateMemoryWithFill) {
          fillFragments();
        }

        // Signal any waiters that a compaction happened.
        this.compactCount.incrementAndGet();

        this.ma.getStats().setLargestFragment(largestFragment);
        this.ma.getStats().setFragments(tmp.size());        
        updateFragmentation();

        return result;
      } // sync
    } finally {
      this.ma.getStats().endCompaction(startCompactionTime);
    }
  }

  private void updateFragmentation() {      
    long freeSize = this.ma.getStats().getFreeMemory();

    // Calculate free space fragmentation only if there is free space available.
    if(freeSize > 0) {
      long largestFragment = this.ma.getStats().getLargestFragment();
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
  private void collectFreeFragmentChunks(List<SyncChunkStack> l) {
    if (this.fragmentList.size() == 0) return;
    SyncChunkStack result = new SyncChunkStack();
    for (Fragment f: this.fragmentList) {
      int offset;
      int diff;
      do {
        offset = f.getFreeIndex();
        diff = f.getSize() - offset;
      } while (diff >= Chunk.MIN_CHUNK_SIZE && !f.allocate(offset, offset+diff));
      if (diff < Chunk.MIN_CHUNK_SIZE) {
        if (diff > 0) {
          SimpleMemoryAllocatorImpl.logger.debug("Lost memory of size {}", diff);
        }
        // fragment is too small to turn into a chunk
        // TODO we need to make sure this never happens
        // by keeping sizes rounded. I think I did this
        // by introducing MIN_CHUNK_SIZE and by rounding
        // the size of huge allocations.
        continue;
      }
      long chunkAddr = f.getMemoryAddress()+offset;
      Chunk.setSize(chunkAddr, diff);
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
    Chunk c = this.hugeChunkSet.pollFirst();
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

  private Chunk allocateFromFragment(final int fragIdx, final int chunkSize, ChunkType chunkType) {
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
        // Try to allocate up to BATCH_SIZE more chunks from it
        int allocSize = chunkSize * SimpleMemoryAllocatorImpl.BATCH_SIZE;
        if (allocSize > fragmentFreeSize) {
          allocSize = (fragmentFreeSize / chunkSize) * chunkSize;
        }
        int newOffset = oldOffset + allocSize;
        int extraSize = fragmentSize - newOffset;
        if (extraSize < Chunk.MIN_CHUNK_SIZE) {
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
          Chunk result = this.ma.chunkFactory.newChunk(fragment.getMemoryAddress()+oldOffset, chunkSize+extraSize, chunkType);
          allocSize -= chunkSize+extraSize;
          oldOffset += extraSize;
          while (allocSize > 0) {
            oldOffset += chunkSize;
            // we add the batch ones immediately to the freelist
            result.readyForFree();
            free(result.getMemoryAddress(), false);
            result = this.ma.chunkFactory.newChunk(fragment.getMemoryAddress()+oldOffset, chunkSize, chunkType);
            allocSize -= chunkSize;
          }

          if(this.ma.validateMemoryWithFill) {
            result.validateFill();
          }

          return result;
        } else {
          // TODO OFFHEAP: if batch allocations are disabled should we not call basicAllocate here?
          // Since we know another thread did a concurrent alloc
          // that possibly did a batch check the free list again.
          Chunk result = basicAllocate(chunkSize, false, chunkType);
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
  private Chunk allocateTiny(int size, boolean useFragments, ChunkType chunkType) {
    return basicAllocate(getNearestTinyMultiple(size), SimpleMemoryAllocatorImpl.TINY_MULTIPLE, 0, this.tinyFreeLists, useFragments, chunkType);
  }
  private Chunk basicAllocate(int idx, int multiple, int offset, AtomicReferenceArray<SyncChunkStack> freeLists, boolean useFragments, ChunkType chunkType) {
    SyncChunkStack clq = freeLists.get(idx);
    if (clq != null) {
      long memAddr = clq.poll();
      if (memAddr != 0) {
        Chunk result = this.ma.chunkFactory.newChunk(memAddr, chunkType);

        // Data integrity check.
        if(this.ma.validateMemoryWithFill) {          
          result.validateFill();
        }

        result.readyForAllocation(chunkType);
        return result;
      }
    }
    if (useFragments) {
      return allocateFromFragments(((idx+1)*multiple)+offset, chunkType);
    } else {
      return null;
    }
  }
  private Chunk allocateHuge(int size, boolean useFragments, ChunkType chunkType) {
    // sizeHolder is a fake Chunk used to search our sorted hugeChunkSet.
    Chunk sizeHolder = new FakeChunk(size);
    NavigableSet<Chunk> ts = this.hugeChunkSet.tailSet(sizeHolder);
    Chunk result = ts.pollFirst();
    if (result != null) {
      if (result.getSize() - (SimpleMemoryAllocatorImpl.HUGE_MULTIPLE - Chunk.OFF_HEAP_HEADER_SIZE) < size) {
        // close enough to the requested size; just return it.

        // Data integrity check.
        if(this.ma.validateMemoryWithFill) {          
          result.validateFill();
        }
        if (chunkType.getSrcType() != Chunk.getSrcType(result.getMemoryAddress())) {
          // The java wrapper class that was cached in the huge chunk list is the wrong type.
          // So allocate a new one and garbage collect the old one.
          result = this.ma.chunkFactory.newChunk(result.getMemoryAddress(), chunkType);
        }
        result.readyForAllocation(chunkType);
        return result;
      } else {
        this.hugeChunkSet.add(result);
      }
    }
    if (useFragments) {
      // We round it up to the next multiple of TINY_MULTIPLE to make
      // sure we always have chunks allocated on an 8 byte boundary.
      return allocateFromFragments(round(SimpleMemoryAllocatorImpl.TINY_MULTIPLE, size), chunkType);
    } else {
      return null;
    }
  }
  
  /**
   * Used by the FreeListManager to easily search its
   * ConcurrentSkipListSet. This is not a real chunk
   * but only used for searching.
   */
  private static class FakeChunk extends Chunk {
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
    free(addr, true);
  }

  private void free(long addr, boolean updateStats) {
    int cSize = Chunk.getSize(addr);
    if (updateStats) {
      this.ma.stats.incObjects(-1);
      this.allocatedSize.addAndGet(-cSize);
      this.ma.stats.incUsedMemory(-cSize);
      this.ma.stats.incFreeMemory(cSize);
      this.ma.notifyListeners();
    }
    if (cSize <= SimpleMemoryAllocatorImpl.MAX_TINY) {
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
      clq = new SyncChunkStack();
      clq.offer(addr);
      if (!freeLists.compareAndSet(idx, null, clq)) {
        clq = freeLists.get(idx);
        clq.offer(addr);
      }
    }

  }
  private void freeHuge(long addr, int cSize) {
    this.hugeChunkSet.add(this.ma.chunkFactory.newChunk(addr)); // TODO make this a collection of longs
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
  
  private void addBlocksFromChunks(Collection<Chunk> src, List<MemoryBlock> dest) {
    for (Chunk chunk : src) {
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
        addr = Chunk.getNext(addr);
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
      return Chunk.getSize(address);
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
    public ChunkType getChunkType() {
      return null;
    }
  }
}