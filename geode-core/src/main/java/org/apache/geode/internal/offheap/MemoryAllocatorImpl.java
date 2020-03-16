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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.offheap.annotations.OffHeapIdentifier;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This allocator is somewhat like an Arena allocator. We start out with an array of multiple large
 * chunks of memory. We also keep lists of any chunk that have been allocated and freed. An
 * allocation will always try to find a chunk in a free list that is a close fit to the requested
 * size. If no close fits exist then it allocates the next slice from the front of one the original
 * large chunks. If we can not find enough free memory then all the existing free memory is
 * defragmented. If we still do not have enough to make the allocation an exception is thrown.
 *
 * @since Geode 1.0
 */
public class MemoryAllocatorImpl implements MemoryAllocator {

  static final Logger logger = LogService.getLogger();

  public static final String FREE_OFF_HEAP_MEMORY_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "free-off-heap-memory";

  private volatile OffHeapMemoryStats stats;

  private volatile OutOfOffHeapMemoryListener ooohml;

  OutOfOffHeapMemoryListener getOutOfOffHeapMemoryListener() {
    return this.ooohml;
  }

  public final FreeListManager freeList;

  private MemoryInspector memoryInspector;

  private volatile MemoryUsageListener[] memoryUsageListeners = new MemoryUsageListener[0];

  @MakeNotStatic
  private static MemoryAllocatorImpl singleton = null;

  public static MemoryAllocatorImpl getAllocator() {
    MemoryAllocatorImpl result = singleton;
    if (result == null) {
      throw new CacheClosedException("Off Heap memory allocator does not exist.");
    }
    return result;
  }

  private static final boolean DO_EXPENSIVE_VALIDATION =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_DO_EXPENSIVE_VALIDATION");

  public static MemoryAllocator create(OutOfOffHeapMemoryListener ooohml, OffHeapMemoryStats stats,
      int slabCount, long offHeapMemorySize, long maxSlabSize) {
    return create(ooohml, stats, slabCount, offHeapMemorySize, maxSlabSize, null,
        new SlabFactory() {
          @Override
          public Slab create(int size) {
            return new SlabImpl(size);
          }
        });
  }

  private static MemoryAllocatorImpl create(OutOfOffHeapMemoryListener ooohml,
      OffHeapMemoryStats stats, int slabCount, long offHeapMemorySize, long maxSlabSize,
      Slab[] slabs, SlabFactory slabFactory) {
    MemoryAllocatorImpl result = singleton;
    boolean created = false;
    try {
      if (result != null) {
        result.reuse(ooohml, stats, offHeapMemorySize, slabs);
        logger.info(
            "Reusing {}  bytes of off-heap memory. The maximum size of a single off-heap object is {}  bytes.",
            result.getTotalMemory(), result.freeList.getLargestSlabSize());
        created = true;
        LifecycleListener.invokeAfterReuse(result);
      } else {
        if (slabs == null) {
          // allocate memory chunks
          logger.info(
              "Allocating {} bytes of off-heap memory. The maximum size of a single off-heap object is {} bytes.",
              offHeapMemorySize, maxSlabSize);
          slabs = new SlabImpl[slabCount];
          long uncreatedMemory = offHeapMemorySize;
          for (int i = 0; i < slabCount; i++) {
            try {
              if (uncreatedMemory >= maxSlabSize) {
                slabs[i] = slabFactory.create((int) maxSlabSize);
                uncreatedMemory -= maxSlabSize;
              } else {
                // the last slab can be smaller then maxSlabSize
                slabs[i] = slabFactory.create((int) uncreatedMemory);
              }
            } catch (OutOfMemoryError err) {
              if (i > 0) {
                logger.error(
                    "Off-heap memory creation failed after successfully allocating {} bytes of off-heap memory.",
                    (i * maxSlabSize));
              }
              for (int j = 0; j < i; j++) {
                if (slabs[j] != null) {
                  slabs[j].free();
                }
              }
              throw err;
            }
          }
        }

        result = new MemoryAllocatorImpl(ooohml, stats, slabs);
        singleton = result;
        LifecycleListener.invokeAfterCreate(result);
        created = true;
      }
    } finally {
      if (!created) {
        if (stats != null) {
          stats.close();
        }
        if (ooohml != null) {
          ooohml.close();
        }
      }
    }
    return result;
  }

  static MemoryAllocatorImpl createForUnitTest(OutOfOffHeapMemoryListener ooohml,
      OffHeapMemoryStats stats, int slabCount, long offHeapMemorySize, long maxSlabSize,
      SlabFactory memChunkFactory) {
    return create(ooohml, stats, slabCount, offHeapMemorySize, maxSlabSize, null, memChunkFactory);
  }

  public static MemoryAllocatorImpl createForUnitTest(OutOfOffHeapMemoryListener oooml,
      OffHeapMemoryStats stats, Slab[] slabs) {
    int slabCount = 0;
    long offHeapMemorySize = 0;
    long maxSlabSize = 0;
    if (slabs != null) {
      slabCount = slabs.length;
      for (int i = 0; i < slabCount; i++) {
        int slabSize = slabs[i].getSize();
        offHeapMemorySize += slabSize;
        if (slabSize > maxSlabSize) {
          maxSlabSize = slabSize;
        }
      }
    }
    return create(oooml, stats, slabCount, offHeapMemorySize, maxSlabSize, slabs, null);
  }


  private void reuse(OutOfOffHeapMemoryListener oooml, OffHeapMemoryStats newStats,
      long offHeapMemorySize, Slab[] slabs) {
    if (isClosed()) {
      throw new IllegalStateException("Can not reuse a closed off-heap memory manager.");
    }
    if (oooml == null) {
      throw new IllegalArgumentException("OutOfOffHeapMemoryListener is null");
    }
    if (getTotalMemory() != offHeapMemorySize) {
      logger.warn("Using {} bytes of existing off-heap memory instead of the requested {}.",
          getTotalMemory(), offHeapMemorySize);
    }
    if (!this.freeList.okToReuse(slabs)) {
      throw new IllegalStateException(
          "attempted to reuse existing off-heap memory even though new off-heap memory was allocated");
    }
    this.ooohml = oooml;
    newStats.initialize(this.stats);
    this.stats = newStats;
  }

  private MemoryAllocatorImpl(final OutOfOffHeapMemoryListener oooml,
      final OffHeapMemoryStats stats, final Slab[] slabs) {
    if (oooml == null) {
      throw new IllegalArgumentException("OutOfOffHeapMemoryListener is null");
    }

    this.ooohml = oooml;
    this.stats = stats;

    this.stats.setFragments(slabs.length);
    this.stats.setLargestFragment(slabs[0].getSize());

    this.freeList = new FreeListManager(this, slabs);
    this.memoryInspector = new MemoryInspectorImpl(this.freeList);

    this.stats.incMaxMemory(this.freeList.getTotalMemory());
    this.stats.incFreeMemory(this.freeList.getTotalMemory());
  }

  public List<OffHeapStoredObject> getLostChunks(InternalCache cache) {
    List<OffHeapStoredObject> liveChunks = this.freeList.getLiveChunks();
    List<OffHeapStoredObject> regionChunks = getRegionLiveChunks(cache);
    Set<OffHeapStoredObject> liveChunksSet = new HashSet<>(liveChunks);
    Set<OffHeapStoredObject> regionChunksSet = new HashSet<>(regionChunks);
    liveChunksSet.removeAll(regionChunksSet);
    return new ArrayList<OffHeapStoredObject>(liveChunksSet);
  }

  /**
   * Returns a possibly empty list that contains all the Chunks used by regions.
   */
  private List<OffHeapStoredObject> getRegionLiveChunks(InternalCache cache) {
    ArrayList<OffHeapStoredObject> result = new ArrayList<OffHeapStoredObject>();
    if (cache != null) {
      Iterator<Region<?, ?>> rootIt = cache.rootRegions().iterator();
      while (rootIt.hasNext()) {
        Region<?, ?> rr = rootIt.next();
        getRegionLiveChunks(rr, result);
        Iterator<Region<?, ?>> srIt = rr.subregions(true).iterator();
        while (srIt.hasNext()) {
          getRegionLiveChunks(srIt.next(), result);
        }
      }
    }
    return result;
  }

  private void getRegionLiveChunks(Region<?, ?> r, List<OffHeapStoredObject> result) {
    if (r.getAttributes().getOffHeap()) {

      if (r instanceof PartitionedRegion) {
        PartitionedRegionDataStore prs = ((PartitionedRegion) r).getDataStore();
        if (prs != null) {
          Set<BucketRegion> brs = prs.getAllLocalBucketRegions();
          if (brs != null) {
            for (BucketRegion br : brs) {
              if (br != null && !br.isDestroyed()) {
                this.basicGetRegionLiveChunks(br, result);
              }

            }
          }
        }
      } else {
        this.basicGetRegionLiveChunks((InternalRegion) r, result);
      }

    }

  }

  private void basicGetRegionLiveChunks(InternalRegion r, List<OffHeapStoredObject> result) {
    for (Object key : r.keySet()) {
      RegionEntry re = r.getRegionEntry(key);
      if (re != null) {
        /*
         * value could be GATEWAY_SENDER_EVENT_IMPL_VALUE or region entry value.
         */
        @Unretained(OffHeapIdentifier.GATEWAY_SENDER_EVENT_IMPL_VALUE)
        Object value = re.getValue();
        if (value instanceof OffHeapStoredObject) {
          result.add((OffHeapStoredObject) value);
        }
      }
    }
  }

  private OffHeapStoredObject allocateOffHeapStoredObject(int size) {
    OffHeapStoredObject result = this.freeList.allocate(size);
    int resultSize = result.getSize();
    stats.incObjects(1);
    stats.incUsedMemory(resultSize);
    stats.incFreeMemory(-resultSize);
    notifyListeners();
    if (ReferenceCountHelper.trackReferenceCounts()) {
      ReferenceCountHelper.refCountChanged(result.getAddress(), false, 1);
    }
    return result;
  }

  @Override
  public StoredObject allocate(int size) {
    // System.out.println("allocating " + size);
    OffHeapStoredObject result = allocateOffHeapStoredObject(size);
    // ("allocated off heap object of size " + size + " @" +
    // Long.toHexString(result.getMemoryAddress()), true);
    return result;
  }

  public static void debugLog(String msg, boolean logStack) {
    if (logStack) {
      logger.info(msg, new RuntimeException(msg));
    } else {
      logger.info(msg);
    }
  }

  @Override
  public StoredObject allocateAndInitialize(byte[] v, boolean isSerialized, boolean isCompressed) {
    return allocateAndInitialize(v, isSerialized, isCompressed, null);
  }

  @Override
  public StoredObject allocateAndInitialize(byte[] v, boolean isSerialized, boolean isCompressed,
      byte[] originalHeapData) {
    long addr = OffHeapRegionEntryHelper.encodeDataAsAddress(v, isSerialized, isCompressed);
    if (addr != 0L) {
      return new TinyStoredObject(addr);
    }
    OffHeapStoredObject result = allocateOffHeapStoredObject(v.length);
    // debugLog("allocated off heap object of size " + v.length + " @" +
    // Long.toHexString(result.getMemoryAddress()), true);
    // debugLog("allocated off heap object of size " + v.length + " @" +
    // Long.toHexString(result.getMemoryAddress()) + "chunkSize=" + result.getSize() + "
    // isSerialized=" + isSerialized + " v=" + Arrays.toString(v), true);
    result.setSerializedValue(v);
    result.setSerialized(isSerialized);
    result.setCompressed(isCompressed);
    if (originalHeapData != null) {
      result = new OffHeapStoredObjectWithHeapForm(result, originalHeapData);
    }
    return result;
  }

  @Override
  public long getFreeMemory() {
    return this.freeList.getFreeMemory();
  }

  @Override
  public long getUsedMemory() {
    return this.freeList.getUsedMemory();
  }

  @Override
  public long getTotalMemory() {
    return this.freeList.getTotalMemory();
  }

  @Override
  public void close() {
    try {
      LifecycleListener.invokeBeforeClose(this);
    } finally {
      this.ooohml.close();
      if (Boolean.getBoolean(FREE_OFF_HEAP_MEMORY_PROPERTY)) {
        realClose();
      }
    }
  }

  public static void freeOffHeapMemory() {
    MemoryAllocatorImpl ma = singleton;
    if (ma != null) {
      ma.realClose();
    }
  }

  private void realClose() {
    // Removing this memory immediately can lead to a SEGV. See 47885.
    if (setClosed()) {
      this.freeList.freeSlabs();
      this.stats.close();
      singleton = null;
    }
  }

  private final AtomicBoolean closed = new AtomicBoolean();

  private boolean isClosed() {
    return this.closed.get();
  }

  /**
   * Returns true if caller is the one who should close; false if some other thread is already
   * closing.
   */
  private boolean setClosed() {
    return this.closed.compareAndSet(false, true);
  }


  FreeListManager getFreeListManager() {
    return this.freeList;
  }

  /**
   * Return the slabId of the slab that contains the given addr.
   */
  int findSlab(long addr) {
    return this.freeList.findSlab(addr);
  }

  @Override
  public OffHeapMemoryStats getStats() {
    return this.stats;
  }

  @Override
  public void addMemoryUsageListener(final MemoryUsageListener listener) {
    synchronized (this.memoryUsageListeners) {
      final MemoryUsageListener[] newMemoryUsageListeners =
          Arrays.copyOf(this.memoryUsageListeners, this.memoryUsageListeners.length + 1);
      newMemoryUsageListeners[this.memoryUsageListeners.length] = listener;
      this.memoryUsageListeners = newMemoryUsageListeners;
    }
  }

  @Override
  public void removeMemoryUsageListener(final MemoryUsageListener listener) {
    synchronized (this.memoryUsageListeners) {
      int listenerIndex = -1;
      for (int i = 0; i < this.memoryUsageListeners.length; i++) {
        if (this.memoryUsageListeners[i] == listener) {
          listenerIndex = i;
          break;
        }
      }

      if (listenerIndex != -1) {
        final MemoryUsageListener[] newMemoryUsageListeners =
            new MemoryUsageListener[this.memoryUsageListeners.length - 1];
        System.arraycopy(this.memoryUsageListeners, 0, newMemoryUsageListeners, 0, listenerIndex);
        System.arraycopy(this.memoryUsageListeners, listenerIndex + 1, newMemoryUsageListeners,
            listenerIndex, this.memoryUsageListeners.length - listenerIndex - 1);
        this.memoryUsageListeners = newMemoryUsageListeners;
      }
    }
  }

  void notifyListeners() {
    final MemoryUsageListener[] savedListeners = this.memoryUsageListeners;

    if (savedListeners.length == 0) {
      return;
    }

    final long bytesUsed = getUsedMemory();
    for (int i = 0; i < savedListeners.length; i++) {
      savedListeners[i].updateMemoryUsed(bytesUsed);
    }
  }

  static void validateAddress(long addr) {
    validateAddressAndSize(addr, -1);
  }

  static void validateAddressAndSize(long addr, int size) {
    // if the caller does not have a "size" to provide then use -1
    if ((addr & 7) != 0) {
      StringBuilder sb = new StringBuilder();
      sb.append("address was not 8 byte aligned: 0x").append(Long.toString(addr, 16));
      MemoryAllocatorImpl ma = MemoryAllocatorImpl.singleton;
      if (ma != null) {
        sb.append(". Valid addresses must be in one of the following ranges: ");
        ma.freeList.getSlabDescriptions(sb);
      }
      throw new IllegalStateException(sb.toString());
    }
    if (addr >= 0 && addr < 1024) {
      throw new IllegalStateException("addr was smaller than expected 0x" + addr);
    }
    validateAddressAndSizeWithinSlab(addr, size, DO_EXPENSIVE_VALIDATION);
  }

  static void validateAddressAndSizeWithinSlab(long addr, int size, boolean doExpensiveValidation) {
    if (doExpensiveValidation) {
      MemoryAllocatorImpl ma = MemoryAllocatorImpl.singleton;
      if (ma != null) {
        if (!ma.freeList.validateAddressAndSizeWithinSlab(addr, size)) {
          throw new IllegalStateException(" address 0x" + Long.toString(addr, 16)
              + " does not address the original slab memory");
        }
      }
    }
  }

  public synchronized List<MemoryBlock> getOrphans(InternalCache cache) {
    List<OffHeapStoredObject> liveChunks = this.freeList.getLiveChunks();
    List<OffHeapStoredObject> regionChunks = getRegionLiveChunks(cache);
    liveChunks.removeAll(regionChunks);
    List<MemoryBlock> orphans = new ArrayList<MemoryBlock>();
    for (OffHeapStoredObject chunk : liveChunks) {
      orphans.add(new MemoryBlockNode(this, chunk));
    }
    Collections.sort(orphans, new Comparator<MemoryBlock>() {
      @Override
      public int compare(MemoryBlock o1, MemoryBlock o2) {
        return Long.compare(o1.getAddress(), o2.getAddress());
      }
    });
    // this.memoryBlocks = new WeakReference<List<MemoryBlock>>(orphans);
    return orphans;
  }

  @Override
  public MemoryInspector getMemoryInspector() {
    return this.memoryInspector;
  }

}
