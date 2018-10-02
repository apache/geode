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
package org.apache.geode.internal.cache.eviction;

import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.QueueStatHelper;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryEvent;
import org.apache.geode.internal.cache.control.ResourceListener;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;

/**
 * Triggers centralized eviction(asynchronously) when the ResourceManager sends an eviction event
 * for on-heap regions. This is registered with the ResourceManager.
 *
 * @since GemFire 6.0
 *
 */
public class HeapEvictor implements ResourceListener<MemoryEvent> {
  private static final Logger logger = LogService.getLogger();

  // Add 1 for the management task that's putting more eviction tasks on the queue
  public static final int MAX_EVICTOR_THREADS =
      Integer.getInteger(GEMFIRE_PREFIX + "HeapLRUCapacityController.MAX_EVICTOR_THREADS",
          Runtime.getRuntime().availableProcessors() * 4) + 1;

  public static final boolean EVICT_HIGH_ENTRY_COUNT_BUCKETS_FIRST =
      Boolean.valueOf(System.getProperty(
          GEMFIRE_PREFIX + "HeapLRUCapacityController.evictHighEntryCountBucketsFirst", "true"));

  public static final int MINIMUM_ENTRIES_PER_BUCKET =
      Integer.getInteger(GEMFIRE_PREFIX + "HeapLRUCapacityController.inlineEvictionThreshold", 0);

  public static final int BUCKET_SORTING_INTERVAL = Integer.getInteger(
      GEMFIRE_PREFIX + "HeapLRUCapacityController.higherEntryCountBucketCalculationInterval", 100);

  private static final boolean DISABLE_HEAP_EVICTOR_THREAD_POOL = Boolean
      .getBoolean(GEMFIRE_PREFIX + "HeapLRUCapacityController.DISABLE_HEAP_EVICTOR_THREAD_POOL");

  private static final long TOTAL_BYTES_TO_EVICT_FROM_HEAP = setTotalBytesToEvictFromHeap();

  private static final String EVICTOR_THREAD_NAME = "EvictorThread";

  private final Object evictionLock = new Object();

  private final AtomicBoolean mustEvict = new AtomicBoolean(false);

  private final List<Integer> testTaskSetSizes = new ArrayList<>();

  private final ExecutorService evictorThreadPool;

  private final InternalCache cache;

  private final AtomicBoolean isRunning = new AtomicBoolean(true);

  private volatile int testAbortAfterLoopCount = Integer.MAX_VALUE;

  /*
   * Since the amount of memory used is to a large degree dependent upon when garbage collection is
   * run, it's difficult to determine when to stop evicting. So, an initial calculation is done to
   * determine the number of evictions that are likely needed in order to bring memory usage below
   * the eviction threshold. This number is stored in 'numFastLoops' and we quickly loop through
   * this number performing evictions. We then continue to evict, but at a progressively slower rate
   * waiting either for an event which indicates we've dropped below the eviction threshold or
   * another eviction event with an updated "number of bytes used". If we get another eviction event
   * with an updated "number of bytes used" then 'numFastLoops' is recalculated and we start over.
   */
  private volatile int numEvictionLoopsCompleted = 0;
  private volatile int numFastLoops;

  public HeapEvictor(final InternalCache cache) {
    this(cache, EVICTOR_THREAD_NAME);
  }

  public HeapEvictor(final InternalCache cache, final String threadName) {
    this.cache = cache;

    if (!DISABLE_HEAP_EVICTOR_THREAD_POOL) {
      QueueStatHelper poolStats = this.cache.getCachePerfStats().getEvictionQueueStatHelper();
      this.evictorThreadPool = LoggingExecutors.newFixedThreadPoolWithTimeout(threadName,
          MAX_EVICTOR_THREADS, 15, poolStats);
    } else {
      // disabled
      this.evictorThreadPool = null;
    }
  }

  protected InternalCache cache() {
    return this.cache;
  }

  protected boolean includePartitionedRegion(PartitionedRegion region) {
    return region.getEvictionAttributes().getAlgorithm().isLRUHeap()
        && region.getDataStore() != null && !region.getAttributes().getOffHeap();
  }

  protected boolean includeLocalRegion(LocalRegion region) {
    return region.getEvictionAttributes().getAlgorithm().isLRUHeap()
        && !region.getAttributes().getOffHeap();
  }

  private List<LocalRegion> getAllRegionList() {
    List<LocalRegion> allRegionsList = new ArrayList<>();
    InternalResourceManager resourceManager = (InternalResourceManager) cache.getResourceManager();

    for (ResourceListener<MemoryEvent> listener : resourceManager
        .getResourceListeners(getResourceType())) {
      if (listener instanceof PartitionedRegion) {
        PartitionedRegion partitionedRegion = (PartitionedRegion) listener;
        if (includePartitionedRegion(partitionedRegion)) {
          allRegionsList.addAll(partitionedRegion.getDataStore().getAllLocalBucketRegions());
        }
      } else if (listener instanceof LocalRegion) {
        LocalRegion lr = (LocalRegion) listener;
        if (includeLocalRegion(lr)) {
          allRegionsList.add(lr);
        }
      }
    }

    if (HeapEvictor.MINIMUM_ENTRIES_PER_BUCKET > 0) {
      Iterator<LocalRegion> iterator = allRegionsList.iterator();
      while (iterator.hasNext()) {
        LocalRegion region = iterator.next();
        if (region instanceof BucketRegion) {
          if (((BucketRegion) region)
              .getNumEntriesInVM() <= HeapEvictor.MINIMUM_ENTRIES_PER_BUCKET) {
            iterator.remove();
          }
        }
      }
    }
    return allRegionsList;
  }

  private List<LocalRegion> getAllSortedRegionList() {
    List<LocalRegion> allRegionList = getAllRegionList();

    // Capture the sizes so that they do not change while sorting
    final Object2LongOpenHashMap<LocalRegion> sizes =
        new Object2LongOpenHashMap<>(allRegionList.size());
    for (LocalRegion region : allRegionList) {
      long size = region instanceof BucketRegion ? ((BucketRegion) region).getSizeForEviction()
          : region.size();
      sizes.put(region, size);
    }

    // Sort with respect to other PR buckets also in case of multiple PRs
    allRegionList.sort((region1, region2) -> {
      long numEntries1 = sizes.get(region1);
      long numEntries2 = sizes.get(region2);
      if (numEntries1 > numEntries2) {
        return -1;
      } else if (numEntries1 < numEntries2) {
        return 1;
      }
      return 0;
    });
    return allRegionList;
  }

  private void executeInThreadPool(Runnable task) {
    try {
      evictorThreadPool.execute(task);
    } catch (RejectedExecutionException e) {
      // ignore rejection if evictor no longer running
      if (isRunning()) {
        throw e;
      }
    }
  }

  public ExecutorService getEvictorThreadPool() {
    if (isRunning()) {
      return evictorThreadPool;
    }
    return null;
  }

  private void createAndSubmitWeightedRegionEvictionTasks() {
    List<LocalRegion> allRegionList = getAllSortedRegionList();
    float numEntriesInVM = 0;
    for (LocalRegion region : allRegionList) {
      if (region instanceof BucketRegion) {
        numEntriesInVM += ((BucketRegion) region).getSizeForEviction();
      } else {
        numEntriesInVM += region.getRegionMap().sizeInVM();
      }
    }

    for (LocalRegion region : allRegionList) {
      float regionEntryCount;
      if (region instanceof BucketRegion) {
        regionEntryCount = ((BucketRegion) region).getSizeForEviction();
      } else {
        regionEntryCount = region.getRegionMap().sizeInVM();
      }

      float percentage = regionEntryCount / numEntriesInVM;
      long bytesToEvictPerTask = (long) (getTotalBytesToEvict() * percentage);
      List<LocalRegion> regionsForSingleTask = new ArrayList<>(1);
      regionsForSingleTask.add(region);
      if (mustEvict()) {
        executeInThreadPool(new RegionEvictorTask(cache.getCachePerfStats(), regionsForSingleTask,
            this, bytesToEvictPerTask));
      } else {
        break;
      }
    }
  }

  private Set<RegionEvictorTask> createRegionEvictionTasks() {
    if (getEvictorThreadPool() == null) {
      return Collections.emptySet();
    }

    int threadsAvailable = MAX_EVICTOR_THREADS;
    long bytesToEvictPerTask = getTotalBytesToEvict() / threadsAvailable;
    List<LocalRegion> allRegionList = getAllRegionList();
    if (allRegionList.isEmpty()) {
      return Collections.emptySet();
    }

    // This shuffling is not required when eviction triggered for the first time
    Collections.shuffle(allRegionList);
    int allRegionSetSize = allRegionList.size();
    Set<RegionEvictorTask> evictorTaskSet = new HashSet<>();
    if (allRegionSetSize <= threadsAvailable) {
      for (LocalRegion region : allRegionList) {
        List<LocalRegion> regionList = new ArrayList<>(1);
        regionList.add(region);
        RegionEvictorTask task =
            new RegionEvictorTask(cache.getCachePerfStats(), regionList, this, bytesToEvictPerTask);
        evictorTaskSet.add(task);
      }
      for (RegionEvictorTask regionEvictorTask : evictorTaskSet) {
        testTaskSetSizes.add(regionEvictorTask.getRegionList().size());
      }
      return evictorTaskSet;
    }

    int numRegionsInTask = allRegionSetSize / threadsAvailable;
    List<LocalRegion> regionsForSingleTask = null;
    Iterator<LocalRegion> regionIterator = allRegionList.iterator();
    for (int i = 0; i < threadsAvailable; i++) {
      regionsForSingleTask = new ArrayList<>(numRegionsInTask);
      int count = 1;
      while (count <= numRegionsInTask) {
        if (regionIterator.hasNext()) {
          regionsForSingleTask.add(regionIterator.next());
        }
        count++;
      }
      evictorTaskSet.add(new RegionEvictorTask(cache.getCachePerfStats(), regionsForSingleTask,
          this, bytesToEvictPerTask));
    }

    // Add leftover regions to last task
    while (regionIterator.hasNext()) {
      regionsForSingleTask.add(regionIterator.next());
    }

    for (RegionEvictorTask regionEvictorTask : evictorTaskSet) {
      testTaskSetSizes.add(regionEvictorTask.getRegionList().size());
    }
    return evictorTaskSet;
  }

  @Override
  public void onEvent(final MemoryEvent event) {
    if (DISABLE_HEAP_EVICTOR_THREAD_POOL) {
      return;
    }

    // Do we care about eviction events and did the eviction event originate
    // in this VM ...
    if (isRunning() && event.isLocal()) {
      if (event.getState().isEviction()) {
        // Have we previously received an eviction event and already started eviction ...
        if (this.mustEvict.get()) {
          if (logger.isDebugEnabled()) {
            logger.debug("Updating eviction in response to memory event: {}", event);
          }

          // We lock here to make sure that the thread that was previously
          // started and running eviction loops is in a state where it's okay
          // to update the number of fast loops to perform.
          synchronized (evictionLock) {
            numEvictionLoopsCompleted = 0;
            numFastLoops = (int) ((event.getBytesUsed()
                - event.getThresholds().getEvictionThresholdClearBytes() + getTotalBytesToEvict())
                / getTotalBytesToEvict());
            evictionLock.notifyAll();
          }

          // We've updated the number of fast loops to perform, and there's
          // already a thread running the evictions, so we're done.
          return;
        }

        if (!this.mustEvict.compareAndSet(false, true)) {
          // Another thread just started evicting.
          return;
        }

        numEvictionLoopsCompleted = 0;
        numFastLoops =
            (int) ((event.getBytesUsed() - event.getThresholds().getEvictionThresholdClearBytes()
                + getTotalBytesToEvict()) / getTotalBytesToEvict());
        if (logger.isDebugEnabled()) {
          logger.debug("Starting eviction in response to memory event: {}", event);
        }

        // The new thread which will run in a loop performing evictions
        final Runnable evictionManagerTask = new Runnable() {
          @Override
          public void run() {
            // Has the test hook been set which will cause eviction to abort early
            if (numEvictionLoopsCompleted < getTestAbortAfterLoopCount()) {
              try {
                // Submit tasks into the queue to do the evictions
                if (EVICT_HIGH_ENTRY_COUNT_BUCKETS_FIRST) {
                  createAndSubmitWeightedRegionEvictionTasks();
                } else {
                  for (RegionEvictorTask task : createRegionEvictionTasks()) {
                    executeInThreadPool(task);
                  }
                }

                // Make sure that another thread isn't processing a new eviction event
                // and changing the number of fast loops to perform.
                synchronized (evictionLock) {
                  int delayTime = getEvictionLoopDelayTime();
                  if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Eviction loop delay time calculated to be {} milliseconds. Fast Loops={}, Loop #={}",
                        delayTime, numFastLoops, numEvictionLoopsCompleted + 1);
                  }
                  numEvictionLoopsCompleted++;
                  try {
                    // Wait and release the lock so that the number of fast loops
                    // needed can be updated by another thread processing a new
                    // eviction event.
                    evictionLock.wait(delayTime);
                  } catch (InterruptedException ignored) {
                    // Loop and try again
                  }
                }

                // Do we think we're still above the eviction threshold ...
                if (HeapEvictor.this.mustEvict.get()) {
                  // Submit this runnable back into the thread pool and execute
                  // another pass at eviction.
                  executeInThreadPool(this);
                }
              } catch (RegionDestroyedException ignored) {
                // A region destroyed exception might be thrown for Region.size() when a bucket
                // moves due to rebalancing. retry submitting the eviction task without
                // logging an error message. fixes bug 48162
                if (HeapEvictor.this.mustEvict.get()) {
                  executeInThreadPool(this);
                }
              }
            }
          }
        };

        // Submit the first pass at eviction into the pool
        executeInThreadPool(evictionManagerTask);

      } else {
        this.mustEvict.set(false);
      }
    }
  }

  protected int getEvictionLoopDelayTime() {
    int delayTime = 850; // The waiting period when running fast loops
    if (numEvictionLoopsCompleted - numFastLoops > 2) {
      delayTime = 3000; // Way below the threshold
    } else if (numEvictionLoopsCompleted >= numFastLoops) {
      delayTime = (numEvictionLoopsCompleted - numFastLoops + 3) * 500; // Just below the threshold
    }

    return delayTime;
  }

  boolean mustEvict() {
    return this.mustEvict.get();
  }

  public void close() {
    if (isRunning.compareAndSet(true, false)) {
      evictorThreadPool.shutdownNow();
    }
  }

  private boolean isRunning() {
    return isRunning.get();
  }

  public List<Integer> testOnlyGetSizeOfTasks() {
    if (isRunning()) {
      return testTaskSetSizes;
    }
    return null;
  }

  public long getTotalBytesToEvict() {
    return TOTAL_BYTES_TO_EVICT_FROM_HEAP;
  }

  protected ResourceType getResourceType() {
    return ResourceType.HEAP_MEMORY;
  }

  private int getTestAbortAfterLoopCount() {
    return testAbortAfterLoopCount;
  }

  public void setTestAbortAfterLoopCount(int testAbortAfterLoopCount) {
    this.testAbortAfterLoopCount = testAbortAfterLoopCount;
  }

  int numEvictionLoopsCompleted() {
    return this.numEvictionLoopsCompleted;
  }

  int numFastLoops() {
    return this.numFastLoops;
  }

  private static long setTotalBytesToEvictFromHeap() {
    float evictionBurstPercentage = Float.parseFloat(System
        .getProperty(GEMFIRE_PREFIX + "HeapLRUCapacityController.evictionBurstPercentage", "0.4"));
    long maxTenuredBytes = HeapMemoryMonitor.getTenuredPoolMaxMemory();
    return (long) (maxTenuredBytes * 0.01 * evictionBurstPercentage);
  }
}
