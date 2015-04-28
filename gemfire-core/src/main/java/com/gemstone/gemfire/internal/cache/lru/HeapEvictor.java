/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.lru;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionEvictorTask;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.MemoryEvent;
import com.gemstone.gemfire.internal.cache.control.ResourceListener;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;

/**
 * Triggers centralized eviction(asynchornously) when ResourceManager sends
 * EVICTION_UP event. This is registered with Resource Manager.
 * 
 * @author Yogesh, Suranjan, Amardeep
 * @since 6.0
 * 
 */
public class HeapEvictor implements ResourceListener<MemoryEvent> {
  private static final Logger logger = LogService.getLogger();
  
  public static final int MAX_EVICTOR_THREADS = Integer.getInteger(
      "gemfire.HeapLRUCapacityController.MAX_EVICTOR_THREADS", Runtime.getRuntime().availableProcessors()*4);

  public static final boolean DISABLE_HEAP_EVICTIOR_THREAD_POOL = Boolean
      .getBoolean("gemfire.HeapLRUCapacityController.DISABLE_HEAP_EVICTIOR_THREAD_POOL");

  public static final boolean EVICT_HIGH_ENTRY_COUNT_BUCKETS_FIRST = Boolean.valueOf(
      System.getProperty(
          "gemfire.HeapLRUCapacityController.evictHighEntryCountBucketsFirst",
          "true")).booleanValue(); 

  public static final int MINIMUM_ENTRIES_PER_BUCKET = Integer
  .getInteger("gemfire.HeapLRUCapacityController.inlineEvictionThreshold",0);
  
  public static final long TOTAL_BYTES_TO_EVICT_FROM_HEAP; 
  
  public static final int BUCKET_SORTING_INTERVAL = Integer.getInteger(
      "gemfire.HeapLRUCapacityController.higherEntryCountBucketCalculationInterval", 100).intValue();               
  
  static {
    float evictionBurstPercentage = Float.parseFloat(System.getProperty(
        "gemfire.HeapLRUCapacityController.evictionBurstPercentage", "0.4"));
    long maxTenuredBytes = InternalResourceManager.getTenuredPoolMaxMemory();
    TOTAL_BYTES_TO_EVICT_FROM_HEAP = (long)(maxTenuredBytes * 0.01 * evictionBurstPercentage);
  }
  
  private ThreadPoolExecutor evictorThreadPool = null;

  private AtomicBoolean mustEvict = new AtomicBoolean(false);

  private final Cache cache;  

  private ArrayList testTaskSetSizes = new  ArrayList();
  
  private BlockingQueue<Runnable> poolQueue;
  
  private AtomicBoolean isRunning = new AtomicBoolean(true);
  
  public HeapEvictor(Cache gemFireCache) {
    this.cache = gemFireCache;
    initializeEvictorThreadPool();
  }

  private List<LocalRegion> getAllRegionList() {
    List<LocalRegion> allRegionList = new ArrayList<LocalRegion>();
    InternalResourceManager irm = (InternalResourceManager)cache
        .getResourceManager();
    for (ResourceListener<MemoryEvent> listener : irm.getMemoryEventListeners()) {
      if (listener instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion)listener;
        if (pr.getEvictionAttributes().getAlgorithm().isLRUHeap()
            && pr.getDataStore() != null) {
          allRegionList.addAll(pr.getDataStore().getAllLocalBucketRegions());
        }
      }
      else if (listener instanceof LocalRegion) {
        LocalRegion lr = (LocalRegion)listener;
        if (lr.getEvictionAttributes().getAlgorithm().isLRUHeap()) {
          allRegionList.add(lr);
        }
      }
    }

    if (HeapEvictor.MINIMUM_ENTRIES_PER_BUCKET > 0) {
      Iterator<LocalRegion> iter = allRegionList.iterator();
      while(iter.hasNext()){
        LocalRegion lr = iter.next();
        if (lr instanceof BucketRegion) {
          if (((BucketRegion)lr).getNumEntriesInVM() <= HeapEvictor.MINIMUM_ENTRIES_PER_BUCKET) {
            iter.remove();
          }
        }        
      }
    }
    return allRegionList;
  }
  
  private List<LocalRegion> getAllSortedRegionList(){
    List<LocalRegion> allRegionList = getAllRegionList();
    
    //Capture the sizes so that they do not change while sorting
    final Object2LongOpenHashMap sizes = new Object2LongOpenHashMap(allRegionList.size());
    for(LocalRegion r : allRegionList) {
      long size = r instanceof BucketRegion ?((BucketRegion)r).getSizeForEviction() : r.size();
      sizes.put(r, size);
    }
    
    //Sort with respect to other PR buckets also in case of multiple PRs
    Collections.sort(allRegionList, new Comparator<LocalRegion>() {
      public int compare(LocalRegion r1, LocalRegion r2) {        
        long numEntries1 = sizes.get(r1);
        long numEntries2 = sizes.get(r2);
        if (numEntries1 > numEntries2) {
          return -1;
        }
        else if (numEntries1 < numEntries2) {
          return 1;
        }
        return 0;
      }
    });
    return allRegionList;
  }

  public GemFireCacheImpl getGemFireCache() {
    return (GemFireCacheImpl)this.cache;
  }
  
  private void initializeEvictorThreadPool() {

    final ThreadGroup evictorThreadGroup = LoggingThreadGroup.createThreadGroup(
        "EvictorThreadGroup", logger);
    ThreadFactory evictorThreadFactory = new ThreadFactory() {
      private int next = 0;

      public Thread newThread(Runnable command) {
        Thread t = new Thread(evictorThreadGroup, command, "EvictorThread "
            + next++);
        t.setDaemon(true);
        return t;
      }
    };

    if (!DISABLE_HEAP_EVICTIOR_THREAD_POOL) {
      this.poolQueue = new OverflowQueueWithDMStats(getGemFireCache().getCachePerfStats().getEvictionQueueStatHelper());
      this.evictorThreadPool = new ThreadPoolExecutor(MAX_EVICTOR_THREADS, MAX_EVICTOR_THREADS,
          15, TimeUnit.SECONDS, this.poolQueue, evictorThreadFactory);
    }
  }

  /**
   * The task(i.e the region on which eviction needs to be performed) is
   * assigned to the threadpool.
   */
  private void submitRegionEvictionTask(Callable<Object> task) {
    evictorThreadPool.submit(task);
  }

  public ThreadPoolExecutor getEvictorThreadPool() {
    if(isRunning.get()) {
      return evictorThreadPool;
    }
    return null;
  }

  /**
   * returns the total number of tasks that are currently being executed or
   * queued for execution
   * 
   * @return sum of scheduled and running tasks
   */
  public int getRunningAndScheduledTasks() {
    if(isRunning.get()){
    return this.evictorThreadPool.getActiveCount()
        + this.evictorThreadPool.getQueue().size();
    }
    return -1;
  }
  
  private void createAndSubmitWeightedRegionEvictionTasks() {
    List<LocalRegion> allRegionList = getAllSortedRegionList();
    float numEntriesInVm = 0 ;
    for(LocalRegion lr : allRegionList){
      if(lr instanceof BucketRegion){
        numEntriesInVm = numEntriesInVm + ((BucketRegion)lr).getSizeForEviction();
      }else {
        numEntriesInVm = numEntriesInVm + lr.getRegionMap().sizeInVM();
      }
    }
    for(LocalRegion lr : allRegionList){
      List<LocalRegion> regionsForSingleTask = new ArrayList<LocalRegion>(1);
      float regionEntryCnt = 0;
      if(lr instanceof BucketRegion){
        regionEntryCnt = ((BucketRegion)lr).getSizeForEviction();
      }else {
        regionEntryCnt = lr.getRegionMap().sizeInVM();
      }
      float percentage = (regionEntryCnt/numEntriesInVm);
      long bytesToEvictPerTask = (long)(TOTAL_BYTES_TO_EVICT_FROM_HEAP * percentage);
      regionsForSingleTask.add(lr);      
      if (mustEvict()) {
        submitRegionEvictionTask(new RegionEvictorTask(regionsForSingleTask, this,bytesToEvictPerTask));
      }else {
        break;
      }       
    }
  }

  private Set<Callable<Object>> createRegionEvictionTasks() {
    Set<Callable<Object>> evictorTaskSet = new HashSet<Callable<Object>>();
    int threadsAvailable = getEvictorThreadPool().getCorePoolSize();
    long bytesToEvictPerTask = TOTAL_BYTES_TO_EVICT_FROM_HEAP/threadsAvailable;
    List<LocalRegion> allRegionList = getAllRegionList();
    // This shuffling is not required when eviction triggered for the first time
    Collections.shuffle(allRegionList);
    int allRegionSetSize = allRegionList.size();
    if (allRegionList.isEmpty()) {
      return evictorTaskSet;
    }
    if (allRegionSetSize <= threadsAvailable) {
      for (LocalRegion region : allRegionList) {
        List<LocalRegion> regionList = new ArrayList<LocalRegion>(1);
        regionList.add(region);
        Callable<Object> task = new RegionEvictorTask(regionList, this, bytesToEvictPerTask);
        evictorTaskSet.add(task);
      }
      Iterator iterator=evictorTaskSet.iterator();
      while(iterator.hasNext())
      {
        RegionEvictorTask regionEvictorTask=(RegionEvictorTask)iterator.next();
        testTaskSetSizes.add(regionEvictorTask.getRegionList().size());
      }
      return evictorTaskSet;
    }
    int numRegionsInTask = allRegionSetSize / threadsAvailable;
    List<LocalRegion> regionsForSingleTask = null;
    Iterator<LocalRegion> itr = allRegionList.iterator();
    for (int i = 0; i < threadsAvailable; i++) {
      int count = 1;
      regionsForSingleTask = new ArrayList<LocalRegion>(numRegionsInTask);
      while (count <= numRegionsInTask) {
        if (itr.hasNext()) {
          regionsForSingleTask.add(itr.next());
        }
        count++;
      }
      evictorTaskSet.add(new RegionEvictorTask(regionsForSingleTask, this,bytesToEvictPerTask));
    }
    //Add leftover regions to last task 
    while (itr.hasNext()) {
      regionsForSingleTask.add(itr.next());
    }
    
    Iterator iterator=evictorTaskSet.iterator();
    while(iterator.hasNext())
    {
      RegionEvictorTask regionEvictorTask=(RegionEvictorTask)iterator.next();
      testTaskSetSizes.add(regionEvictorTask.getRegionList().size());
    }
    return evictorTaskSet;
  }

  public void onEvent(MemoryEvent event) {
    if(isRunning.get()) {
      if (event.isLocal()) {
        if (event.getType().isEvictionUp() || event.getType().isEvictMore()) {
          this.mustEvict.set(true);
          if (!DISABLE_HEAP_EVICTIOR_THREAD_POOL) {
            for (;;) {
              try {
                if (EVICT_HIGH_ENTRY_COUNT_BUCKETS_FIRST) {
                  createAndSubmitWeightedRegionEvictionTasks();
                }
                else {
                  for (Callable<Object> task : createRegionEvictionTasks()) {
                    submitRegionEvictionTask(task);
                  }
                }
                RegionEvictorTask.setLastTaskCompletionTime(System.currentTimeMillis()); // bug 41938
                break;
              } catch (RegionDestroyedException e) {
                // A region destroyed exception might be thrown for Region.size() when a bucket
                // moves due to rebalancing. retry submitting the eviction task without
                // logging an error message. fixes bug 48162
              }
            }
          }
        }
        else if (event.getType().isEvictionDown()
            || event.getType().isEvictionDisabled()) {
          this.mustEvict.set(false);
        }
      }
    }
  }

  public boolean mustEvict() {
    return this.mustEvict.get();
  }
  
  public void close() {
    getEvictorThreadPool().shutdownNow();
    isRunning.set(false);
  }
  
  public ArrayList testOnlyGetSizeOfTasks()
  {
    if(isRunning.get())
      return testTaskSetSizes;
    return null;
  }
}
