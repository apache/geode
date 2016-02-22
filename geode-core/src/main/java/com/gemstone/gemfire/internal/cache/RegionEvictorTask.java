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
package com.gemstone.gemfire.internal.cache;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.internal.cache.lru.HeapEvictor;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * 
 * Takes delta to be evicted and tries to evict the least no of LRU entry which
 * would make evictedBytes more than or equal to the delta
 * 
 * @since 6.0
 * 
 */
public class RegionEvictorTask implements Callable<Object> {
  
  private static final Logger logger = LogService.getLogger();
  
  private static final int EVICTION_BURST_PAUSE_TIME_MILLIS;

  public static int TEST_EVICTION_BURST_PAUSE_TIME_MILLIS = Integer.MAX_VALUE;

  static {
    EVICTION_BURST_PAUSE_TIME_MILLIS = Integer.getInteger(
        "gemfire.evictionBurstPauseTimeMillis", 1000);
  }

  private static volatile long lastTaskCompletionTime = 0;

  public static void setLastTaskCompletionTime(long v) {
    lastTaskCompletionTime = v;
  }
  public static long getLastTaskCompletionTime() {
    return lastTaskCompletionTime;
  }

  private List<LocalRegion> regionSet;

  private final HeapEvictor evictor;

  private final long bytesToEvictPerTask ; 
  
  public RegionEvictorTask(List<LocalRegion> regionSet, HeapEvictor evictor, long bytesToEvictPerTask) {
    this.evictor = evictor;
    this.regionSet = regionSet;
    this.bytesToEvictPerTask = bytesToEvictPerTask;
  }

  
  public List<LocalRegion> getRegionList() {
    synchronized (this.regionSet) {
      return this.regionSet;
    }
  }

  private GemFireCacheImpl getGemFireCache() {
    return getHeapEvictor().getGemFireCache();
  }

  private HeapEvictor getHeapEvictor() {
    return this.evictor;
  }

  public Object call() throws Exception {
    getGemFireCache().getCachePerfStats().incEvictorJobsStarted();
    long bytesEvicted = 0;
    long totalBytesEvicted = 0;
    try {
      while (true) {
        getGemFireCache().getCachePerfStats();
        final long start = CachePerfStats.getStatTime();
        synchronized (this.regionSet) {
          if (this.regionSet.isEmpty()) {
            lastTaskCompletionTime = System.currentTimeMillis();
            return null;
          }
          // TODO: Yogesh : try Fisher-Yates shuffle algorithm
          Iterator<LocalRegion> iter = regionSet.iterator();
          while (iter.hasNext()) {
            LocalRegion region = iter.next();
            try {
              bytesEvicted = ((AbstractLRURegionMap)region.entries)
                  .centralizedLruUpdateCallback();
              if (bytesEvicted == 0) {
                iter.remove();
              }
              totalBytesEvicted += bytesEvicted;
              if (totalBytesEvicted >= bytesToEvictPerTask
                  || !getHeapEvictor().mustEvict() || this.regionSet.size() == 0) {
                lastTaskCompletionTime = System.currentTimeMillis();
                return null;
              }
            } catch (RegionDestroyedException rd) {
              region.cache.getCancelCriterion().checkCancelInProgress(rd);
            } catch (Exception e) {
              region.cache.getCancelCriterion().checkCancelInProgress(e);
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.Eviction_EVICTOR_TASK_EXCEPTION,
                  new Object[] { e.getMessage() }), e);
            } finally {
              getGemFireCache().getCachePerfStats();
              long end = CachePerfStats.getStatTime();
              getGemFireCache().getCachePerfStats().incEvictWorkTime(end-start);
            }
          }
        }
      }
    } finally {
      getGemFireCache().getCachePerfStats().incEvictorJobsCompleted();
    }
  }

  public static int getEvictionBurstPauseTimeMillis() {
    if (TEST_EVICTION_BURST_PAUSE_TIME_MILLIS != Integer.MAX_VALUE) {
      return TEST_EVICTION_BURST_PAUSE_TIME_MILLIS;
    }
    return EVICTION_BURST_PAUSE_TIME_MILLIS;
  }
}
