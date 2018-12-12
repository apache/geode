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

import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.logging.LogService;

/**
 * Takes delta to be evicted and tries to evict the least no of LRU entry which would make
 * evictedBytes more than or equal to the delta
 *
 * @since GemFire 6.0
 */
public class RegionEvictorTask implements Runnable {

  private static final Logger logger = LogService.getLogger();

  private final CachePerfStats stats;

  private final List<LocalRegion> regions;

  private final HeapEvictor evictor;

  private final long bytesToEvictPerTask;

  RegionEvictorTask(final CachePerfStats stats, final List<LocalRegion> regions,
      final HeapEvictor evictor, final long bytesToEvictPerTask) {
    this.stats = stats;
    this.evictor = evictor;
    this.regions = regions;
    this.bytesToEvictPerTask = bytesToEvictPerTask;
  }

  List<LocalRegion> getRegionList() {
    synchronized (this.regions) {
      return this.regions;
    }
  }

  private HeapEvictor getHeapEvictor() {
    return this.evictor;
  }

  @Override
  public void run() {
    this.stats.incEvictorJobsStarted();
    try {
      long totalBytesEvicted = 0;
      while (true) {
        final long start = CachePerfStats.getStatTime();
        synchronized (this.regions) {
          if (this.regions.isEmpty()) {
            return;
          }
          // consider trying Fisher-Yates shuffle algorithm
          for (Iterator<LocalRegion> iterator = regions.iterator(); iterator.hasNext();) {
            LocalRegion region = iterator.next();
            try {
              long bytesEvicted = region.getRegionMap().centralizedLruUpdateCallback();
              if (bytesEvicted == 0) {
                iterator.remove();
              }
              totalBytesEvicted += bytesEvicted;
              if (totalBytesEvicted >= bytesToEvictPerTask || !getHeapEvictor().mustEvict()
                  || this.regions.isEmpty()) {
                return;
              }
            } catch (RegionDestroyedException e) {
              region.getCache().getCancelCriterion().checkCancelInProgress(e);
            } catch (RuntimeException e) {
              region.getCache().getCancelCriterion().checkCancelInProgress(e);
              logger.warn(String.format("Exception: %s occurred during eviction ",
                  new Object[] {e.getMessage()}), e);
            } finally {
              long end = CachePerfStats.getStatTime();
              this.stats.incEvictWorkTime(end - start);
            }
          }
        }
      }
    } finally {
      this.stats.incEvictorJobsCompleted();
    }
  }
}
