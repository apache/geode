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

package org.apache.geode.internal.cache;

import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.internal.InternalStatisticsDisabledException;

class CacheStatisticsImpl implements CacheStatistics {
  private final InternalRegion region;
  private final RegionEntry regionEntry;

  CacheStatisticsImpl(RegionEntry regionEntry, InternalRegion region) {
    this.region = region;
    // entry stats are all on commited state so ok to ignore tx state
    this.regionEntry = regionEntry;
  }

  @Override
  public long getHitCount() throws StatisticsDisabledException {
    checkEntryDestroyed();
    try {
      return regionEntry.getHitCount();
    } catch (InternalStatisticsDisabledException e) {
      throw new StatisticsDisabledException(e);
    }
  }

  @Override
  public float getHitRatio() throws StatisticsDisabledException {
    checkEntryDestroyed();
    // Don't worry about write synchronizing. This is just a stat
    // so its ok if the hit ratio is inaccurate because the hit count
    // and miss count are sampled without a write lock.
    RegionEntry entry = regionEntry;
    try {
      long hitCount = entry.getHitCount();
      long total = hitCount + entry.getMissCount();
      return total == 0L ? 0.0f : ((float) hitCount / total);
    } catch (InternalStatisticsDisabledException e) {
      throw new StatisticsDisabledException(e);
    }
  }

  @Override
  public long getLastAccessedTime() throws StatisticsDisabledException {
    checkEntryDestroyed();
    try {
      return regionEntry.getLastAccessed();
    } catch (InternalStatisticsDisabledException e) {
      throw new StatisticsDisabledException(e);
    }
  }

  @Override
  public long getLastModifiedTime() {
    checkEntryDestroyed();
    return regionEntry.getLastModified();
  }

  @Override
  public long getMissCount() throws StatisticsDisabledException {
    checkEntryDestroyed();
    try {
      return regionEntry.getMissCount();
    } catch (InternalStatisticsDisabledException e) {
      throw new StatisticsDisabledException(e);
    }
  }

  @Override
  public void resetCounts() throws StatisticsDisabledException {
    checkEntryDestroyed();
    try {
      regionEntry.resetCounts();
    } catch (InternalStatisticsDisabledException e) {
      throw new StatisticsDisabledException(e);
    }
  }

  /*
   * throws CacheClosedException or EntryDestroyedException if this entry is destroyed.
   */
  private void checkEntryDestroyed() {
    region.getCancelCriterion().checkCancelInProgress(null);
    if (regionEntry.isRemoved()) {
      throw new EntryDestroyedException(regionEntry.getKey().toString());
    }
  }
}
