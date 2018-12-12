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

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.persistence.DiskRegionView;

/**
 * A {@code CapacityController} that will evict an entry from a region once the region entry count
 * reaches a certain capacity.
 *
 * @since GemFire 2.0.2
 */
public class CountLRUEviction extends AbstractEvictionController {

  /** The maximum number entries allowed by this controller */
  private volatile int maximumEntries;

  /**
   * Creates an LRU capacity controller that allows the given number of maximum entries.
   *
   *
   * @param maximumEntries The maximum number of entries allowed in the region whose capacity this
   *        controller controls. Once there are {@code capacity} entries in a region, this
   *        controller will remove the least recently used entry.<br>
   *        <p>
   *        For a region with {@link DataPolicy#PARTITION}, the maximum number of entries allowed in
   *        the region, collectively for its primary buckets and redundant copies for this VM. After
   *        there are {@code capacity} entries in the region's primary buckets and redundant copies
   *        for this VM, this controller will remove the least recently used entry from the bucket
   *        in which the subsequent {@code put} takes place.
   * @param evictionAction The action to perform upon the least recently used entry.
   */
  public CountLRUEviction(EvictionCounters evictionCounters, int maximumEntries,
      EvictionAction evictionAction, EvictionAlgorithm algorithm) {
    super(evictionCounters, evictionAction, algorithm);
    setMaximumEntries(maximumEntries);
  }

  /**
   * Sets the limit on the number of entries allowed. This change takes place on next region
   * operation that could increase the region size.
   */
  private void setMaximumEntries(int maximumEntries) {
    if (maximumEntries <= 0) {
      throw new IllegalArgumentException(
          "Maximum entries must be positive");
    }
    this.maximumEntries = maximumEntries;
    getCounters().setLimit(maximumEntries);
  }

  @Override
  public void setLimit(int max) {
    setMaximumEntries(max);
  }

  ////////////////////// Instance Methods /////////////////////

  @Override
  public long getLimit() {
    return this.maximumEntries;
  }

  /**
   * All entries for the LRUCapacityController are considered to be of size 1.
   */
  @Override
  public int entrySize(Object key, Object value) {

    if (Token.isRemoved(value)) {
      // bug #42228 - lruEntryDestroy removes an entry from the LRU, but if
      // it is subsequently resurrected we want the new entry to generate a delta
      return 0;
    }
    if ((value == null /* overflow to disk */ || value == Token.INVALID
        || value == Token.LOCAL_INVALID) && getEvictionAction().isOverflowToDisk()) {
      // Don't count this entry toward LRU
      return 0;

    } else {
      return 1;
    }
  }

  @Override
  public boolean mustEvict(EvictionCounters stats, InternalRegion region, int delta) {
    return stats.getCounter() + delta > stats.getLimit();
  }

  @Override
  public boolean lruLimitExceeded(EvictionCounters stats, DiskRegionView diskRegionView) {
    return stats.getCounter() > stats.getLimit();
  }


  /**
   * Returns a brief description of this capacity controller.
   *
   * @since GemFire 4.0
   */
  @Override
  public String toString() {
    return String.format(
        "LRUCapacityController with a capacity of %s entries and eviction action %s",
        getLimit(), getEvictionAction());
  }
}
