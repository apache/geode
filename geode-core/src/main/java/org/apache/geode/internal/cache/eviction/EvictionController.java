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

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.persistence.DiskRegionView;

/**
 * Marker interface to eviction controller that determines if LRU list maintenance is required.
 */
public interface EvictionController {

  /**
   * return the size of an entry or its worth when constraining the size of an LRU EntriesMap.
   */
  int entrySize(Object key, Object value);

  /**
   * return the limit for the accumulated entrySize which triggers disposal.
   */
  long limit();

  /** return the eviction controller instance this came from */
  EvictionAlgorithm getEvictionAlgorithm();

  /** return the stats object for this eviction controller */
  EvictionCounters getCounters();

  /**
   * Returns the {@code EvictionAction} to take when the LRU entry is evicted.
   */
  EvictionAction getEvictionAction();

  /**
   * Returns whether or not there is enough room to accommodate data of the given size based on the
   * given {@code EvictionStatistics}.
   */
  boolean mustEvict(EvictionCounters stats, InternalRegion region, int delta);

  boolean lruLimitExceeded(EvictionCounters stats, DiskRegionView diskRegionView);

  /**
   * Returns the "limit" as defined by this LRU algorithm
   */
  long getLimit();

  /**
   * Set the limiting parameter used to determine when eviction is needed.
   */
  void setLimit(int maximum);

  void close();

  void closeBucket(BucketRegion bucketRegion);

  void setPerEntryOverhead(int entryOverhead);

}
