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

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.cache.BucketRegion;

/**
 * Eviction controllers that extend this class evict the least recently used (LRU) entry in the
 * region whose capacity they controller. In order to provide an efficient computation of the LRU
 * entry, GemFire uses special internal data structures for managing the contents of a region. As a
 * result, there are several restrictions that are placed on regions whose capacity is governed by
 * an LRU algorithm.
 *
 * <ul>
 * <li>If the capacity of a region is to be controlled by an LRU algorithm, then the region must be
 * <b>created</b> with {@link EvictionAttributes}
 * <li>The eviction controller of a region governed by an LRU algorithm cannot be changed.</li>
 * <li>An LRU algorithm cannot be applied to a region after the region has been created.</li>
 * </ul>
 *
 * <p>
 * LRU algorithms also specify what {@linkplain EvictionAction action} should be performed upon the
 * least recently used entry when the capacity is reached. Currently, there are two supported
 * actions: {@linkplain EvictionAction#LOCAL_DESTROY locally destroying} the entry (which is the
 * {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION default}), thus freeing up space in the VM,
 * and {@linkplain EvictionAction#OVERFLOW_TO_DISK overflowing} the value of the entry to disk.
 *
 * <p>
 * {@link EvictionAttributes Eviction controllers} that use an LRU algorithm maintain certain
 * region-dependent state (such as the maximum number of entries allowed in the region). As a
 * result, an instance of {@code AbstractEvictionController} cannot be shared among multiple
 * regions. Attempts to create a region with a LRU-based capacity controller that has already been
 * used to create another region will result in an {@link IllegalStateException} being thrown.
 *
 * @since GemFire 3.2
 */
public abstract class AbstractEvictionController implements EvictionController {

  /**
   * Create and return the appropriate eviction controller using the attributes provided.
   */
  public static EvictionController create(EvictionAttributes evictionAttributes, boolean isOffHeap,
      StatisticsFactory statsFactory, String statsName) {
    EvictionAlgorithm algorithm = evictionAttributes.getAlgorithm();
    EvictionAction action = evictionAttributes.getAction();
    ObjectSizer sizer = evictionAttributes.getObjectSizer();
    int maximum = evictionAttributes.getMaximum();
    EvictionStats evictionStats;
    EvictionCounters evictionCounters;
    if (algorithm == EvictionAlgorithm.LRU_HEAP) {
      evictionStats = new HeapLRUStatistics(statsFactory, statsName);
      evictionCounters = new EvictionCountersImpl(evictionStats);
      return new HeapLRUController(evictionCounters, action, sizer, algorithm);
    }
    if (algorithm == EvictionAlgorithm.LRU_MEMORY || algorithm == EvictionAlgorithm.LIFO_MEMORY) {
      evictionStats = new MemoryLRUStatistics(statsFactory, statsName);
      evictionCounters = new EvictionCountersImpl(evictionStats);
      return new MemoryLRUController(evictionCounters, maximum, sizer, action, isOffHeap,
          algorithm);
    }
    if (algorithm == EvictionAlgorithm.LRU_ENTRY || algorithm == EvictionAlgorithm.LIFO_ENTRY) {
      evictionStats = new CountLRUStatistics(statsFactory, statsName);
      evictionCounters = new EvictionCountersImpl(evictionStats);
      return new CountLRUEviction(evictionCounters, maximum, action, algorithm);
    }

    throw new IllegalStateException("Unhandled algorithm " + algorithm);
  }

  /**
   * What to do upon eviction
   */
  private final EvictionAction evictionAction;

  /**
   * Used to dynamically track the changing region limit.
   */
  private final EvictionCounters counters;

  private final EvictionAlgorithm algorithm;

  /**
   * Creates a new {@code AbstractEvictionController} with the given {@linkplain EvictionAction
   * eviction action}.
   *
   */
  protected AbstractEvictionController(EvictionCounters evictionCounters,
      EvictionAction evictionAction, EvictionAlgorithm algorithm) {
    counters = evictionCounters;
    this.evictionAction = evictionAction;
    this.algorithm = algorithm;
  }

  /**
   * Force subclasses to have a reasonable {@code toString}
   *
   * @since GemFire 4.0
   */
  @Override
  public abstract String toString();

  /**
   * Gets the action that is performed on the least recently used entry when it is evicted from the
   * VM.
   *
   * @return one of the following constants: {@link EvictionAction#LOCAL_DESTROY},
   *         {@link EvictionAction#OVERFLOW_TO_DISK}
   */
  @Override
  public EvictionAction getEvictionAction() {
    return evictionAction;
  }

  @Override
  public EvictionCounters getCounters() {
    return counters;
  }

  @Override
  public EvictionAlgorithm getEvictionAlgorithm() {
    return algorithm;
  }

  @Override
  public long limit() {
    return getCounters().getLimit();
  }

  @Override
  public void close() {
    getCounters().close();
  }

  @Override
  public void closeBucket(BucketRegion bucketRegion) {
    getCounters().decrementCounter(bucketRegion.getCounter());
  }

  @Override
  public void setPerEntryOverhead(int entryOverhead) {
    // nothing needed by default
  }
}
