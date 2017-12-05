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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;

abstract class AbstractEvictionList implements EvictionList {
  private static final Logger logger = LogService.getLogger();

  /** The last node in the LRU list after which all new nodes are added */
  protected final EvictionNode tail = new GuardNode();

  /** The starting point in the LRU list for searching for the LRU node */
  protected final EvictionNode head = new GuardNode();

  /** Description of the Field */
  protected final InternalEvictionStatistics stats;

  /** Counter for the size of the LRU list */
  private final AtomicInteger size = new AtomicInteger();

  /** ReadWriteLock for clearRegion consistency with AbrstractRegionMap */
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private BucketRegion bucketRegion;

  AbstractEvictionList(InternalEvictionStatistics stats, BucketRegion region) {
    if (stats == null) {
      throw new IllegalArgumentException("EvictionStatistics must not be null");
    }
    this.stats = stats;
    this.bucketRegion = region;
    initEmptyList();
  }

  @Override
  public int size() {
    return size.get();
  }

  private void incrementSize() {
    size.incrementAndGet();
  }

  void decrementSize() {
    int newSize = size.decrementAndGet();
    if (newSize < 0) {
      throw new IllegalStateException("Eviction list size cannot be negative");
    }
  }

  @Override
  public void closeStats() {
    stats.close();
  }

  @Override
  public EvictionStatistics getStatistics() {
    return stats;
  }

  @Override
  public void setBucketRegion(Object region) {
    if (region instanceof BucketRegion) {
      this.bucketRegion = (BucketRegion) region; // see bug 41388
    }
  }

  @Override
  public void clear(RegionVersionVector regionVersionVector) {
    if (regionVersionVector != null) {
      return; // when concurrency checks are enabled the clear operation removes entries iteratively
    }

    synchronized (this) {
      if (bucketRegion != null) {
        stats.decrementCounter(bucketRegion.getCounter());
        bucketRegion.resetCounter();
      } else {
        stats.resetCounter();
      }
      initEmptyList();
    }
  }

  private synchronized void initEmptyList() {
    size.set(0);
    head.setNext(tail);
    tail.setPrevious(head);
  }

  /**
   * Adds an lru node to the tail of the list.
   */
  @Override
  public synchronized void appendEntry(final EvictionNode evictionNode) {
    if (evictionNode.next() != null) {
      // already in the list
      return;
    }

    evictionNode.unsetRecentlyUsed();

    if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
      logger.trace(LogMarker.LRU_CLOCK, LocalizedMessage
          .create(LocalizedStrings.NewLRUClockHand_ADDING_ANODE_TO_LRU_LIST, evictionNode));
    }

    evictionNode.setNext(tail);
    tail.previous().setNext(evictionNode);
    evictionNode.setPrevious(tail.previous());
    tail.setPrevious(evictionNode);

    incrementSize();
  }

  @Override
  public synchronized void destroyEntry(EvictionNode evictionNode) {
    if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
      logger.trace(LogMarker.LRU_CLOCK, LocalizedMessage
          .create(LocalizedStrings.NewLRUClockHand_UNLINKENTRY_CALLED, evictionNode));
    }

    if (removeEntry(evictionNode)) {
      getStatistics().incDestroys();
    }

  }

  protected synchronized boolean removeEntry(EvictionNode evictionNode) {
    if (evictionNode.next() == null) {
      // not in the list anymore.
      return false;
    }

    unlinkEntry(evictionNode);
    return true;
  }

  protected synchronized void unlinkEntry(EvictionNode evictionNode) {
    EvictionNode next = evictionNode.next();
    EvictionNode previous = evictionNode.previous();
    next.setPrevious(previous);
    previous.setNext(next);
    evictionNode.setNext(null);
    evictionNode.setPrevious(null);
    decrementSize();
  }

  @Override
  public void acquireWriteLock() {
    readWriteLock.writeLock().lock();
  }

  @Override
  public void releaseWriteLock() {
    readWriteLock.writeLock().unlock();;
  }

  @Override
  public void acquireReadLock() {
    readWriteLock.readLock().lock();
  }

  @Override
  public void releaseReadLock() {
    readWriteLock.readLock().unlock();
  }
}
