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

package org.apache.geode.internal.cache.lru;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.logging.log4j.Logger;

/**
 * AbstractLRUClockHand holds the lrulist, and the behavior for maintaining the list in a cu-pipe
 * and determining the next entry to be removed. Each EntriesMap that supports LRU holds one of
 * these.
 */
public class NewLRUClockHand {
  private static final Logger logger = LogService.getLogger();

  private BucketRegion bucketRegion = null;

  /** The last node in the LRU list after which all new nodes are added */
  protected LRUClockNode tail = new GuardNode();

  /** The starting point in the LRU list for searching for the LRU node */
  protected LRUClockNode head = new GuardNode();

  /** The object for locking the head of the cu-pipe. */
  final protected HeadLock lock;

  /** Description of the Field */
  final private LRUStatistics stats;
  /** Counter for the size of the LRU list */
  protected int size = 0;

  public static final boolean debug =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "verbose-lru-clock");

  static private final int maxEntries;

  static {
    String squelch = System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "lru.maxSearchEntries");
    if (squelch == null)
      maxEntries = -1;
    else
      maxEntries = Integer.parseInt(squelch);
  }

  /** only used by enhancer */
  // protected NewLRUClockHand( ) { }

  // private long size = 0;

  public NewLRUClockHand(Object region, EnableLRU ccHelper,
      InternalRegionArguments internalRegionArgs) {
    setBucketRegion(region);
    this.lock = new HeadLock();
    // behavior relies on a single evicted node in the pipe when the pipe is empty.
    initHeadAndTail();
    if (this.bucketRegion != null) {
      this.stats = internalRegionArgs.getPartitionedRegion() != null
          ? internalRegionArgs.getPartitionedRegion().getEvictionController().stats : null;
    } else {
      LRUStatistics tmp = null;
      if (region instanceof PlaceHolderDiskRegion) {
        tmp = ((PlaceHolderDiskRegion) region).getPRLRUStats();
      } else if (region instanceof PartitionedRegion) {
        tmp = ((PartitionedRegion) region).getPRLRUStatsDuringInitialization(); // bug 41938
        PartitionedRegion pr = (PartitionedRegion) region;
        if (tmp != null) {
          pr.getEvictionController().stats = tmp;
        }
      }
      if (tmp == null) {
        StatisticsFactory sf = GemFireCacheImpl.getExisting("").getDistributedSystem();
        tmp = ccHelper.initStats(region, sf);
      }
      this.stats = tmp;
    }
  }

  public void setBucketRegion(Object r) {
    if (r instanceof BucketRegion) {
      this.bucketRegion = (BucketRegion) r; // see bug 41388
    }
  }

  public NewLRUClockHand(Region region, EnableLRU ccHelper, NewLRUClockHand oldList) {
    setBucketRegion(region);
    this.lock = new HeadLock();
    // behavior relies on a single evicted node in the pipe when the pipe is empty.
    initHeadAndTail();
    if (oldList.stats == null) {
      // see bug 41388
      StatisticsFactory sf = region.getCache().getDistributedSystem();
      this.stats = ccHelper.initStats(region, sf);
    } else {
      this.stats = oldList.stats;
      if (this.bucketRegion != null) {
        this.stats.decrementCounter(this.bucketRegion.getCounter());
        this.bucketRegion.resetCounter();
      } else {
        this.stats.resetCounter();
      }
    }
  }

  /** Description of the Method */
  public void close() {
    closeStats();
    if (bucketRegion != null)
      bucketRegion.close();
  }

  public void closeStats() {
    LRUStatistics ls = this.stats;
    if (ls != null) {
      ls.close();
    }
  }

  /**
   * Adds a new lru node for the entry between the current tail and head of the list.
   *
   * @param aNode Description of the Parameter
   */
  public void appendEntry(final LRUClockNode aNode) {
    synchronized (this.lock) {
      if (aNode.nextLRUNode() != null || aNode.prevLRUNode() != null) {
        return;
      }

      if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
        logger.trace(LogMarker.LRU_CLOCK, LocalizedMessage
            .create(LocalizedStrings.NewLRUClockHand_ADDING_ANODE_TO_LRU_LIST, aNode));
      }
      aNode.setNextLRUNode(this.tail);
      this.tail.prevLRUNode().setNextLRUNode(aNode);
      aNode.setPrevLRUNode(this.tail.prevLRUNode());
      this.tail.setPrevLRUNode(aNode);

      this.size++;
    }
  }

  /**
   * return the head entry in the list preserving the cupipe requirement of at least one entry left
   * in the list
   */
  private LRUClockNode getHeadEntry() {
    synchronized (lock) {
      LRUClockNode aNode = NewLRUClockHand.this.head.nextLRUNode();
      if (aNode == this.tail) {
        return null;
      }

      LRUClockNode next = aNode.nextLRUNode();
      this.head.setNextLRUNode(next);
      next.setPrevLRUNode(this.head);

      aNode.setNextLRUNode(null);
      aNode.setPrevLRUNode(null);
      this.size--;
      return aNode;
    }
  }


  /**
   * return the Entry that is considered least recently used. The entry will no longer be in the
   * pipe (unless it is the last empty marker).
   */
  public LRUClockNode getLRUEntry() {
    long numEvals = 0;

    for (;;) {
      LRUClockNode aNode = null;
      aNode = getHeadEntry();

      if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
        logger.trace(LogMarker.LRU_CLOCK, "lru considering {}", aNode);
      }

      if (aNode == null) { // hit the end of the list
        this.stats.incEvaluations(numEvals);
        return aNode;
      } // hit the end of the list

      numEvals++;

      // If this Entry is part of a transaction, skip it since
      // eviction should not cause commit conflicts
      synchronized (aNode) {
        if (aNode instanceof AbstractRegionEntry) {
          if (((AbstractRegionEntry) aNode).isInUseByTransaction()) {
            if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
              logger.trace(LogMarker.LRU_CLOCK, LocalizedMessage.create(
                  LocalizedStrings.NewLRUClockHand_REMOVING_TRANSACTIONAL_ENTRY_FROM_CONSIDERATION));
            }
            continue;
          }
        }
        if (aNode.testEvicted()) {
          if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
            logger.trace(LogMarker.LRU_CLOCK,
                LocalizedMessage.create(LocalizedStrings.NewLRUClockHand_DISCARDING_EVICTED_ENTRY));
          }
          continue;
        }

        // At this point we have any acceptable entry. Now
        // use various criteria to determine if it's good enough
        // to return, or if we need to add it back to the list.
        if (maxEntries > 0 && numEvals > maxEntries) {
          if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
            logger.trace(LogMarker.LRU_CLOCK, LocalizedMessage
                .create(LocalizedStrings.NewLRUClockHand_GREEDILY_PICKING_AN_AVAILABLE_ENTRY));
          }
          this.stats.incGreedyReturns(1);
          // fall through, return this node
        } else if (aNode.testRecentlyUsed()) {
          // Throw it back, it's in the working set
          aNode.unsetRecentlyUsed();
          // aNode.setInList();
          if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
            logger.trace(LogMarker.LRU_CLOCK, LocalizedMessage
                .create(LocalizedStrings.NewLRUClockHand_SKIPPING_RECENTLY_USED_ENTRY, aNode));
          }
          appendEntry(aNode);
          continue; // keep looking
        } else {
          if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
            logger.trace(LogMarker.LRU_CLOCK, LocalizedMessage
                .create(LocalizedStrings.NewLRUClockHand_RETURNING_UNUSED_ENTRY, aNode));
          }
          // fall through, return this node
        }

        // Return the current node.
        this.stats.incEvaluations(numEvals);
        return aNode;
      } // synchronized
    } // for
  }

  public void dumpList() {
    final boolean isDebugEnabled = logger.isTraceEnabled(LogMarker.LRU_CLOCK);
    if (!isDebugEnabled) {
      return;
    }
    synchronized (lock) {
      int idx = 1;
      for (LRUClockNode aNode = this.head; aNode != null; aNode = aNode.nextLRUNode()) {
        if (isDebugEnabled) {
          logger.trace(LogMarker.LRU_CLOCK, "  ({}) {}", (idx++), aNode);
        }
      }
    }
  }

  public long getExpensiveListCount() {
    synchronized (lock) {
      long count = 0;
      for (LRUClockNode aNode = this.head.nextLRUNode(); aNode != this.tail; aNode =
          aNode.nextLRUNode()) {
        count++;
      }
      return count;
    }
  }

  public String getAuditReport() {
    LRUClockNode h = this.head;
    int totalNodes = 0;
    int evictedNodes = 0;
    int usedNodes = 0;
    while (h != null) {
      totalNodes++;
      if (h.testEvicted())
        evictedNodes++;
      if (h.testRecentlyUsed())
        usedNodes++;
      h = h.nextLRUNode();
    }
    StringBuffer result = new StringBuffer(128);
    result.append("LRUList Audit: listEntries = ").append(totalNodes).append(" evicted = ")
        .append(evictedNodes).append(" used = ").append(usedNodes);
    return result.toString();
  }

  /** unsynchronized audit...only run after activity has ceased. */
  public void audit() {
    System.out.println(getAuditReport());
  }

  /** remove an entry from the pipe... (marks it evicted to be skipped later) */
  public boolean unlinkEntry(LRUClockNode entry) {
    if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
      logger.trace(LogMarker.LRU_CLOCK,
          LocalizedMessage.create(LocalizedStrings.NewLRUClockHand_UNLINKENTRY_CALLED, entry));
    }
    entry.setEvicted();
    stats().incDestroys();
    synchronized (lock) {
      LRUClockNode next = entry.nextLRUNode();
      LRUClockNode prev = entry.prevLRUNode();
      if (next == null || prev == null) {
        // not in the list anymore.
        return false;
      }
      next.setPrevLRUNode(prev);
      prev.setNextLRUNode(next);
      entry.setNextLRUNode(null);
      entry.setPrevLRUNode(null);
      this.size--;
    }
    return true;
  }

  /**
   * Get the modifier for lru based statistics.
   *
   * @return The LRUStatistics for this Clock hand's region.
   */
  public LRUStatistics stats() {
    return this.stats;
  }

  /**
   * called when an LRU map is cleared... resets stats and releases prev and next.
   */

  public void clear(RegionVersionVector rvv) {
    if (rvv != null) {
      return; // when concurrency checks are enabled the clear operation removes entries iteratively
    }
    synchronized (this.lock) {
      if (bucketRegion != null) {
        this.stats.decrementCounter(bucketRegion.getCounter());
        bucketRegion.resetCounter();
      } else {
        this.stats.resetCounter();
      }
      initHeadAndTail();
      // LRUClockNode node = this.tail;
      // node.setEvicted();
      //
      // // NYI need to walk the list and call unsetInList for each one.
      //
      // // tail's next should already be null.
      // setHead( node );
    }
  }

  private void initHeadAndTail() {
    // I'm not sure, but I think it's important that we
    // drop the references to the old head and tail on a region clear
    // That will prevent any concurrent operations that are messing
    // with existing nodes from screwing up the head and tail after
    // the clear.
    // Dan 9/23/09
    this.head = new GuardNode();
    this.tail = new GuardNode();
    this.head.setNextLRUNode(this.tail);
    this.tail.setPrevLRUNode(this.head);
    this.size = 0;
  }

  /**
   * Get size of LRU queue
   *
   * @return size
   */
  public int size() {
    return size;
  }

  /** perform work of clear(), after subclass has properly synchronized */
  // private void internalClear() {
  // stats().resetCounter();
  // LRUClockNode node = this.tail;
  // node.setEvicted();
  //
  // // NYI need to walk the list and call unsetInList for each one.
  //
  // // tail's next should already be null.
  // setHead( node );
  // }

  /** Marker class name to identify the lock more easily in thread dumps */
  protected static class HeadLock extends Object {
  }

  private static class GuardNode implements LRUClockNode {

    private LRUClockNode next;
    LRUClockNode prev;

    public int getEntrySize() {
      return 0;
    }

    public LRUClockNode nextLRUNode() {
      return next;
    }

    public LRUClockNode prevLRUNode() {
      return prev;
    }

    public void setEvicted() {

    }

    public void setNextLRUNode(LRUClockNode next) {
      this.next = next;
    }

    public void setPrevLRUNode(LRUClockNode prev) {
      this.prev = prev;
    }

    public void setRecentlyUsed() {}

    public boolean testEvicted() {
      return false;
    }

    public boolean testRecentlyUsed() {
      return false;
    }

    public void unsetEvicted() {}

    public void unsetRecentlyUsed() {}

    public int updateEntrySize(EnableLRU ccHelper) {
      return 0;
    }

    public int updateEntrySize(EnableLRU ccHelper, Object value) {
      return 0;
    }
  }
}

