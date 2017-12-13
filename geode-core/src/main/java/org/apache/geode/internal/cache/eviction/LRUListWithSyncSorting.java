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

import java.util.Optional;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;

public class LRUListWithSyncSorting extends AbstractEvictionList {

  private static final Logger logger = LogService.getLogger();

  private final int maxEntries;

  LRUListWithSyncSorting(InternalEvictionStatistics stats, BucketRegion region) {
    super(stats, region);
    this.maxEntries = readMaxEntriesProperty();
  }

  private int readMaxEntriesProperty() {
    int result = -1;
    Optional<Integer> optionalMaxEntries = SystemPropertyHelper
        .getProductIntegerProperty(SystemPropertyHelper.EVICTION_SEARCH_MAX_ENTRIES);
    if (optionalMaxEntries.isPresent()) {
      result = optionalMaxEntries.get();
    }
    return result;
  }

  /**
   * return the Entry that is considered least recently used. The entry will no longer be in the
   * pipe (unless it is the last empty marker).
   */
  @Override
  public EvictableEntry getEvictableEntry() {
    long numEvals = 0;

    for (;;) {
      EvictionNode aNode = this.unlinkHeadEntry();

      if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
        logger.trace(LogMarker.LRU_CLOCK, "lru considering {}", aNode);
      }

      if (aNode == null) { // hit the end of the list
        this.stats.incEvaluations(numEvals);
        return null;
      }

      numEvals++;

      if (!isEvictable(aNode)) {
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
      } else if (aNode.isRecentlyUsed()) {
        if (logger.isTraceEnabled(LogMarker.LRU_CLOCK)) {
          logger.trace(LogMarker.LRU_CLOCK, LocalizedMessage
              .create(LocalizedStrings.NewLRUClockHand_SKIPPING_RECENTLY_USED_ENTRY, aNode));
        }
        aNode.unsetRecentlyUsed();
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
      return (EvictableEntry) aNode;
    } // synchronized
  } // for

  @Override
  public void incrementRecentlyUsed() {
    // nothing needed
  }
}
