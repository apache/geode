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


/**
 * NewLIFOClockHand holds the behavior for LIFO logic , Overwriting getLRUEntry() to return most
 * recently added Entry.
 *
 * @since GemFire 5.7
 */
public class LIFOList extends AbstractEvictionList {

  public LIFOList(EvictionController controller) {
    super(controller);
  }

  /**
   * Return the Entry that is considered most recently used and available to be evicted to overflow.
   * Note that this implementation basically just returns the most recent thing added to the list.
   * So, unlike the parent class, is does no scanning based on the recentlyUsed bit. This is a
   * perfect implementation for our queues (gateway, client subscription) as long as they never
   * update something already in the queue. Since updates simply set the recentlyUsed bit then the
   * most recent node may be the one that was just updated and not moved to the tail of the list.
   */
  @Override
  public EvictableEntry getEvictableEntry() {
    long evaluations = 0;
    EvictionNode evictionNode;
    // search for entry to return from list
    for (;;) {
      evictionNode = unlinkTailEntry();
      // end of Lifo list stop searching
      if (evictionNode == null) {
        break;
      }
      evaluations++;
      synchronized (evictionNode) {
        // if entry NOT used by transaction and NOT evicted return entry
        if (!evictionNode.isInUseByTransaction() && !evictionNode.isEvicted()) {
          break;
        }
      }
    }
    getStatistics().incEvaluations(evaluations);
    return (EvictableEntry) evictionNode;
  }

  @Override
  public void incrementRecentlyUsed() {
    // nothing
  }
}
