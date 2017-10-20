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

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;

/**
 * NewLIFOClockHand holds the behavior for LIFO logic , Overwriting getLRUEntry() to return most
 * recently added Entry
 * 
 * @since GemFire 5.7
 */

public class NewLIFOClockHand extends NewLRUClockHand {
  /*
   * constructor
   */
  public NewLIFOClockHand(Object region, EnableLRU ccHelper,
      InternalRegionArguments internalRegionArgs) {
    super(region, ccHelper, internalRegionArgs);
  }

  public NewLIFOClockHand(Region region, EnableLRU ccHelper, NewLRUClockHand oldList) {
    super(region, ccHelper, oldList);
  }

  /*
   * Fetch the tail member which should be the last added value and remove it from the list
   */
  protected LRUClockNode getTailEntry() {
    LRUClockNode aNode = null;
    synchronized (this.lock) {
      aNode = this.tail.prevLRUNode();
      if (aNode == this.head) {
        return null; // end of lru list
      }
      // remove entry from list
      LRUClockNode prev = aNode.prevLRUNode();
      prev.setNextLRUNode(this.tail);
      this.tail.setPrevLRUNode(prev);
      aNode.setNextLRUNode(null);
      aNode.setPrevLRUNode(null);
      super.size--;
    }
    return aNode;
  }

  /*
   * return the Entry that is considered most recently used and available to be evicted to overflow
   */
  @Override
  public LRUClockNode getLRUEntry() {
    long numEvals = 0;
    LRUClockNode aNode = null;
    // search for entry to return from list
    for (;;) {
      aNode = getTailEntry();
      // end of Lifo list stop searching
      if (aNode == null) {
        break;
      }
      numEvals++;
      synchronized (aNode) {
        // look for another entry if in transaction
        boolean inUseByTransaction = false;
        if (aNode instanceof AbstractRegionEntry) {
          if (((AbstractRegionEntry) aNode).isInUseByTransaction()) {
            inUseByTransaction = true;
          }
        }
        // if entry NOT used by transaction and NOT evicted return entry
        if (!inUseByTransaction && !aNode.testEvicted()) {
          break;
        }
      }
    }
    this.stats().incEvaluations(numEvals);
    return aNode;
  }
}
