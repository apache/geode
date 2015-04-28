/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.lru;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;

/**
 * NewLIFOClockHand holds the behavior for LIFO logic , Overwriting
 * getLRUEntry() to return most recently added Entry
 * 
 * @author aingle
 * @since 5.7
 */

public class NewLIFOClockHand extends NewLRUClockHand {
  /*
   * constructor
   */
  public NewLIFOClockHand(Object region, EnableLRU ccHelper, InternalRegionArguments internalRegionArgs) {
    super(region,ccHelper,internalRegionArgs);
  }
  
  public NewLIFOClockHand( Region region, EnableLRU ccHelper
      ,NewLRUClockHand oldList){
    super(region,ccHelper,oldList);
  }

  /*
   *  return the Entry that is considered most recently used
   */
  @Override
   public LRUClockNode getLRUEntry() { // new getLIFOEntry
    LRUClockNode aNode = null;
    synchronized (this.lock) {
      aNode = this.tail.prevLRUNode();
      if(aNode == this.head) {
        return null;
      }
      //TODO - Dan 9/23/09 We should probably
      //do something like this to change the tail pointer.
      //But this code wasn't changing the tail before
      //I made this a doubly linked list, and I don't
      //want to change it on this branch.
//      LRUClockNode prev = aNode.prevLRUNode();
//      prev.setNextLRUNode(this.tail);
//      aNode.setNextLRUNode(null);
//      aNode.setPrevLRUNode(null);
    }
    /* no need to update stats here as when this function finished executing 
       next few calls update stats */
    return aNode.testEvicted()? null:aNode;
  }
}
