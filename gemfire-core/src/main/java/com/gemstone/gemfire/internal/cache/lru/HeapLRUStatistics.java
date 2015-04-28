/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.lru;

import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;

/**
 * Statistics for the HeapLRUCapacityController, which treats the
 * counter statistic differently than other flavors of
 * <code>LRUAlgorithms</code>
 *
 * @see com.gemstone.gemfire.internal.cache.lru.MemLRUCapacityController
 * @see com.gemstone.gemfire.internal.cache.lru.LRUCapacityController
 * @author Mitch Thomas
 * @since 4.0
 */
public class HeapLRUStatistics extends LRUStatistics {

  public HeapLRUStatistics(StatisticsFactory factory, 
                              String name, EnableLRU helper) {
    super(factory, name, helper);
  }

  /** Ignore the delta value since the change isn't relevant for heap
   *  related LRU since the counter reflects a percentage of used
   *  memory.  Normally the delta reflects either the number of
   *  entries that changed for the <code>LRUCapacityController</code>
   *  or the estimated amount of memory that has changed after
   *  performing a Region operation.  The
   *  <code>HeapLRUCapacityController</code> however does not care
   *  about <code>Region</code> changes, it only considers heap
   *  changes and uses <code>Runtime</code> to determine how much to
   *  evict.
   * @see com.gemstone.gemfire.internal.cache.lru.HeapLRUCapacityController#createLRUHelper
   * @see EnableLRU#mustEvict
   */
  @Override
  final public void updateCounter( long delta ) {
    super.updateCounter(delta);
  }

  /** The counter for <code>HeapLRUCapacityController</code> reflects
   *  in use heap.  Since you can not programatically reset the amount
   *  of heap usage (at least not directly) this method does <b>NOT</b>
   *  reset the counter value.
   */
  @Override
  final public void resetCounter() {
    super.resetCounter();
  }
}

