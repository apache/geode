/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * @since GemFire 4.0
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

