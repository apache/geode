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

//import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.RegionEntry;
//import com.gemstone.gemfire.internal.cache.LocalRegion;

/** The lru action on the map for evicting items must be called while the current thread is free of any map synchronizations.
 */
public interface LRUMapCallbacks {

  /** to be called by LocalRegion after any synchronization surrounding a map.put or map.replace call is made.
   * This will then perform the lru removals, or region.localDestroy() calls to make up for the recent addition. 
   */
  public void lruUpdateCallback();

  public void lruUpdateCallback(int n);
  /**
   * Disables lruUpdateCallback in calling thread
   * @return false if it's already disabled
   */
  public boolean disableLruUpdateCallback();
  /**
   * Enables lruUpdateCallback in calling thread
   */
  public void enableLruUpdateCallback();

  /** if an exception occurs between an LRUEntriesMap put and the call to lruUpdateCallback, then this must be
   * called to allow the thread to continue to work with other regions.
   */
  public void resetThreadLocals();

  /**
   * Return true if the lru has exceeded its limit and needs to evict.
   * Note that this method is currently used to prevent disk recovery from faulting
   * in values once the limit is exceeded.
   */
  public boolean lruLimitExceeded();

  public void lruCloseStats();
  
  /**
   * Called when an entry is faulted in from disk.
   */
  public void lruEntryFaultIn(LRUEntry entry);
}
