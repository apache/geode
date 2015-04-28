/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
