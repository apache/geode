/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.lru;

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;

/**
 * Represents an entry in an LRU map
 */
public interface LRUEntry extends LRUClockNode, RegionEntry {
  /**
   * If the key is stored as an Object then returns that object;
   * but if the key is stored as primitives then returns null.
   */
  public Object getKeyForSizing();
  public void setDelayedDiskId(LocalRegion r);
}
