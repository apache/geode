/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.internal.cache.lru.*;
//import com.gemstone.gemfire.internal.util.Sizeof;

/**
 * Internal implementation of {@link RegionMap} for regions stored
 * in normal VM memory that maintain an LRU.
 *
 * @since 3.5.1
 *
 * @author Darrel Schneider
 *
 */
final class VMLRURegionMap extends AbstractLRURegionMap {

  VMLRURegionMap(Object owner, Attributes attr,
      InternalRegionArguments internalRegionArgs) {
    super(internalRegionArgs);
    initialize(owner, attr, internalRegionArgs);
  }

  // LRU fields and accessors
  /**
   *  A tool from the eviction controller for sizing entries and
   *  expressing limits.
   */
  private EnableLRU ccHelper;
  /**  The list of nodes in LRU order */
  private NewLRUClockHand lruList;

  @Override
  protected final void _setCCHelper(EnableLRU ccHelper) {
    this.ccHelper = ccHelper;
  }
  @Override
  protected final EnableLRU _getCCHelper() {
    return this.ccHelper;
  }
  @Override
  protected final void _setLruList(NewLRUClockHand lruList) {
    this.lruList = lruList;
  }
  @Override
  public final NewLRUClockHand _getLruList() {
    return this.lruList;
  }
}
