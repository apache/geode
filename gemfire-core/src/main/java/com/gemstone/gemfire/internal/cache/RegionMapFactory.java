/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;



/**
 * Used to produce instances of RegionMap
 *
 * @since 3.5.1
 *
 * @author Darrel Schneider
 *
 */
class RegionMapFactory {
  /**
   * Creates a RegionMap that is stored in the VM.
   * @param owner the region that will be the owner of the map
   * @param attrs attributes used to create the map and its entries
   */
  public static RegionMap createVM(LocalRegion owner,
                                   RegionMap.Attributes attrs,InternalRegionArguments internalRegionArgs)
  {
    //final boolean isNotPartitionedRegion = !(owner.getPartitionAttributes() != null || owner
    //.getDataPolicy().withPartitioning());
    if (owner.isProxy() /*|| owner instanceof PartitionedRegion*/) { // TODO enabling this causes eviction tests to fail
      return new ProxyRegionMap(owner, attrs, internalRegionArgs);
    //else if (owner.getEvictionController() != null && isNotPartitionedRegion) {
    } else if (owner.getEvictionController() != null ) {
      return new VMLRURegionMap(owner, attrs,internalRegionArgs);
    } else {
      return new VMRegionMap(owner, attrs, internalRegionArgs);
    }
  }

  /**
   * Creates a RegionMap that is stored in the VM.
   * Called during DiskStore recovery before the region actually exists.
   * @param owner the place holder disk region that will be the owner of the map
   *      until the actual region is created.
   */
  public static RegionMap createVM(PlaceHolderDiskRegion owner,
      DiskStoreImpl ds, InternalRegionArguments internalRegionArgs) {
    RegionMap.Attributes ma = new RegionMap.Attributes();
    ma.statisticsEnabled = owner.getStatisticsEnabled();
    ma.loadFactor = owner.getLoadFactor();
    ma.initialCapacity = owner.getInitialCapacity();
    ma.concurrencyLevel = owner.getConcurrencyLevel();
    if (owner.getLruAlgorithm() != 0) {
      return new VMLRURegionMap(owner, ma, internalRegionArgs);
    }
    else {
      return new VMRegionMap(owner, ma, internalRegionArgs);
    }
  }
}
