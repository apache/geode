/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;
/**
 * 
 * @author sbawaska
 *
 */
public abstract class AbstractDiskRegionEntry
  extends AbstractRegionEntry
  implements DiskEntry
{
  protected AbstractDiskRegionEntry(RegionEntryContext context, Object value) {
    super(context, value);
  }
  
  @Override
  public  void setValue(RegionEntryContext context, Object v) throws RegionClearedException {
    setValue(context, v, null);
  }
  
  @Override
  public void setValue(RegionEntryContext context, Object value, EntryEventImpl event) throws RegionClearedException {
    Helper.update(this, (LocalRegion) context, value, event);
    setRecentlyUsed(); // fix for bug #42284 - entry just put into the cache is evicted
  }

  /**
   * Sets the value with a {@link RegionEntryContext}.
   * @param context the value's context.
   * @param value an entry value.
   */
  @Override
  public void setValueWithContext(RegionEntryContext context, Object value) {
    _setValue(value);
    //_setValue(compress(context,value));  // compress is now called in AbstractRegionMap.prepareValueForCache
  }
  
  // Do not add any instances fields to this class.
  // Instead add them to the DISK section of LeafRegionEntry.cpp.
}
