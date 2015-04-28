/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;


import com.gemstone.gemfire.internal.cache.lru.LRUEntry;

/**
 * Abstract implementation class of RegionEntry interface.
 * This adds LRU support behavior to entries that already have Disk support.
 *
 * @since 3.5.1
 *
 * @author Darrel Schneider
 *
 */
public abstract class AbstractDiskLRURegionEntry
  extends AbstractOplogDiskRegionEntry
  implements LRUEntry
{
  protected AbstractDiskLRURegionEntry(RegionEntryContext context, Object value) {
    super(context, value);
  }
  
  // Do not add any instance variables to this class.
  // Instead add them to the LRU section of LeafRegionEntry.cpp.
}
