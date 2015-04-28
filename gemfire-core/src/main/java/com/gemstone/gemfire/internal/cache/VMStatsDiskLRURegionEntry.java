/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

/**
 * Implementation class of RegionEntry interface.
 * VM -> entries stored in VM memory
 * Stats -> extra statistics
 * Disk -> entries can be on disk 
 * LRU -> entries maintain LRU information
 *
 * @since 3.5.1
 *
 * @author Darrel Schneider
 *
 */
public abstract class VMStatsDiskLRURegionEntry
  extends AbstractDiskLRURegionEntry
{
  protected VMStatsDiskLRURegionEntry(RegionEntryContext context, Object value) {
    super(context, value);
  }
  // Do not add any instance fields to this class.
  // Instead add them to the STATS section of LeafRegionEntry.cpp.
}
