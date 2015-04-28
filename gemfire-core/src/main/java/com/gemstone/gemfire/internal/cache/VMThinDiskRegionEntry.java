/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;


//import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.distributed.internal.DM;

/**
 * Implementation class of RegionEntry interface.
 * VM -> entries stored in VM memory
 * Thin -> no extra statistics
 * Disk -> entries can be on disk 
 *
 * @since 3.5.1
 *
 * @author Darrel Schneider
 *
 */
public abstract class VMThinDiskRegionEntry
  extends AbstractOplogDiskRegionEntry
{
  protected VMThinDiskRegionEntry(RegionEntryContext context, Object value) {
    super(context, value);
  }
}
