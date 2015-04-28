/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.internal.cache.versions.VersionStamp;

/**
 * @author bruce
 *
 */
public abstract class VersionedThinDiskRegionEntry extends VMThinDiskRegionEntry 
  implements VersionStamp {
  
  protected VersionedThinDiskRegionEntry(RegionEntryContext context, Object value) {
    super(context, value);
  }
  // Do not add any instance fields to this class.
  // Instead add them to the VERSIONED section of LeafRegionEntry.cpp.
}
