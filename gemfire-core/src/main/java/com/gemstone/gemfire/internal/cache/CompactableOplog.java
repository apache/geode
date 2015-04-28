/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.internal.cache.DiskStoreImpl.OplogCompactor;
import com.gemstone.gemfire.internal.cache.persistence.BytesAndBits;
import com.gemstone.gemfire.internal.cache.persistence.DiskRegionView;
/**
 * Contract that must be implemented by oplogs so that
 * they can be compacted.
 * 
 * @author Darrel Schneider
 * 
 * @since 6.5
 */

public interface CompactableOplog {
  public void prepareForCompact();
  public int compact(OplogCompactor compactor);
  public BytesAndBits getBytesAndBits(DiskRegionView dr, DiskId id,
      boolean faultIn, boolean bitOnly);
  public BytesAndBits getNoBuffer(DiskRegion dr, DiskId id);
}
