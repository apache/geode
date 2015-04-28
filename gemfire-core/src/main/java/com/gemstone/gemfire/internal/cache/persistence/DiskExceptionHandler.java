/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;

/**
 * An interface for handling exceptions that occur at the disk layer, used
 * by the {@link DiskRegion} class. The exception handler is expected to close
 * the region. This interface exists so that ProxyBucketRegions can handle
 * disk access exceptions by passing them on to the parent partition region.
 * @author dsmith
 *
 */
public interface DiskExceptionHandler {
  
  /**
   * @param dae DiskAccessException encountered by the thread
   * @see LocalRegion#handleDiskAccessException(DiskAccessException)
   */
  void handleDiskAccessException(DiskAccessException dae);
}
