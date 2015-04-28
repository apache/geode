/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * This interface is used by testing/debugging code to be notified of different
 * events. See the documentation for class PartitionedRegionObserverHolder for
 * details.
 * 
 * @author Kishor Bachhav
 * 
 */

public interface PartitionedRegionObserver {

  /**
   * This callback is called just before calculating starting bucket id on
   * datastore
   */
  public void beforeCalculatingStartingBucketId();
  
  public void beforeBucketCreation(PartitionedRegion region, int bucketId);
}
