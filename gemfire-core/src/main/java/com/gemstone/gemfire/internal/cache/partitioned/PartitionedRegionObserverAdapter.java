/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.ProxyBucketRegion;

/**
 * This class provides 'do-nothing' implementations of all of the methods of
 * interface PartitionedRegionObserver. See the documentation for class
 * PartitionedRegionObserverHolder for details.
 * 
 * @author Kishor Bachhav
 */

public class PartitionedRegionObserverAdapter implements PartitionedRegionObserver {

  /**
   * This callback is called just before calculating starting bucket id on
   * datastore
   */
  
  public void beforeCalculatingStartingBucketId() {
  }

  @Override
  public void beforeBucketCreation(PartitionedRegion region, int bucketId) {
  }
}
