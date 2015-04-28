/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.cache.partition;

import com.gemstone.gemfire.cache.Region;

/**
 * <p>Utility class that implements all methods in <code>PartitionListener</code>
 * with empty implementations. Applications can subclass this class and only
 * override the methods of interest.<p>
 * 
 * <p>Subclasses declared in a Cache XML file, it must also implement {@link com.gemstone.gemfire.cache.Declarable}
 * </p>
 * 
 * Note : Please contact Pivotal support before using these APIs
 *
 * @author Barry Oglesby
 * 
 * @since 6.6.2
 */
public class PartitionListenerAdapter implements PartitionListener {

  public void afterPrimary(int bucketId) {
  }

  public void afterRegionCreate(Region<?, ?> region) {
  }
  
  public void afterBucketRemoved(int bucketId, Iterable<?> keys) {
  }
  
  public void afterBucketCreated(int bucketId, Iterable<?> keys) {
  }
}
