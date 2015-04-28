/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import java.util.Iterator;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;


/**
 * This interface is implemented by the iterators
 * GemFireContainer.PRLocalEntriesIterator,
 * PartitionedRegion.PRLocalBucketSetEntriesIterator and
 * PartitionedRegion.KeysSetIterator used by SqlFabric to obtain information of
 * the bucket ID from which the current local entry is being fetched from.
 * 
 * @author Asif
 */
public interface PREntriesIterator<T> extends Iterator<T>{

  /**
   * @return the PartitionedRegion being iterated
   */
  public PartitionedRegion getPartitionedRegion();

  /**
   * @return int bucket ID of the bucket in which the Local Entry resides.
   */
  public int getBucketId();
}
