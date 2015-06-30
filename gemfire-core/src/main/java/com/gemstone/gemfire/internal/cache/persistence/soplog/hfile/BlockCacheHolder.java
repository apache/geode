/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog.hfile;

import org.apache.hadoop.hbase.io.hfile.BlockCache;

import com.gemstone.gemfire.internal.cache.persistence.soplog.HFileStoreStatistics;

public class BlockCacheHolder {
  private BlockCache cache;
  private HFileStoreStatistics stats;
  
  public BlockCacheHolder(HFileStoreStatistics stats, BlockCache cache) {
    this.stats = stats;
    this.cache = cache;
  }

  public synchronized BlockCache getBlockCache() {
    return cache;
  }
  
  public synchronized HFileStoreStatistics getHFileStoreStats() {
    return stats;
  }
}
