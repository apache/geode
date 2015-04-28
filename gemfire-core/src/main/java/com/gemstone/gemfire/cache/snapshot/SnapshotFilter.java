/*=========================================================================
 * Copyright (c) 2012 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.snapshot;

import java.io.Serializable;
import java.util.Map.Entry;

/**
 * Filters region entries for inclusion into a snapshot.
 * 
 * @param <K> the key type
 * @param <V> the value type
 * 
 * @see CacheSnapshotService
 * @see RegionSnapshotService
 * 
 * @author bakera
 * @since 7.0
 */
public interface SnapshotFilter<K, V> extends Serializable {
  /**
   * Returns true if the entry passes the filter criteria; false otherwise.  Invoking 
   * <code>Entry.getValue</code> may incur additional overhead to deserialize the
   * entry value.
   * 
   * @param entry the cache entry to evaluate
   * @return true if the entry is accepted by the filter
   */
  boolean accept(Entry<K, V> entry);
}
  
