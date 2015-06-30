/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. GoPivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache;

import java.util.Iterator;
import java.util.Map;

/**
 * Interface implemented by an EVICTION BY CRITERIA of
 * {@link CustomEvictionAttributes}. This will be invoked by periodic evictor
 * task that will get the keys to be evicted using this and then destroy from
 * the region to which this is attached.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public interface EvictionCriteria<K, V> {

  /**
   * Get the (key, routing object) of the entries to be evicted from region
   * satisfying EVICTION BY CRITERIA at this point of time.
   * <p>
   * The returned Map.Entry object by the Iterator may be reused internally so
   * caller must extract the key, routing object from the entry on each
   * iteration.
   */
  Iterator<Map.Entry<K, Object>> getKeysToBeEvicted(long currentMillis,
      Region<K, V> region);

  /**
   * Last moment check if an entry should be evicted or not applying the
   * EVICTION BY CRITERIA again under the region entry lock in case the entry
   * has changed after the check in {@link #getKeysToBeEvicted}.
   */
  boolean doEvict(EntryEvent<K, V> event);

  /**
   * Return true if this eviction criteria is equivalent to the other one. This
   * is used to ensure that custom eviction is configured identically on all the
   * nodes of a cluster hosting the region to which this eviction criteria has
   * been attached.
   */
  boolean isEquivalent(EvictionCriteria<K, V> other);
}
