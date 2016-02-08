/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
