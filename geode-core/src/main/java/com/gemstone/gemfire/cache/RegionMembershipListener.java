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

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * A listener that can be implemented to handle region membership events.
 * 
 * <p>
 * Instead of implementing this interface it is recommended that you extend
 * the {@link com.gemstone.gemfire.cache.util.RegionMembershipListenerAdapter} class.
 * 
 * 
 * 
 * @see AttributesFactory#addCacheListener
 * @see AttributesFactory#initCacheListeners
 * @see RegionAttributes#getCacheListeners
 * @see AttributesMutator#addCacheListener
 * @see AttributesMutator#removeCacheListener
 * @see AttributesMutator#initCacheListeners
 * @since GemFire 5.0
 */
public interface RegionMembershipListener<K,V> extends CacheListener<K,V> {
  /**
   * Invoked when the listener is first initialized and is
   * given the set of members that have the region created at that time.
   * The listener is initialized when:
   * <ul>
   * <li> the region is created with an already added listener
   * <li> a listener is added using the {@link AttributesMutator}.
   * </ul>
   * @param region the {@link Region} the listener is registered on
   * @param initialMembers an array of the other members that have this region
   *   at the time this listener is added.
   */
  public void initialMembers(Region<K,V> region, DistributedMember[] initialMembers);
  /**
   * Invoked when another member has created the distributed region this
   * listener is on.
   * @param event the event from the member whose region was created.
   */
  public void afterRemoteRegionCreate(RegionEvent<K,V> event);

  /**
   * Invoked when another member's distributed region is no longer
   * available to this cache due to normal operations.  
   * This can be triggered by one of the following methods:
   * <ul>
   * <li>{@link Region#localDestroyRegion()}
   * <li>{@link Region#close}
   * <li>{@link Cache#close()}
   * </ul>
   * This differs from afterRemoteRegionCrash notification in that the
   * departed member performed an action either to remove its region or to close
   * its region or cache.
   * @param event the event from the member whose region is no longer available.
   */
  public void afterRemoteRegionDeparture(RegionEvent<K,V> event);
  /**
   * Invoked when another member's distributed region is no longer
   * available to this cache because the member has crashed or is no
   * longer reachable on the network.<p>

   * @param event the event from the member whose region is no longer available.
   */
  public void afterRemoteRegionCrash(RegionEvent<K,V> event);
}
