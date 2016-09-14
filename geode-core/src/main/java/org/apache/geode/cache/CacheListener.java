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
package org.apache.geode.cache;

/**
 * <p>
 * A listener to handle region or entry related events.
 * </p>
 * 
 * <p>
 * Instead of implementing this interface it is recommended that you extend the
 * {@link org.apache.geode.cache.util.CacheListenerAdapter} class.
 * </p>
 * 
 * <h4>Avoiding the risk of deadlock</h4>
 * <p>
 * The methods on a <code>CacheListener</code> are invoked while holding a lock
 * on the entry described by the {@link EntryEvent}, as a result if the listener
 * method takes a long time to execute then it will cause the operation that
 * caused it to be invoked to take a long time. In addition, listener code which
 * calls {@link Region} methods could result in a deadlock. For example, in
 * {@link #afterUpdate(EntryEvent)} for entry key k1,
 * {@link Region#put(Object, Object) put(k2, someVal)} is called at the same
 * time {@link #afterUpdate(EntryEvent)} for entry key k2 calls
 * {@link Region#put(Object, Object) put(k1, someVal)} a deadlock may result.
 * This co-key dependency example can be extended to a co-Region dependency
 * where listener code in Region "A" performs Region operations on "B" and
 * listener code in Region "B" performs Region operations on "A". Deadlocks may
 * be either java-level or distributed multi-VM dead locks depending on Region
 * configuration. To be assured of no deadlocks, listener code should cause some
 * other thread to access the region and must not wait for that thread to
 * complete the task.
 * </p>
 * 
 * <h4>Concurrency</h4>
 * <p>
 * Multiple events, on different entries, can cause concurrent invocation of
 * <code>CacheListener</code> methods. Any exceptions thrown by the listener are
 * caught by GemFire and logged.
 * </p>
 * 
 * <h4>Declaring instances in Cache XML files</h4> 
 * <p>
 * To declare a CacheListener in a Cache XML file, it must also implement
 * {@link Declarable}
 * </p>
 * 
 * 
 * @see AttributesFactory#addCacheListener
 * @see AttributesFactory#initCacheListeners
 * @see RegionAttributes#getCacheListeners
 * @see AttributesMutator#addCacheListener
 * @see AttributesMutator#removeCacheListener
 * @see AttributesMutator#initCacheListeners
 * @since GemFire 3.0
 */
public interface CacheListener<K,V> extends CacheCallback {

  /**
   * Handles the event of new key being added to a region. The entry did not
   * previously exist in this region in the local cache (even with a null
   * value).
   * 
   * @param event the EntryEvent
   * @see Region#create(Object, Object)
   * @see Region#put(Object, Object)
   * @see Region#get(Object)
   */
  public void afterCreate(EntryEvent<K,V> event);

  /**
   * Handles the event of an entry's value being modified in a region. This
   * entry previously existed in this region in the local cache, but its
   * previous value may have been null.
   * 
   * @param event the EntryEvent
   * @see Region#put(Object, Object)
   */
  public void afterUpdate(EntryEvent<K,V> event);

  /**
   * Handles the event of an entry's value being invalidated.
   * 
   * @param event the EntryEvent
   * @see Region#invalidate(Object)
   */
  public void afterInvalidate(EntryEvent<K,V> event);

  /**
   * Handles the event of an entry being destroyed.
   * 
   * @param event the EntryEvent
   * @see Region#destroy(Object)
   */
  public void afterDestroy(EntryEvent<K,V> event);

  /**
   * Handles the event of a region being invalidated. Events are not invoked for
   * each individual value that is invalidated as a result of the region being
   * invalidated. Each subregion, however, gets its own
   * <code>regionInvalidated</code> event invoked on its listener.
   * 
   * @param event the RegionEvent
   * @see Region#invalidateRegion()
   * @see Region#localInvalidateRegion()
   */
  public void afterRegionInvalidate(RegionEvent<K,V> event);

  /**
   * Handles the event of a region being destroyed. Events are not invoked for
   * each individual entry that is destroyed as a result of the region being
   * destroyed. Each subregion, however, gets its own
   * <code>afterRegionDestroyed</code> event invoked on its listener.
   * 
   * @param event the RegionEvent
   * @see Region#destroyRegion()
   * @see Region#localDestroyRegion()
   * @see Region#close
   * @see Cache#close()
   */
  public void afterRegionDestroy(RegionEvent<K,V> event);

  /**
   * Handles the event of a region being cleared. Events are not invoked for
   * each individual entry that is removed as a result of the region being
   * cleared.
   * 
   * @param event the RegionEvent
   *
   * @see Region#clear
   * @since GemFire 5.0
   */
  public void afterRegionClear(RegionEvent<K,V> event);
  
  /**
   * Handles the event of a region being created. Events are invoked for
   * each individual region that is created.
   * <p>Note that this method is only called
   * for creates done in the local vm. To be notified of creates done in remote
   * vms use {@link RegionMembershipListener#afterRemoteRegionCreate}.
   * 
   * @param event the RegionEvent
   *
   * @see Cache#createRegion
   * @see Region#createSubregion
   * @since GemFire 5.0
   */
  public void afterRegionCreate(RegionEvent<K,V> event);

  /**
   * Handles the event of a region being live after receiving the marker from the server.
   *
   * @param event the RegionEvent
   * 
   * @see Cache#readyForEvents
   * @since GemFire 5.5
   */
  public void afterRegionLive(RegionEvent<K,V> event);
}
