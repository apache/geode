/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.cache;

/**
 * A user-defined object defined in the {@link RegionAttributes} that is called synchronously before
 * a region or entry in the cache is modified. The typical use for a <code>CacheWriter</code> is to
 * update a database. Application writers should implement these methods to execute
 * application-specific behavior before the cache is modified.
 *
 * <p>
 * Before the region is updated via a put, create, or destroy operation, GemFire will call a
 * <code>CacheWriter</code> that is installed anywhere in any participating cache for that region,
 * preferring a local <code>CacheWriter</code> if there is one. Usually there will be only one
 * <code>CacheWriter</code> in the distributed system. If there are multiple
 * <code>CacheWriter</code>s available in the distributed system, the GemFire implementation always
 * prefers one that is stored locally, or else picks one arbitrarily; in any case only one
 * <code>CacheWriter</code> will be invoked.
 *
 * <p>
 * The <code>CacheWriter</code> is capable of aborting the update to the cache by throwing a
 * <code>CacheWriterException</code>. This exception or any runtime exception thrown by the
 * <code>CacheWriter</code> will abort the operation and the exception will be propagated to the
 * initiator of the operation, regardless of whether the initiator is in the same VM as the
 * <code>CacheWriter</code>.
 *
 * <p>
 * WARNING: To avoid risk of deadlock, do not invoke CacheFactory.getAnyInstance() from within any
 * callback methods. Instead use EntryEvent.getRegion().getCache() or
 * RegionEvent.getRegion().getCache().
 *
 * @see AttributesFactory#setCacheWriter
 * @see RegionAttributes#getCacheWriter
 * @see AttributesMutator#setCacheWriter
 * @since GemFire 3.0
 */
public interface CacheWriter<K, V> extends CacheCallback {

  /**
   * Called before an entry is updated. The entry update is initiated by a <code>put</code> or a
   * <code>get</code> that causes the loader to update an existing entry. The entry previously
   * existed in the cache where the operation was initiated, although the old value may have been
   * null. The entry being updated may or may not exist in the local cache where the CacheWriter is
   * installed.
   *
   * @param event an EntryEvent that provides information about the operation in progress
   * @throws CacheWriterException if thrown will abort the operation in progress, and the exception
   *         will be propagated back to caller that initiated the operation
   * @see Region#put(Object, Object)
   * @see Region#get(Object)
   */
  void beforeUpdate(EntryEvent<K, V> event) throws CacheWriterException;

  /**
   * Called before an entry is created. Entry creation is initiated by a <code>create</code>, a
   * <code>put</code>, or a <code>get</code>. The <code>CacheWriter</code> can determine whether
   * this value comes from a <code>get</code> or not by evaluating the
   * {@link CacheEvent#getOperation() Operation}'s {@link Operation#isLoad()} method. The entry
   * being created may already exist in the local cache where this <code>CacheWriter</code> is
   * installed, but it does not yet exist in the cache where the operation was initiated.
   *
   * @param event an EntryEvent that provides information about the operation in progress
   * @throws CacheWriterException if thrown will abort the operation in progress, and the exception
   *         will be propagated back to caller that initiated the operation
   * @see Region#create(Object, Object)
   * @see Region#put(Object, Object)
   * @see Region#get(Object)
   */
  void beforeCreate(EntryEvent<K, V> event) throws CacheWriterException;

  /**
   * Called before an entry is destroyed. The entry being destroyed may or may not exist in the
   * local cache where the CacheWriter is installed. This method is <em>not</em> called as a result
   * of expiration or {@link Region#localDestroy(Object)}.
   *
   * @param event an EntryEvent that provides information about the operation in progress
   * @throws CacheWriterException if thrown will abort the operation in progress, and the exception
   *         will be propagated back to caller that initiated the operation
   *
   * @see Region#destroy(Object)
   */
  void beforeDestroy(EntryEvent<K, V> event) throws CacheWriterException;

  /**
   * Called before a region is destroyed. The <code>CacheWriter</code> will not additionally be
   * called for each entry that is destroyed in the region as a result of a region destroy. If the
   * region's subregions have <code>CacheWriter</code>s installed, then they will be called for the
   * cascading subregion destroys. This method is <em>not</em> called as a result of
   * {@link Region#close}, {@link Cache#close}, or {@link Region#localDestroyRegion()}. However, the
   * {@link Region#close} method is invoked regardless of whether a region is destroyed locally. A
   * non-local region destroy results in an invocation of this method is followed by
   * an invocation of {@link Region#close}.
   * <p>
   * WARNING: This method should not destroy or create any regions itself or a deadlock will occur.
   *
   * @param event a RegionEvent that provides information about the operation
   *
   * @throws CacheWriterException if thrown, will abort the operation in progress, and the exception
   *         will be propagated back to the caller that initiated the operation
   *
   * @see Region#destroyRegion()
   */
  void beforeRegionDestroy(RegionEvent<K, V> event) throws CacheWriterException;

  /**
   * Called before a region is cleared. The <code>CacheWriter</code> will not additionally be called
   * for each entry that is cleared in the region as a result of a region clear.
   *
   * <p>
   * WARNING: This method should not clear/destroy any regions
   *
   *
   * @param event a RegionEvent that provides information about the operation
   *
   * @throws CacheWriterException if thrown, will abort the operation in progress, and the exception
   *         will be propagated back to the caller that initiated the operation
   *
   * @see Region#clear
   */
  void beforeRegionClear(RegionEvent<K, V> event) throws CacheWriterException;
}
