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
 * Contains information about an event affecting an entry, including its identity and the the
 * circumstances of the event. It is passed in to <code>CacheListener</code>,
 * <code>CapacityController</code>, and <code>CacheWriter</code>.
 * <p>
 * If this event originated from a region stored off heap then this event can only be used as long
 * as the notification method that obtained it has not returned. For example in your implementation
 * of {@link CacheListener#afterUpdate(EntryEvent)} the event parameter is only valid until your
 * afterUpdate method returns. It is not safe to store instances of this class and use them later
 * when using off heap storage. Attempts to access off-heap data from this event after it has
 * expired will result in an IllegalStateException.
 *
 *
 *
 * @see CacheListener
 * @see CacheWriter
 * @see RegionEvent
 * @since GemFire 3.0
 */
public interface EntryEvent<K, V> extends CacheEvent<K, V> {

  /**
   * Returns the key.
   *
   * @return the key
   */
  K getKey();


  /**
   * Returns the value in the cache prior to this event. When passed to an event handler after an
   * event occurs, this value reflects the value that was in the cache in this VM, not necessarily
   * the value that was in the cache VM that initiated the operation. In certain scenarios the old
   * value may no longer be available in which case <code>null</code> is returned. This can happen
   * for disk regions when the old value is on disk only.
   *
   * @return the old value in the cache prior to this event. If the entry did not exist, was
   *         invalid, or was not available, then null is returned.
   * @throws IllegalStateException if off-heap and called after the method that was passed this
   *         EntryEvent returns.
   */
  V getOldValue();

  /**
   * Returns the serialized form of the value in the cache before this event.
   *
   * @return the serialized form of the value in the cache before this event
   * @throws IllegalStateException if off-heap and called after the method that was passed this
   *         EntryEvent returns.
   *
   * @since GemFire 5.5
   */
  SerializedCacheValue<V> getSerializedOldValue();

  /**
   * Returns the value in the cache after this event.
   *
   * @return the value in the cache after this event
   * @throws IllegalStateException if off-heap and called after the method that was passed this
   *         EntryEvent returns.
   */
  V getNewValue();

  /**
   * Returns the serialized form of the value in the cache after this event.
   *
   * @return the serialized form of the value in the cache after this event
   * @throws IllegalStateException if off-heap and called after the method that was passed this
   *         EntryEvent returns.
   *
   * @since GemFire 5.5
   */
  SerializedCacheValue<V> getSerializedNewValue();

  /**
   * Gets the TransactionId for this EntryEvent.
   *
   * @return the ID of the transaction that performed the operation that generated this event; null
   *         if no transaction involved.
   * @since GemFire 4.0
   */
  TransactionId getTransactionId();

  /**
   * Returns true if this event originated on a client.
   *
   * @since GemFire 5.7
   * @return true if this event originated on a client.
   */
  boolean hasClientOrigin();

  /**
   * Returns <code>true</code> if the old value is "available". Not available means that an old
   * value existed but it could not be obtained or it was deemed too expensive to obtain. Note that
   * {@link #getOldValue} will return <code>null</code> when this method returns <code>false</code>.
   *
   * @return whether the old value is "available"
   *
   * @since GemFire 6.0
   */
  boolean isOldValueAvailable();
}
