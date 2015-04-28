/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/**
 * Class <code>SerializedCacheValue</code> represents a serialized cache value.
 * Instances of this class obtained from a region can be put into another region
 * without a copy of this value being made. The two region entries will both have a
 * reference to the same value.
 * <p>
 * If this value originated from a region stored off heap then this object can
 * only be used as long as notification method that obtained it has not returned.
 * For example if your implementation of {@link CacheListener#afterUpdate(EntryEvent)} obtains one
 * by calling {@link EntryEvent#getSerializedOldValue()} then the SerializedCacheValue returned
 * is only valid until your afterUpdate method returns. It is not safe to store instances of this
 * class and use them later when using off heap storage.
 *
 * @author Barry Oglesby
 * @since 5.5
 */
public interface SerializedCacheValue<V> {

  /**
   * Returns the raw byte[] that represents this cache value.
   * @return the raw byte[] that represents this cache value
   */
  public byte[] getSerializedValue();

  /**
   * Returns the deserialized object for this cache value.
   * @return the deserialized object for this cache value
   */
  public V getDeserializedValue();
}
