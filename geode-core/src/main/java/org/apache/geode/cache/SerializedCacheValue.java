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
 * Attempts to access off-heap data from this event after it has expired will result in an
 * IllegalStateException.
 *
 * @since GemFire 5.5
 */
public interface SerializedCacheValue<V> {

  /**
   * Returns the raw byte[] that represents this cache value.
   * @return the raw byte[] that represents this cache value
   * @throws IllegalStateException if off-heap and called after the method that was passed the EntryEvent that this serialize value was obtained from returns and this value does not already have the serialized value.
   */
  public byte[] getSerializedValue();

  /**
   * Returns the deserialized object for this cache value.
   * @return the deserialized object for this cache value
   * @throws IllegalStateException if off-heap and called after the method that was passed the EntryEvent that this serialize value was obtained from returns.
   */
  public V getDeserializedValue();
}
