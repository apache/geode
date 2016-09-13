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
 * Gemfire Context passed to <code>PartitionResolver</code> to compute the
 * data location
 * 
 * 
 * @see PartitionResolver  
 * @since GemFire 6.0
 */
public interface EntryOperation<K,V> {

  /** 
   * Returns the region to which this cached object belongs or
   * the region that raised this event for <code>RegionEvent</code>s.
   * @return the region associated with this object or the region that raised
   * this event.
   */
  public Region<K,V> getRegion();

  /**
   * Return a description of the operation that triggered this event.
   * It may return null and should not be used to generate routing object
   * in {@link PartitionResolver#getRoutingObject(EntryOperation)}
   * @return the operation that triggered this event.
   * @since GemFire 6.0
   * @deprecated
   */
  public Operation getOperation();

  /** 
   * Returns the key.
   * @return the key
   */
  public K getKey();

  /** Returns the callbackArgument passed to the method that generated this event.
   * Provided primarily in case this object or region has already been
   * destroyed. See the {@link Region} interface methods that take a
   * callbackArgument parameter.
   * Only fields on the key should be used when creating the routing object.
   * @return the callbackArgument associated with this event. <code>null</code>
   * is returned if the callback argument is not propagated to the event.
   * This happens for events given to {@link TransactionListener}
   * and to {@link CacheListener} on the remote side of a transaction commit.
   */
  public Object getCallbackArgument();

  /**
   * Returns <code>true</code> if the callback argument is "available".
   * Not available means that the callback argument may have existed but it could
   * not be obtained.
   * Note that {@link #getCallbackArgument} will return <code>null</code>
   * when this method returns <code>false</code>.
   * @since GemFire 6.0
   */
  public boolean isCallbackArgumentAvailable();
  
  /**
   * Returns the value but may return null and should not be used to generate 
   * routing object in {@link PartitionResolver#getRoutingObject(EntryOperation)}.
   *  Only fields on the key should be used when creating the routing object.
   * @return the value.
   * @deprecated
   */
  public V getNewValue();
}
