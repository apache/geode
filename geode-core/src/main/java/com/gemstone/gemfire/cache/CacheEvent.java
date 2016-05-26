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
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;

/**
 * A region- or entry-related event affecting the cache.
   * <P>
   * Prior to release <code>6.0</code> the <code>NOT_AVAILABLE</code> constant
   * was used to indicate an object value was not available.
   * However in <code>6.0</code> generic typing was added
   * to {@link Region} and since this constant's type will not be an
   * instance of the generic type <code>V</code> returning it would cause
   * a ClassCastException. So in <code>6.0</code> and later
   * <code>null</code> is now used in place of <code>NOT_AVAILABLE</code>.
 *
 * @see CacheListener
 *
 *
 *
 * @since GemFire 2.0
 */
public interface CacheEvent<K,V> {

  /** Returns the region to which this cached object belongs or
   * the region that raised this event for <code>RegionEvent</code>s.
   * @return the region associated with this object or the region that raised
   * this event.
   */
  public Region<K,V> getRegion();

  /**
   * Return a description of the operation that triggered this event.
   * @return the operation that triggered this event.
   * @since GemFire 5.0
   */
  public Operation getOperation();
  
  /** Returns the callbackArgument passed to the method that generated this event.
   * Provided primarily in case this object or region has already been
   * destroyed. See the {@link Region} interface methods that take a
   * callbackArgument parameter.
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

  /** Answer true if this event originated in a cache other than this one.
   * Answer false if this event originated in this cache.
   * @return true if this event originated remotely
   *
   */
  public boolean isOriginRemote();
  /**
   * Returns the {@link DistributedMember} that this event originated in.
   * @return the member that performed the operation that originated this event.
   * @since GemFire 5.0
   */
  public DistributedMember getDistributedMember();

  /** Answer true if this event resulted from expiration.
   * @return true if this event resulted from expiration
   * @deprecated as of GemFire 5.0, use {@link Operation#isExpiration} instead.
   *
   */
  @Deprecated
  public boolean isExpiration();

  /** Answers true if this event resulted from a distributed operation;
   * false if a local operation.
   * 
   * This is useful to distinguish between invalidate and localInvalidate, and
   * destroy and localDestroy.
   *
   * @return true if this event resulted from a distributed operation
   * @deprecated as of GemFire 5.0, use {@link Operation#isDistributed} instead.
   *
   */
  @Deprecated
  public boolean isDistributed();
}
