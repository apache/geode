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

package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;

/**
 * Interface <code>GatewayEvent</code> represents <code>Cache</code> events
 * delivered to <code>Gateway</code>s.
 *
 * @since GemFire 5.1
 * 
 * @deprecated as of GemFire 8.0. Use {@link com.gemstone.gemfire.cache.wan.GatewayQueueEvent} instead
 * 
 */
@Deprecated
public interface GatewayEvent {

  /**
   * Returns the <code>Region</code> associated with this <code>GatewayEvent</code>.
   * @return the <code>Region</code> associated with this <code>GatewayEvent</code>
   */
  public Region<?,?> getRegion();

  /**
   * Returns the <code>Operation</code> that triggered this event.
   * @return the <code>Operation</code> that triggered this event
   */
  public Operation getOperation();

  /**
   * Returns the callbackArgument associated with this event.
   * @return the callbackArgument associated with this event
   */
  public Object getCallbackArgument();

  /**
   * Returns the key associated with this event.
   * @return the key associated with this event
   */
  public Object getKey();

  /**
   * Returns the deserialized value associated with this event.
   * @return the deserialized value associated with this event
   */
  public Object getDeserializedValue();

  /**
   * Returns the serialized form of the value associated with this event.
   * @return the serialized form of the value associated with this event
   */
  public byte[] getSerializedValue();

  /**
   * Sets whether this event is a possible duplicate.
   * @param possibleDuplicate whether this event is a possible duplicate
   */
  public void setPossibleDuplicate(boolean possibleDuplicate);

  /**
   * Returns whether this event is a possible duplicate.
   * @return whether this event is a possible duplicate
   */
  public boolean getPossibleDuplicate();
  
  /**
   * Returns the creation timestamp in milliseconds.
   * @return the creation timestamp in milliseconds
   * 
   * @since GemFire 6.0
   */
  public long getCreationTime();
}
