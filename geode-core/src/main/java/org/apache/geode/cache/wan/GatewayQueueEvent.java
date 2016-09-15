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
package org.apache.geode.cache.wan;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;

/**
 * Represents <code>Cache</code> events going through 
 * <code>GatewaySender</code>s.
 * 
 * 
 * @since GemFire 7.0
 */
public interface GatewayQueueEvent<K, V> {
  /**
   * Returns the <code>Region</code> associated with this AsyncEvent
   * 
   * @return the <code>Region</code> associated with this AsyncEvent
   *         OR null if <code>Region</code> not found (e.g. this can happen 
   *         if it is destroyed).
   */
  public Region<K, V> getRegion();

  /**
   * Returns the <code>Operation</code> that triggered this event.
   * 
   * @return the <code>Operation</code> that triggered this event
   */
  public Operation getOperation();

  /**
   * Returns the callbackArgument associated with this event.
   * 
   * @return the callbackArgument associated with this event
   */
  public Object getCallbackArgument();

  /**
   * Returns the key associated with this event.
   * 
   * @return the key associated with this event
   */
  public K getKey();

  /**
   * Returns the deserialized value associated with this event.
   * 
   * @return the deserialized value associated with this event
   * 
   * @throws IllegalStateException may be thrown if the event's value was stored off-heap
   * and {@link AsyncEventListener#processEvents(java.util.List)} has already returned.
   */
  public V getDeserializedValue();

  /**
   * Returns the serialized form of the value associated with this event.
   * 
   * @return the serialized form of the value associated with this event
   * 
   * @throws IllegalStateException may be thrown if the event's value was stored off-heap
   * and {@link AsyncEventListener#processEvents(java.util.List)} has already returned.
   */
  public byte[] getSerializedValue();
}
