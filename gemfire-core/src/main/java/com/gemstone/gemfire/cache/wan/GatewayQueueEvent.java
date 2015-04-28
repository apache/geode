/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.wan;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;

/**
 * Represents <code>Cache</code> events going through 
 * <code>GatewaySender</code>s.
 * 
 * @author Pushkar Deole
 * 
 * @since 7.0
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
   */
  public V getDeserializedValue();

  /**
   * Returns the serialized form of the value associated with this event.
   * 
   * @return the serialized form of the value associated with this event
   */
  public byte[] getSerializedValue();
}
