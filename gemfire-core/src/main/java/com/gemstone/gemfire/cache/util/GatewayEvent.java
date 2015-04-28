/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;

/**
 * Interface <code>GatewayEvent</code> represents <code>Cache</code> events
 * delivered to <code>Gateway</code>s.
 *
 * @author Barry Oglesby
 * @since 5.1
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
   * @since 6.0
   */
  public long getCreationTime();
}
