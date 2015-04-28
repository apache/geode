/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.Serializable;

/**
 * Interface <code>Conflatable</code> is used by the bridge server client
 * notification mechanism to conflate messages being sent from the server to the
 * client.
 * 
 * @author Barry Oglesby
 * 
 * @since 4.2
 */
public interface Conflatable extends Serializable
{

  /**
   * Returns whether the object should be conflated
   * 
   * @return whether the object should be conflated
   */
  public boolean shouldBeConflated();

  /**
   * Returns the name of the region for this <code>Conflatable</code>
   * 
   * @return the name of the region for this <code>Conflatable</code>
   */
  public String getRegionToConflate();

  /**
   * Returns the key for this <code>Conflatable</code>
   * 
   * @return the key for this <code>Conflatable</code>
   */
  public Object getKeyToConflate();

  /**
   * Returns the value for this <code>Conflatable</code>
   * 
   * @return the value for this <code>Conflatable</code>
   */
  public Object getValueToConflate();

  /**
   * Sets the latest value for this <code>Conflatable</code>
   * 
   * @param value
   *          The latest value
   */
  public void setLatestValue(Object value);

  /**
   * Return this event's identifier
   * 
   * @return EventID object uniquely identifying the Event
   */
  public EventID getEventId();
}
