/* =========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.cache.asyncqueue;

import com.gemstone.gemfire.cache.wan.EventSequenceID;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;

/**
 * Represents <code>Cache</code> events delivered to <code>AsyncEventListener</code>.
 * 
 * @author pdeole
 * @since 7.0
 */
public interface AsyncEvent<K, V> extends GatewayQueueEvent<K, V>{
  /**
   * Returns whether possibleDuplicate is set for this event.
   */
  public boolean getPossibleDuplicate();
  
  /**
   * Returns the wrapper over the DistributedMembershipID, ThreadID, SequenceID
   * which are used to uniquely identify any region operation like create, update etc.
   * This helps in sequencing the events belonging to a unique producer.
   * e.g. The EventID can be used to track events received by <code>AsyncEventListener</code>
   * to avoid processing duplicates.
   */
  public EventSequenceID getEventSequenceID();
}
