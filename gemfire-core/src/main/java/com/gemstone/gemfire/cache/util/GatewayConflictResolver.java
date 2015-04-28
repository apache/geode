/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;



/**
 * GatewayConflictResolver is a Cache-level plugin that is called upon to decide what to do
 * with events that originate in other systems and arrive through the WAN Gateway.  A
 * GatewayConflictResolver is invoked if the current value in a cache entry was established by a
 * different distributed system (with a different distributed-system-id) than an event
 * that is attempting to modify the entry.  It is not invoked if the event has the same
 * distributed system ID as the event that last changed the entry.
 * @since 7.0
 * @author Bruce Schuchardt
 */
public interface GatewayConflictResolver {
  /**
   * This method is invoked when a change is received from another distributed system and
   * the last modification to the affected cache entry did not also come from the same system.
   * <p>
   * The given GatewayConflictHelper can be used to allow the change to be made to the cache,
   * disallow the modification or make a change to the value to be stored in the cache.
   * <p>This method is invoked under synchronization on the cache entry in
   * order to prevent it from concurrent modification</p>
   * <p>For any two events, all GatewayConflictResolvers must make the same decision
   * on the resolution of the conflict in order to maintain consistency.  They must
   * do so regardless of the order of the events.</p>
   * @param event the event that is in conflict with the current cache state
   * @param helper an object to be used in modifying the course of action for this event
   */
  public void onEvent(TimestampedEntryEvent event, GatewayConflictHelper helper);

}
