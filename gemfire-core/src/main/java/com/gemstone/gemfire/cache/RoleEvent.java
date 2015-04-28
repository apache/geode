/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

import java.util.Set;

/** 
 * Contains information about an event affecting a region reliability, 
 * including its identity and the circumstances of the event. This is 
 * passed in to {@link RegionRoleListener}.
 *
 * @author Kirk Lund
 * @see RegionRoleListener
 * @since 5.0
 */
public interface RoleEvent<K,V> extends RegionEvent<K,V> {
  
  /**
   * Returns the required roles that were lost or gained because of this
   * event.
   */
  public Set<String> getRequiredRoles();
  
}
