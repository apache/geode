/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * A listener that can be implemented to handle region reliability membership 
 * events.  These are membership events that are specific to loss or gain of
 * required roles as defined by the region's {@link MembershipAttributes}.
 * <p>
 * Instead of implementing this interface it is recommended that you extend
 * the {@link com.gemstone.gemfire.cache.util.RegionRoleListenerAdapter} 
 * class.
 * 
 * @author Kirk Lund
 * 
 * @see AttributesFactory#setCacheListener
 * @see RegionAttributes#getCacheListener
 * @see AttributesMutator#setCacheListener
 * @since 5.0
 */
public interface RegionRoleListener<K,V> extends CacheListener<K,V> {

  /**
   * Invoked when a required role has returned to the distributed system
   * after being absent.
   *
   * @param event describes the member that fills the required role.
   */
  public void afterRoleGain(RoleEvent<K,V> event);
  
  /**
   * Invoked when a required role is no longer available in the distributed
   * system.
   *
   * @param event describes the member that last filled the required role.
   */
  public void afterRoleLoss(RoleEvent<K,V> event);
  
}

