/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.RegionRoleListener;
import com.gemstone.gemfire.cache.RoleEvent;

/**
 * Utility class that implements all methods in 
 * <code>RegionRoleListener</code> with empty implementations. 
 * Applications can subclass this class and only override the methods for 
 * the events of interest.
 * 
 * @author Kirk Lund
 * @since 5.0
 */
public abstract class RegionRoleListenerAdapter<K,V> 
extends RegionMembershipListenerAdapter<K,V>
implements RegionRoleListener<K,V> {

  public void afterRoleGain(RoleEvent<K,V> event) {}
  
  public void afterRoleLoss(RoleEvent<K,V> event) {}

}

