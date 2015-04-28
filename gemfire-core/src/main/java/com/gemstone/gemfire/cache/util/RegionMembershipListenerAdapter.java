/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionMembershipListener;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Utility class that implements all methods in <code>RegionMembershipListener</code>
 * with empty implementations. Applications can subclass this class and only
 * override the methods for the events of interest.
 * 
 * @author Darrel Schneider
 * 
 * @since 5.0
 */
public abstract class RegionMembershipListenerAdapter<K,V> 
extends CacheListenerAdapter<K,V> 
implements RegionMembershipListener<K,V> {
  public void initialMembers(Region<K,V> r, DistributedMember[] initialMembers) {
  }
  public void afterRemoteRegionCreate(RegionEvent<K,V> event) {
  }
  public void afterRemoteRegionDeparture(RegionEvent<K,V> event) {
  }
  public void afterRemoteRegionCrash(RegionEvent<K,V> event) {
  }
}
