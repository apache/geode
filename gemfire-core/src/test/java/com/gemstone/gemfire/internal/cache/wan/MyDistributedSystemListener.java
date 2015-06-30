/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;

public class MyDistributedSystemListener implements DistributedSystemListener {

  public int addCount;
  public int removeCount;
  Cache cache;
  
  public MyDistributedSystemListener() {
  }
  
  /**
   * Please note that dynamic addition of the sender id to region is not yet available.  
   */
  public void addedDistributedSystem(int remoteDsId) {
     addCount++;
     List<Locator> locatorsConfigured = Locator.getLocators();
     Locator locator = locatorsConfigured.get(0);
     Map<Integer,Set<DistributionLocatorId>> allSiteMetaData = ((InternalLocator)locator).getlocatorMembershipListener().getAllLocatorsInfo();
     System.out.println("Added : allSiteMetaData : " + allSiteMetaData);
  }
  
  public void removedDistributedSystem(int remoteDsId) {
    removeCount++;
    List<Locator> locatorsConfigured = Locator.getLocators();
    Locator locator = locatorsConfigured.get(0);
    Map<Integer,Set<DistributionLocatorId>> allSiteMetaData = ((InternalLocator)locator).getlocatorMembershipListener().getAllLocatorsInfo();
    System.out.println("Removed : allSiteMetaData : " + allSiteMetaData);
  }

  public int getAddCount() {
    return addCount;
  }
  
  public int getRemoveCount() {
    return removeCount;
  }
   
 
}
