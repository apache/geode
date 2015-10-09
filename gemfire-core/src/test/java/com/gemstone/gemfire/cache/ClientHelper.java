/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.DynamicRegionFactory.Config;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.ServerProxy;
import com.gemstone.gemfire.internal.cache.LocalRegion;

/**
 * Provides methods for getting at the 
 * bridge client and connection proxies used by a
 * region.
 * 
 * @author dsmith
 *
 */
public class ClientHelper {
  
  public static PoolImpl getPool(Region region) {
    ServerProxy proxy = ((LocalRegion)region).getServerProxy();
    if(proxy == null) {
      return null;
    } else {
      return (PoolImpl) proxy.getPool();
    }
  }
  
  public static Set getActiveServers(Region region) {
    return new HashSet(getPool(region).getCurrentServers());
  }
  
//   public static Set getDeadServers(Region region) {
//   }
  
  private ClientHelper() {
    
  }
  
  public static int getRetryInterval(Region region) {
    return (int)(getPool(region).getPingInterval());
  }

  /**
   * @param region
   */
  public static void release(Region region) {
    
    PoolImpl pool = getPool(region);
    if(pool != null) {
      pool.releaseThreadLocalConnection();
    }
  }

}
