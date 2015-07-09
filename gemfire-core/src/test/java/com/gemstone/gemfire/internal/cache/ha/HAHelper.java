/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.ha;

import java.util.Map;

import com.gemstone.gemfire.internal.cache.HARegion;

/**
 * Helper class to access the required functions of this package from
 * outside the package.
 * @author Girish Thombare
 */

public class HAHelper
{

  public static String getRegionQueueName(String proxyId)
  {
    return HARegionQueue.createRegionName(proxyId.toString());
  }

  public static HARegionQueue getRegionQueue(HARegion hr)
  {
    return hr.getOwner();
  }

  public static HARegionQueueStats getRegionQueueStats(HARegionQueue hq)
  {
    return hq.getStatistics();
  }
  
  public static Map getDispatchMessageMap(Object mapWrapper)
  {
    return ((MapWrapper)mapWrapper).map;
  }
}
