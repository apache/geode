/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management;

import java.util.HashMap;
import java.util.Map;

public class CompositeTestMBean implements CompositeTestMXBean{
  private final String connectionStatsType = "AX";
  private long connectionsOpened  =100;
  private long connectionsClosed = 50;
  private long connectionsAttempted = 120;
  private long connectionsFailed = 20;
  private long connectionLifeTime = 100; 
  
  @Override
  public CompositeStats getCompositeStats() {
    return new CompositeStats(connectionStatsType,connectionsOpened,connectionsClosed,connectionsAttempted,connectionsFailed,connectionLifeTime);
  }

  @Override
  public CompositeStats listCompositeStats() {
    return new CompositeStats(connectionStatsType,connectionsOpened,connectionsClosed,connectionsAttempted,connectionsFailed,connectionLifeTime);
  }

  @Override
  public Map<String, Integer> getMap() {
    Map<String, Integer> testMap = new HashMap<String,Integer>();
    testMap.put("KEY-1", 5);
    return testMap;
  }

  @Override
  public CompositeStats[] getCompositeArray() {
    
    CompositeStats[] arr = new CompositeStats[2];
    for(int i=0 ;i < arr.length; i++){
      arr[i] = new CompositeStats("AX"+i,connectionsOpened,connectionsClosed,connectionsAttempted,connectionsFailed,connectionLifeTime);
    }
    return arr;
  }

  @Override
  public Integer[] getIntegerArray() {
    Integer[] arr = new Integer[2];
    for(int i=0 ;i < arr.length; i++){
      arr[i] = new Integer(0);
    }
    return arr;
  }
}
