/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management;

import java.util.HashMap;
import java.util.Map;

public class CompositeTestMBean implements CompositeTestMXBean {

  private final String connectionStatsType = "AX";
  private final long connectionsOpened = 100;
  private final long connectionsClosed = 50;
  private final long connectionsAttempted = 120;
  private final long connectionsFailed = 20;
  private final long connectionLifeTime = 100;

  @Override
  public CompositeStats getCompositeStats() {
    return new CompositeStats(connectionStatsType, connectionsOpened, connectionsClosed,
        connectionsAttempted, connectionsFailed, connectionLifeTime);
  }

  @Override
  public CompositeStats listCompositeStats() {
    return new CompositeStats(connectionStatsType, connectionsOpened, connectionsClosed,
        connectionsAttempted, connectionsFailed, connectionLifeTime);
  }

  @Override
  public Map<String, Integer> getMap() {
    Map<String, Integer> testMap = new HashMap<String, Integer>();
    testMap.put("KEY-1", 5);
    return testMap;
  }

  @Override
  public CompositeStats[] getCompositeArray() {
    CompositeStats[] arr = new CompositeStats[2];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = new CompositeStats("AX" + i, connectionsOpened, connectionsClosed,
          connectionsAttempted, connectionsFailed, connectionLifeTime);
    }
    return arr;
  }

  @Override
  public Integer[] getIntegerArray() {
    Integer[] arr = new Integer[2];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = new Integer(0);
    }
    return arr;
  }
}
