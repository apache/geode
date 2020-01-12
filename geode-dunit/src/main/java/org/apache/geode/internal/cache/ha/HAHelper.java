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
package org.apache.geode.internal.cache.ha;

import java.util.Map;

import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.ha.HARegionQueue.MapWrapper;

/**
 * Helper class to access the required functions of this package from outside the package.
 */
public class HAHelper {

  public static String getRegionQueueName(String proxyId) {
    return HARegionQueue.createRegionName(proxyId);
  }

  public static HARegionQueue getRegionQueue(HARegion hr) {
    return hr.getOwner();
  }

  public static HARegionQueueStats getRegionQueueStats(HARegionQueue hq) {
    return hq.getStatistics();
  }

  public static Map getDispatchMessageMap(Object mapWrapper) {
    return ((MapWrapper) mapWrapper).map;
  }
}
