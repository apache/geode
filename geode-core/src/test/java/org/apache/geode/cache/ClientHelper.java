/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.DynamicRegionFactory.Config;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ServerProxy;
import org.apache.geode.internal.cache.LocalRegion;

/**
 * Provides methods for getting at the 
 * bridge client and connection proxies used by a
 * region.
 * 
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
