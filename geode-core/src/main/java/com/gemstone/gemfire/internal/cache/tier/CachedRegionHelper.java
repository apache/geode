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
package com.gemstone.gemfire.internal.cache.tier;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.InternalCache;

/**
 * Helper class that maintains a weak hashmap of referenced regions
 *
 * @since GemFire 2.0.2
 */
public class CachedRegionHelper {

  private final InternalCache cache;
  private volatile boolean shutdown = false;
//  private Map regions;
  private volatile int slowEmulationSleep = 0;

  public CachedRegionHelper(InternalCache c) {
    this.cache = c;
//    this.regions = new WeakHashMap();
  }

  public void checkCancelInProgress(Throwable e) 
      throws CancelException {
    cache.getCancelCriterion().checkCancelInProgress(e);
  }
  
  public Region getRegion(String name) {
  /*
    //long start=0, end=0;
    //start = NanoTimer.getTime();
    Region region = (Region) this.regions.get(name);
    //end = NanoTimer.getTime();
    //System.out.println("Got region from map in " + (end-start) + "ns");
    if (region == null) {
      //start = NanoTimer.getTime();
      region = this.cache.getRegion(name);
      //end = NanoTimer.getTime();
      //System.out.println("Got region from cache in " + (end-start) + "ns");
      this.regions.put(name, region);
    }
    return region;
    */
  // To prevent an exception like the following, don't cache the region.
  // It doesn't make much difference anyway.
  //  com.gemstone.gemfire.cache.RegionDestroyedException: com.gemstone.gemfire.internal.cache.DistributedRegion[path='/vmroot/test';scope=DISTRIBUTED_NO_ACK';mirrorType=KEYS_VALUES]
  //  at com.gemstone.gemfire.internal.cache.LocalRegion.checkRegionDestroyed(LocalRegion.java:3633)
  //  at com.gemstone.gemfire.internal.cache.LocalRegion.checkReadiness(LocalRegion.java:1578)
  //  at com.gemstone.gemfire.internal.cache.LocalRegion.entries(LocalRegion.java:1063)
  //  at com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection.fillAndSendRegisterInterestResponseChunks(ServerConnection.java:1075)
  //  at com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection.run(ServerConnection.java:633
  return cache.getRegion(name);
  }
  
  public InternalCache getCache() {
	  return this.cache;
  }

  public void setShutdown(boolean shutdown) {
    this.shutdown = shutdown;
  }

  public boolean isShutdown() {
    return shutdown 
        || cache.getCancelCriterion().cancelInProgress() != null;
  }

  public void close() {
    //cache = null;
    //regions = null;
  }
  
  /**
   * Just ensure that this class gets loaded.
   * 
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    // nothing needed, just make sure this class gets loaded  
  }
  
  public void setEmulateSlowServer(int i) {
    this.slowEmulationSleep = i;
  }
  
  public int emulateSlowServer() {
    return this.slowEmulationSleep;
  }
}
