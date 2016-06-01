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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

/**
 * 
 *
 * The test is written to verify that the rootRegion() in GemfireCache.java
 * doesn't return any metaRegions or HA Regions.
 * 
 */

public class Bug37805DUnitTest extends DistributedTestCase{

  private VM server1VM;

  private VM durableClientVM;

  private String regionName;

  private int PORT1;

  public Bug37805DUnitTest(String name) {
    super(name);
  }

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    this.server1VM = host.getVM(0);
    this.durableClientVM = host.getVM(1);
    regionName = Bug37805DUnitTest.class.getName() + "_region";
    CacheServerTestUtil.disableShufflingOfEndpoints();
  }
  
  @Override
  public final void preTearDown() throws Exception {
    // Stop server 1
    this.server1VM.invoke(() -> CacheServerTestUtil.closeCache());
    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }
  
  public void testFunctionality() {
 // Step 1: Starting the servers

    PORT1 = ((Integer)this.server1VM.invoke(() -> CacheServerTestUtil.createCacheServer( regionName, new Boolean(true)
            ))).intValue();
    final int durableClientTimeout = 600; 
    
    
    // Step 2: Starting Client and creating durableRegion
    final String durableClientId = getName() + "_client";

    this.durableClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
            getClientPool(NetworkUtils.getServerHostName(durableClientVM.getHost()), PORT1, true, 0),
            regionName,
            getDurableClientDistributedSystemProperties(durableClientId,
                durableClientTimeout), Boolean.TRUE ));

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable(
        "Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });
    
    this.server1VM.invoke(() -> Bug37805DUnitTest.checkRootRegions());
    
    
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache());
  }
  
  public static void checkRootRegions() {
    Set rootRegions = CacheServerTestUtil.getCache().rootRegions();
    if(rootRegions != null) {
      for(Iterator itr = rootRegions.iterator(); itr.hasNext(); ){
        Region region = (Region)itr.next();
        if (region instanceof HARegion)
          fail("region of HARegion present");
      }
    }
    //assertNull(rootRegions);
    //assertIndexDetailsEquals(0,((Collection)CacheServerTestUtil.getCache().rootRegions()).size());
  }
  
  private Pool getClientPool(String host, int server1Port,
      boolean establishCallbackConnection, int redundancyLevel) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, server1Port)
      .setSubscriptionEnabled(establishCallbackConnection)
      .setSubscriptionRedundancy(redundancyLevel);
    return ((PoolFactoryImpl)pf).getPoolAttributes();
  }

  private Properties getDurableClientDistributedSystemProperties(
      String durableClientId, int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    properties.setProperty(DURABLE_CLIENT_ID,
        durableClientId);
    properties.setProperty(DURABLE_CLIENT_TIMEOUT,
        String.valueOf(durableClientTimeout));
    return properties;
  }
}
