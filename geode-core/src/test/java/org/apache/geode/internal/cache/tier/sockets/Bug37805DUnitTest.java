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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * The test is written to verify that the rootRegion() in GemfireCache.java
 * doesn't return any metaRegions or HA Regions.
 */
@Category(DistributedTest.class)
public class Bug37805DUnitTest extends JUnit4DistributedTestCase {

  private VM server1VM;

  private VM durableClientVM;

  private String regionName;

  private int PORT1;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    this.server1VM = host.getVM(0);
    this.durableClientVM = host.getVM(1);
    regionName = "Bug37805_region";
    CacheServerTestUtil.disableShufflingOfEndpoints();
  }
  
  @Override
  public final void preTearDown() throws Exception {
    // Stop server 1
    this.server1VM.invoke(() -> CacheServerTestUtil.closeCache());
    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }
  
  @Test
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
