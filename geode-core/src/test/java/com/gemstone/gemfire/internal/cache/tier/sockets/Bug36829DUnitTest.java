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

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class Bug36829DUnitTest extends JUnit4DistributedTestCase {

  private VM serverVM;

  private VM ClientVM;

  private String regionName;

  private int PORT;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    this.serverVM = host.getVM(0);
    this.ClientVM = host.getVM(1);
    regionName = Bug36829DUnitTest.class.getName() + "_region";
    CacheServerTestUtil.disableShufflingOfEndpoints();
  }

  @Test
  public void testBug36829() {
    // Step 1: Starting the servers
    final String durableClientId = getName() + "_client";

    final int durableClientTimeout = 600; // keep the client alive for 600

    PORT = ((Integer)this.serverVM.invoke(() -> CacheServerTestUtil.createCacheServer( "DUMMY_REGION", new Boolean(true)
          ))).intValue();

    this.ClientVM.invoke(() -> CacheServerTestUtil.createCacheClient(
            getClientPool(NetworkUtils.getServerHostName(ClientVM.getHost()), PORT, true, 0),
            regionName,
            getClientDistributedSystemProperties(durableClientId,
                durableClientTimeout), Boolean.TRUE ));

    // Send clientReady message
    this.ClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // We expect in registerKey() that the RegionNotFoundException is thrown.
    // If exception is not thrown then the test fails.
    this.ClientVM.invoke(() -> Bug36829DUnitTest.registerKey( "Key1" ));

    // creating Region on the Server
/*    this.serverVM.invoke(() -> CacheServerTestUtil.createRegion( regionName ));
     // should be successful.
    this.ClientVM.invoke(() -> Bug36829DUnitTest.registerKeyAfterRegionCreation( "Key1" ));*/

    // Stop the durable client
    this.ClientVM.invoke(() -> CacheServerTestUtil.closeCache());

    // Stop server 1
    this.serverVM.invoke(() -> CacheServerTestUtil.closeCache());
  }

  private static void registerKey(String key) throws Exception {
    // Get the region
    Region region = CacheServerTestUtil.getCache().getRegion(Bug36829DUnitTest.class.getName() + "_region");
    assertNotNull(region);
    try {
      region.registerInterest(key, InterestResultPolicy.NONE);
      fail("expected ServerOperationException");
    }
    catch (ServerOperationException expected) {
    }
  }

  private static void registerKeyAfterRegionCreation(String key) throws Exception {
    // Get the region
    Region region = CacheServerTestUtil.getCache().getRegion(Bug36829DUnitTest.class.getName() + "_region");
    assertNotNull(region);

    region.registerInterest(key, InterestResultPolicy.NONE);
  }

  private Pool getClientPool(String host, int server1Port,
      boolean establishCallbackConnection, int redundancyLevel) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, server1Port)
      .setSubscriptionEnabled(establishCallbackConnection)
      .setSubscriptionRedundancy(redundancyLevel);
    return ((PoolFactoryImpl)pf).getPoolAttributes();
  }

  private Properties getClientDistributedSystemProperties(
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
  
  @Override
  public final void preTearDown() throws Exception {
    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }
}
