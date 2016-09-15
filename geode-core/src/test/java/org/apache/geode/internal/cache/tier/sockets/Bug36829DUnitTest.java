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

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class Bug36829DUnitTest extends JUnit4DistributedTestCase {

  private VM serverVM;

  private VM ClientVM;

  private static final String REGION_NAME = "Bug36829_region";

  private int PORT;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    this.serverVM = host.getVM(0);
    this.ClientVM = host.getVM(1);

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
            getClientPool(NetworkUtils.getServerHostName(ClientVM.getHost()), PORT, true, 0), REGION_NAME,
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
/*    this.serverVM.invoke(() -> CacheServerTestUtil.createRegion( REGION_NAME ));
     // should be successful.
    this.ClientVM.invoke(() -> Bug36829DUnitTest.registerKeyAfterRegionCreation( "Key1" ));*/

    // Stop the durable client
    this.ClientVM.invoke(() -> CacheServerTestUtil.closeCache());

    // Stop server 1
    this.serverVM.invoke(() -> CacheServerTestUtil.closeCache());
  }

  private static void registerKey(String key) throws Exception {
    // Get the region
    Region region = CacheServerTestUtil.getCache().getRegion(REGION_NAME);
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
    Region region = CacheServerTestUtil.getCache().getRegion(REGION_NAME);
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
