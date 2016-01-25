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

import java.util.Properties;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

public class Bug36829DUnitTest extends DistributedTestCase {
  private VM serverVM;

  private VM ClientVM;

  private String regionName;

  private int PORT;

  public Bug36829DUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    this.serverVM = host.getVM(0);
    this.ClientVM = host.getVM(1);
    regionName = Bug36829DUnitTest.class.getName() + "_region";
    CacheServerTestUtil.disableShufflingOfEndpoints();
  }

  public void testBug36829() {

    // Step 1: Starting the servers
    final String durableClientId = getName() + "_client";

    final int durableClientTimeout = 600; // keep the client alive for 600

    PORT = ((Integer)this.serverVM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] { "DUMMY_REGION", new Boolean(true)
          })).intValue();

    this.ClientVM.invoke(CacheServerTestUtil.class, "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(ClientVM.getHost()), PORT, true, 0),
            regionName,
            getClientDistributedSystemProperties(durableClientId,
                durableClientTimeout), Boolean.TRUE });

    // Send clientReady message
    this.ClientVM.invoke(new CacheSerializableRunnable("Send clientReady") {
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    // We expect in registerKey() that the RegionNotFoundException is thrown.
    // If exception is not thrown then the test fails.
    this.ClientVM.invoke(Bug36829DUnitTest.class, "registerKey",
        new Object[] { "Key1" });


    // creating REgion on the Server
/*    this.serverVM.invoke(CacheServerTestUtil.class, "createRegion",
        new Object[] { regionName });
     // should be successful.
    this.ClientVM.invoke(Bug36829DUnitTest.class, "registerKeyAfterRegionCreation",
        new Object[] { "Key1" });*/


    // Stop the durable client
    this.ClientVM.invoke(CacheServerTestUtil.class, "closeCache");

    // Stop server 1
    this.serverVM.invoke(CacheServerTestUtil.class, "closeCache");

  }

  
  private static void registerKey(String key) throws Exception {
    try {
      // Get the region
      Region region = CacheServerTestUtil.getCache().getRegion(
          Bug36829DUnitTest.class.getName() + "_region");
      // Region region =
      // CacheServerTestUtil.getCache().getRegion(regionName);
      assertNotNull(region);
      try {
        region.registerInterest(key, InterestResultPolicy.NONE);
        fail("expected ServerOperationException");
      }
      catch (ServerOperationException expected) {
      }
    }
    catch (Exception ex) {
      fail("failed while registering interest in registerKey function", ex);
    }
  }

  private static void registerKeyAfterRegionCreation(String key)
      throws Exception {
    try {
      // Get the region
      Region region = CacheServerTestUtil.getCache().getRegion(
          Bug36829DUnitTest.class.getName() + "_region");
      // Region region =
      // CacheServerTestUtil.getCache().getRegion(regionName);
      assertNotNull(region);
      try {
        region.registerInterest(key, InterestResultPolicy.NONE);
      }
      catch (Exception e) {
        fail("unexpected Exception while registerInterest inspite of region present on the server."
            + " Details of Exception:" + "\ne.getCause:" + e.getCause()
            + "\ne.getMessage:" + e.getMessage());
      }
    }

    catch (Exception ex) {
      fail("failed while registering interest in registerKey function", ex);
    }
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
    properties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.setProperty(DistributionConfig.LOCATORS_NAME, "");
    properties.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME,
        durableClientId);
    properties.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME,
        String.valueOf(durableClientTimeout));
    return properties;
  }
  
  public void tearDown2() throws Exception
  {
    super.tearDown2();
    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }
}
