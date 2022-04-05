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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheClient;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.createCacheServer;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.disableShufflingOfEndpoints;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.getCache;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.getClientCache;
import static org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * We have 2 servers and One client which registers some keys with durable interest and some without
 * it. We maintain queues on only One server as redundancy level is one. Following 2 tests have the
 * two TestCase scenarios
 *
 * There are two Tests First Test does the follows : // Step 1: Starting the servers // Step 2:
 * Bring Up the Client // Step 3: Client registers Interests // Step 4: Update Values on the Server
 * for Keys // Step 5: Verify Updates on the Client // Step 6: Close Cache of the DurableClient //
 * Step 7: Update the Values // Step 8: Re-start the Client // Step 9: Verify Updates on the Client
 * // Step 10 : Stop all VMs
 *
 * For Test 2 the steps are as follows // Step 1: Starting the servers // Step 2: Bring Up the
 * Client // Step 3: Client registers Interests // Step 4: Update Values on the Server for Keys //
 * Step 5: Verify Updates on the Client // Step 6: Close Cache of the DurableClient // Step 7:
 * Update the Values // Step 8: Re-start the Client // Step 9: Send Client Ready Message // Step 10:
 * Register all Keys (K1, K2 as Non-Durable. K3, K4 as Durable) // Step 11: Unregister Some Keys
 * (Here K1, K3) // Step 12: Modify values on the server for all the Keys // Step 13: Check the
 * values for the ones not unregistered and the Unregistered Keys' Values should be null
 */
@Category({ClientSubscriptionTest.class})
public class DurableRegistrationDistributedTest extends JUnit4DistributedTestCase {

  private VM server1VM;

  private VM server2VM;

  private VM durableClientVM;

  private String regionName;
  private String hostName;

  private int PORT1;

  private int PORT2;

  private static final String K1 = "KEY_STONE1";

  private static final String K2 = "KEY_STONE2";

  private static final String K3 = "KEY_STONE3";

  private static final String K4 = "KEY_STONE4";

  public DurableRegistrationDistributedTest() {
    super();
  }

  @Override
  public final void postSetUp() {
    server1VM = VM.getVM(0);
    server2VM = VM.getVM(1);
    durableClientVM = VM.getVM(2);
    hostName = VM.getHostName();
    regionName = DurableRegistrationDistributedTest.class.getName() + "_region";
    disableShufflingOfEndpoints();
  }

  @Test
  public void testSimpleDurableClient() {

    // Step 1: Starting the servers
    PORT1 = server1VM.invoke(() -> createCacheServer(regionName, Boolean.TRUE));
    PORT2 = server2VM.invoke(() -> createCacheServer(regionName, Boolean.TRUE));

    // Step 2: Bring Up the Client
    // Start a durable client that is not kept alive on the server when it stops normally
    final String durableClientId = getName() + "_client";

    final int durableClientTimeout = 600; // keep the client alive for 600
    // seconds
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(hostName, PORT1, PORT2, true,
            0),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout),
        Boolean.TRUE));



    // Step 3: Client registers Interests
    // KEY_STONE1, KEY_STONE2 are registered as durableKeys & KEY_STONE3,
    // KEY_STONE4 as non-durableKeys
    durableClientVM.invoke(() -> registerKey(K1, Boolean.FALSE));
    durableClientVM.invoke(() -> registerKey(K2, Boolean.FALSE));
    durableClientVM.invoke(() -> registerKey(K3, Boolean.TRUE));
    durableClientVM.invoke(() -> registerKey(K4, Boolean.TRUE));


    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });

    // Step 4: Update Values on the Server for KEY_STONE1, KEY_STONE2,
    // KEY_STONE3, KEY_STONE4
    server2VM.invoke(() -> putValue(K1, "Value1"));
    server2VM.invoke(() -> putValue(K2, "Value2"));
    server2VM.invoke(() -> putValue(K3, "Value3"));
    server2VM.invoke(() -> putValue(K4, "Value4"));


    GeodeAwaitility.await().atMost(1000, TimeUnit.MILLISECONDS)
        .until(() -> server2VM.invoke(() -> getValue(K1).contentEquals("Value1")));
    // Step 5: Verify Updates on the Client
    assertThat(server2VM.invoke(() -> getValue(K1))).isEqualTo("Value1");
    assertThat(server1VM.invoke(() -> getValue(K1))).isEqualTo("Value1");

    assertThat(durableClientVM.invoke(() -> getValue(K1))).isEqualTo("Value1");
    assertThat(durableClientVM.invoke(() -> getValue(K2))).isEqualTo("Value2");
    assertThat(durableClientVM.invoke(() -> getValue(K3))).isEqualTo("Value3");
    assertThat(durableClientVM.invoke(() -> getValue(K4))).isEqualTo("Value4");

    // Step 6: Close Cache of the DurableClient
    durableClientVM.invoke(this::closeCache);

    // Step 7: Update KEY_STONE1,KEY_STONE2,KEY_STONE3,KEY_STONE4 on the
    // Server say with values PingPong1, PingPong2, PingPong3, PingPong4
    server2VM.invoke(() -> putValue(K1, "PingPong1"));
    server2VM.invoke(() -> putValue(K2, "PingPong2"));
    server2VM.invoke(() -> putValue(K3, "PingPong3"));
    server2VM.invoke(() -> putValue(K4, "PingPong4"));

    // Step 8: Re-start the Client
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(hostName, PORT1, PORT2,
            true, 0),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });

    GeodeAwaitility.await().atMost(5000, TimeUnit.MILLISECONDS)
        .until(() -> durableClientVM.invoke(() -> getValue(K1) == null));

    assertThat(durableClientVM.invoke(() -> getValue(K2))).isNull();

    durableClientVM.invoke(() -> registerKey(K1, Boolean.FALSE));

    GeodeAwaitility.await().atMost(5000, TimeUnit.MILLISECONDS)
        .until(() -> durableClientVM.invoke(() -> getValue(K1) == null));
    assertThat(durableClientVM.invoke(() -> getValue(K2))).isNull();

    server2VM.invoke(() -> putValue(K1, "PingPong_updated_1"));
    server2VM.invoke(() -> putValue(K2, "PingPong_updated_2"));
    server2VM.invoke(() -> putValue(K3, "PingPong_updated_3"));
    server2VM.invoke(() -> putValue(K4, "PingPong_updated_4"));


    // Step 9: Verify Updates on the Client
    GeodeAwaitility.await().atMost(5000, TimeUnit.MILLISECONDS).until(
        () -> durableClientVM.invoke(() -> getValue(K1).contentEquals("PingPong_updated_1")));

    assertThat(durableClientVM.invoke(() -> getValue(K2))).isNull();
    assertThat(durableClientVM.invoke(() -> getValue(K3))).isEqualTo("PingPong_updated_3");
    assertThat(durableClientVM.invoke(() -> getValue(K4))).isEqualTo("PingPong_updated_4");

    // Step 10 : Stop all VMs
    // Stop the durable client
    durableClientVM.invoke(() -> CacheServerTestUtil.closeCache());

    // Stop server 2
    server2VM.invoke(() -> CacheServerTestUtil.closeCache());

    // Stop server 1
    server1VM.invoke(() -> CacheServerTestUtil.closeCache());

  }

  @Test
  public void testSimpleDurableClientWithRegistration() {
    // Step 1: Starting the servers
    PORT1 = server1VM.invoke(() -> createCacheServer(regionName, Boolean.TRUE));
    PORT2 = server2VM.invoke(() -> createCacheServer(regionName, Boolean.TRUE));

    // Step 2: Bring Up the Client
    // Start a durable client that is not kept alive on the server when it stops normally
    final String durableClientId = getName() + "_client";
    // keep the client alive for 600 seconds
    final int durableClientTimeout = 600;
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(hostName, PORT1, PORT2, true,
            0),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)));

    // Step 3: Client registers Interests
    // KEY_STONE1, KEY_STONE2 are registered as durableKeys & KEY_STONE3,
    // KEY_STONE4 as non-durableKeys

    durableClientVM.invoke(() -> registerKey(K1, Boolean.FALSE));
    durableClientVM.invoke(() -> registerKey(K2, Boolean.FALSE));
    durableClientVM.invoke(() -> registerKey(K3, Boolean.TRUE));
    durableClientVM.invoke(() -> registerKey(K4, Boolean.TRUE));

    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });

    // Step 4: Update Values on the Server for KEY_STONE1, KEY_STONE2,
    // KEY_STONE3, KEY_STONE4
    server2VM.invoke(() -> putValue(K1, "Value1"));
    server2VM.invoke(() -> putValue(K2, "Value2"));
    server2VM.invoke(() -> putValue(K3, "Value3"));
    server2VM.invoke(() -> putValue(K4, "Value4"));


    // Step 5: Verify Updates on the Client
    GeodeAwaitility.await().atMost(1000, TimeUnit.MILLISECONDS)
        .until(() -> server2VM.invoke(() -> getValue(K1).contentEquals("Value1")));
    assertThat(server1VM.invoke(() -> getValue(K1))).isEqualTo("Value1");

    assertThat(durableClientVM.invoke(() -> getValue(K1))).isEqualTo("Value1");
    assertThat(durableClientVM.invoke(() -> getValue(K2))).isEqualTo("Value2");
    assertThat(durableClientVM.invoke(() -> getValue(K3))).isEqualTo("Value3");
    assertThat(durableClientVM.invoke(() -> getValue(K4))).isEqualTo("Value4");

    // Step 6: Close Cache of the DurableClient
    durableClientVM.invoke(this::closeCache);

    // Step 7: Update KEY_STONE1,KEY_STONE2,KEY_STONE3,KEY_STONE4 on the
    // Server say with values PingPong1, PingPong2, PingPong3, PingPong4
    server2VM.invoke(() -> putValue(K1, "PingPong1"));
    server2VM.invoke(() -> putValue(K2, "PingPong2"));
    server2VM.invoke(() -> putValue(K3, "PingPong3"));
    server2VM.invoke(() -> putValue(K4, "PingPong4"));

    // Step 8: Re-start the Client
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(hostName, PORT1, PORT2, true, 0),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));


    // Step 9: Register all Keys
    durableClientVM.invoke(() -> registerKey(K3, Boolean.TRUE));
    durableClientVM.invoke(() -> registerKey(K4, Boolean.TRUE));

    durableClientVM.invoke(() -> registerKey(K1, Boolean.FALSE));
    durableClientVM.invoke(() -> registerKey(K2, Boolean.FALSE));


    // Step 10: Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });

    // Step 11: Unregister Some Keys (Here K1, K3)
    durableClientVM.invoke(() -> unregisterKey(K1));
    durableClientVM.invoke(() -> unregisterKey(K3));

    // Step 12: Modify values on the server for all the Keys
    server2VM.invoke(() -> putValue(K1, "PingPong_updated_1"));
    server2VM.invoke(() -> putValue(K2, "PingPong_updated_2"));
    server2VM.invoke(() -> putValue(K3, "PingPong_updated_3"));
    server2VM.invoke(() -> putValue(K4, "PingPong_updated_4"));

    // Step 13: Check the values for the ones not unregistered and the
    // Unregistered Keys' Values should be null
    GeodeAwaitility.await("Prob in KEY_STONE2: ").atMost(5000, TimeUnit.MILLISECONDS).until(
        () -> durableClientVM.invoke(() -> getValue(K2).contentEquals("PingPong_updated_2")));
    assertThat(durableClientVM.invoke(() -> getValue(K4))).describedAs("Prob in KEY_STONE4: ")
        .isEqualTo("PingPong_updated_4");


    assertThat(durableClientVM.invoke(() -> getValue(K1))).describedAs("Prob in KEY_STONE1: ")
        .isNull();
    assertThat(durableClientVM.invoke(() -> getValue(K3))).describedAs("Prob in KEY_STONE1: ")
        .isEqualTo("PingPong3");

    // Stop the durable client
    durableClientVM.invoke(() -> CacheServerTestUtil.closeCache());

    // Stop server 2
    server2VM.invoke(() -> CacheServerTestUtil.closeCache());

    // Stop server 1
    server1VM.invoke(() -> CacheServerTestUtil.closeCache());

  }

  @Test
  public void testDurableClientWithRegistrationHA() {

    // Step 1: Start server1
    PORT1 = server1VM.invoke(() -> createCacheServer(regionName, Boolean.TRUE));
    PORT2 = getRandomAvailableTCPPort();

    // Step 2: Bring Up the Client
    final String durableClientId = getName() + "_client";
    // keep the client alive for 600 seconds
    final int durableClientTimeout = 600;
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(hostName, PORT1, PORT2, true,
            1),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)));

    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });

    // Step 3: Client registers Interests
    durableClientVM.invoke(() -> registerKey(K1, Boolean.FALSE));
    durableClientVM.invoke(() -> registerKey(K2, Boolean.FALSE));
    durableClientVM.invoke(() -> registerKey(K3, Boolean.TRUE));
    durableClientVM.invoke(() -> registerKey(K4, Boolean.TRUE));

    // Step 4: Bring up the server2
    server2VM.invoke(() -> createCacheServer(regionName, Boolean.TRUE, PORT2));

    GeodeAwaitility.await().atMost(3000, TimeUnit.MILLISECONDS)
        .until(() -> server2VM.invoke(() -> getCache().getCacheServers().get(0).isRunning()));


    // Check server2 got all the interests registered by the durable client.
    server2VM.invoke("Verify Interests.", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        logger.info("### Verifying interests registered by DurableClient. ###");
        CacheClientNotifier ccn = CacheClientNotifier.getInstance();
        CacheClientProxy p = null;

        // Get proxy for the client.
        for (int i = 0; i < 60; i++) {
          Iterator<CacheClientProxy> ps = ccn.getClientProxies().iterator();
          if (!ps.hasNext()) {
            Wait.pause(1000);
          } else {
            p = ps.next();
            break;
          }
        }

        if (p == null) {
          fail("Proxy initialization taking long time. Increase the wait time.");
        }

        Iterator<String> rs = p.getInterestRegisteredRegions().iterator();
        String rName = rs.next();
        assertThat(rs).describedAs("Null region Name found.").isNotNull();

        assertThat(p.getCache()).isEqualTo(GemFireCacheImpl.getInstance());
        LocalRegion r = (LocalRegion) GemFireCacheImpl.getInstance().getRegion(rName);
        assertThat(r).describedAs("Null region found.").isNotNull();
        FilterProfile pf = r.getFilterProfile();

        Set<String> interestKeys = pf.getKeysOfInterest(p.getProxyID().getDurableId());
        assertThat(interestKeys).describedAs("durable Interests not found for the proxy")
            .isNotNull();
        assertThat(interestKeys.size())
            .describedAs("The number of durable keys registered during HARegion GII doesn't match.")
            .isEqualTo(2);
        interestKeys = pf.getKeysOfInterest(p.getProxyID());
        assertThat(interestKeys).describedAs("non-durable Interests not found for the proxy")
            .isNotNull();
        assertThat(2)
            .describedAs(
                "The number of non-durable keys registered during HARegion GII doesn't match.")
            .isEqualTo(interestKeys.size());
      }
    });


    // Stop the durable client
    durableClientVM.invoke(() -> CacheServerTestUtil.closeCache());

    // Stop server 2
    server2VM.invoke(() -> CacheServerTestUtil.closeCache());

    // Stop server 1
    server1VM.invoke(() -> CacheServerTestUtil.closeCache());

  }

  @Test
  public void testDurableClientDisConnectWithRegistrationHA() {

    // Step 1: Start server1
    PORT1 = server1VM
        .invoke(() -> createCacheServer(regionName, Boolean.TRUE));
    PORT2 = getRandomAvailableTCPPort();

    // Step 2: Bring Up the Client
    final String durableClientId = getName() + "_client";
    // keep the client alive for 600 seconds
    final int durableClientTimeout = 600;
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(hostName, PORT1, PORT2, true,
            1),
        regionName, getClientDistributedSystemProperties(durableClientId, durableClientTimeout)));

    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });

    // Step 3: Client registers Interests
    durableClientVM.invoke(() -> registerKey(K1, Boolean.FALSE));
    durableClientVM.invoke(() -> registerKey(K2, Boolean.FALSE));
    durableClientVM.invoke(() -> registerKey(K3, Boolean.TRUE));
    durableClientVM.invoke(() -> registerKey(K4, Boolean.TRUE));

    // Close Cache of the DurableClient
    durableClientVM.invoke(this::closeCache);
    GeodeAwaitility.await().until(() -> durableClientVM.invoke(() -> getClientCache().isClosed()));

    // Re-start the Client
    durableClientVM.invoke(() -> createCacheClient(
        getClientPool(hostName, PORT1, PORT2,
            true, 1),
        regionName, getClientDistributedSystemProperties(durableClientId), Boolean.TRUE));

    // Send clientReady message
    durableClientVM.invoke("Send clientReady", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        getClientCache().readyForEvents();
      }
    });

    // Step 4: Bring up the server2
    server2VM
        .invoke(() -> createCacheServer(regionName, Boolean.TRUE, PORT2));

    Wait.pause(3000);

    // Check server2 got all the interests registered by the durable client.
    server2VM.invoke("Verify Interests.", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        logger.info("### Verifying interests registered by DurableClient. ###");
        CacheClientNotifier ccn = CacheClientNotifier.getInstance();
        CacheClientProxy p = null;

        // Get proxy for the client.
        for (int i = 0; i < 60; i++) {
          Iterator<CacheClientProxy> ps = ccn.getClientProxies().iterator();
          if (!ps.hasNext()) {
            Wait.pause(1000);
          } else {
            p = ps.next();
            break;
          }
        }

        if (p == null) {
          fail("Proxy initialization taking long time. Increase the wait time.");
        }

        Iterator<String> rs = p.getInterestRegisteredRegions().iterator();
        String rName = rs.next();
        assertThat(rs).describedAs("Null region Name found.").isNotNull();
        LocalRegion r = (LocalRegion) GemFireCacheImpl.getInstance().getRegion(rName);
        assertThat(r).describedAs("Null region found.").isNotNull();
        FilterProfile pf = r.getFilterProfile();

        Set<String> interestKeys = pf.getKeysOfInterest(p.getProxyID().getDurableId());
        assertThat(interestKeys).describedAs("durable Interests not found for the proxy")
            .isNotNull();
        assertThat(2)
            .describedAs("The number of durable keys registered during HARegion GII doesn't match.")
            .isEqualTo(interestKeys.size());
        interestKeys = pf.getKeysOfInterest(p.getProxyID());
        assertThat(interestKeys).describedAs("non-durable Interests found for the proxy").isNull();
      }
    });


    // Stop the durable client
    durableClientVM.invoke(() -> CacheServerTestUtil.closeCache());

    // Stop server 2
    server2VM.invoke(() -> CacheServerTestUtil.closeCache());

    // Stop server 1
    server1VM.invoke(() -> CacheServerTestUtil.closeCache());

  }

  private void registerKeys() {
    try {
      // Get the region
      Region<String, String> region = getCache()
          .getRegion(DurableRegistrationDistributedTest.class.getName() + "_region");

      assertThat(region).isNotNull();

      region.registerInterest(K1, InterestResultPolicy.KEYS_VALUES, false);
      region.registerInterest(K2, InterestResultPolicy.KEYS_VALUES, false);
      region.registerInterest(K3, InterestResultPolicy.KEYS_VALUES, true);
      region.registerInterest(K4, InterestResultPolicy.KEYS_VALUES, true);

      assertThat(region.getInterestList()).isNotNull();

    } catch (Exception ex) {
      fail("failed while registering interest in registerKey function", ex);
    }
  }

  private String getValue(String key) {
    Region<String, String> r =
        getCache().getRegion(DurableRegistrationDistributedTest.class.getName() + "_region");

    assertThat(r).isNotNull();

    Region.Entry<String, String> re = r.getEntry(key);

    if (re == null) {
      return null;
    } else {
      return re.getValue();
    }
  }

  private void registerKey(String key, boolean isDurable) {
    try {
      // Get the region
      Region<String, String> region = getCache()
          .getRegion(DurableRegistrationDistributedTest.class.getName() + "_region");

      assertThat(region).isNotNull();
      region.registerInterest(key, InterestResultPolicy.NONE, isDurable);
    } catch (Exception ex) {
      fail("failed while registering interest in registerKey function", ex);
    }
  }

  private void unregisterKey(String key) {
    try {
      // Get the region
      Region<String, String> region = getCache()
          .getRegion(DurableRegistrationDistributedTest.class.getName() + "_region");

      assertThat(region).isNotNull();
      region.unregisterInterest(key);
    } catch (Exception ex) {
      fail("failed while registering interest in registerKey function", ex);
    }
  }

  private void putValue(String key, String value) {
    try {
      Region<String, String> r = getCache()
          .getRegion(DurableRegistrationDistributedTest.class.getName() + "_region");

      assertThat(r).isNotNull();
      if (r.getEntry(key) != null) {
        r.put(key, value);
      } else {
        r.create(key, value);
      }
      assertThat(r.getEntry(key).getValue()).isEqualTo(value);
    } catch (Exception e) {

      fail("Put in Server has some fight");

    }
  }

  private Pool getClientPool(String host, int server1Port, int server2Port,
      boolean establishCallbackConnection, int redundancyLevel) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, server1Port).addServer(host, server2Port)
        .setSubscriptionEnabled(establishCallbackConnection)
        .setSubscriptionRedundancy(redundancyLevel);
    return ((PoolFactoryImpl) pf).getPoolAttributes();
  }

  private Properties getClientDistributedSystemProperties(String durableClientId) {
    return getClientDistributedSystemProperties(durableClientId,
        DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT);
  }

  protected int getNumberOfClientProxies() {
    return getBridgeServer().getAcceptor().getCacheClientNotifier().getClientProxies().size();
  }

  private Properties getClientDistributedSystemProperties(String durableClientId,
      int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
    properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(durableClientTimeout));
    return properties;
  }

  private CacheClientProxy getClientProxy() {
    // Get the CacheClientNotifier
    CacheClientNotifier notifier = getBridgeServer().getAcceptor().getCacheClientNotifier();

    // Get the CacheClientProxy or not (if proxy set is empty)
    CacheClientProxy proxy = null;
    Iterator<CacheClientProxy> i = notifier.getClientProxies().iterator();
    if (i.hasNext()) {
      proxy = i.next();
    }
    return proxy;
  }

  private CacheServerImpl getBridgeServer() {
    CacheServerImpl bridgeServer =
        (CacheServerImpl) getCache().getCacheServers().iterator().next();
    assertThat(bridgeServer).isNotNull();
    return bridgeServer;
  }

  public void closeCache() {
    ClientCache cache = getClientCache();
    if (cache != null && !cache.isClosed()) {
      cache.close(true);
    }
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  @Override
  public final void preTearDown() {
    resetDisableShufflingOfEndpointsFlag();
  }
}
