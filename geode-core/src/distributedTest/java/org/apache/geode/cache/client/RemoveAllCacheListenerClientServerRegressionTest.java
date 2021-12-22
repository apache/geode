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
package org.apache.geode.cache.client;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class RemoveAllCacheListenerClientServerRegressionTest extends DistributedTestCase {

  private final AtomicInteger serverPort = new AtomicInteger(0);
  private final String REGION_NAME = "TestRegion";
  private VM clientVM = null;
  private VM serverVM = null;
  private final String BB_MAILBOX_KEY = "error";
  private final String NON_EXISTENT_KEY = "nonExistentKey";

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    IgnoredException.addIgnoredException("java.net.ConnectException");
    reset();
  }

  @Override
  public final void preTearDown() throws Exception {
    disconnectAllFromDS();
    reset();
  }

  private void reset() {
    getBlackboard().initBlackboard();
    getBlackboard().setMailbox(BB_MAILBOX_KEY, "");
    clientVM = null;
    serverVM = null;
    serverPort.set(0);
  }

  // ================================================================================
  // replicate on server
  // caching proxy client
  // server concurrency checks enabled
  // client concurrency checks enabled
  @Test
  public void removeAllListenerReplicateCachingProxyServerChecksClientChecksTest() {
    doRemoveAllTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, true, true);
  }

  @Test
  public void destroyListenerReplicateCachingProxyServerChecksClientChecksTest() {
    doDestroyTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, true, true);
  }

  @Test
  public void removeListenerReplicateCachingProxyServerChecksClientChecksTest() {
    doRemoveTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, true, true);
  }

  // ================================================================================
  // partitioned on server
  // caching proxy client
  // server concurrency checks enabled
  // client concurrency checks enabled
  @Test
  public void removeAllListenerPartitionCachingProxyServerChecksClientChecksTest() {
    doRemoveAllTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, true, true);
  }

  @Test
  public void destroyListenerPartitionCachingProxyServerChecksClientChecksTest() {
    doDestroyTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, true, true);
  }

  @Test
  public void removeListenerPartitionCachingProxyServerChecksClientChecksTest() {
    doRemoveTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, true, true);
  }

  // ================================================================================
  // replicate on server
  // caching proxy client
  // server concurrency checks disabled
  // client concurrency checks enabled
  @Test
  public void removeAllListenerReplicateCachingProxyClientChecksOnlyTest() {
    doRemoveAllTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, false, true);
  }

  @Test
  public void destroyListenerReplicateCachingProxyClientChecksOnlyTest() {
    doDestroyTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, false, true);
  }

  @Test
  public void removeListenerReplicateCachingProxyClientChecksOnlyTest() {
    doRemoveTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, false, true);
  }

  // ================================================================================
  // partitioned on server
  // caching proxy client
  // server concurrency checks disabled
  // client concurrency checks enabled
  @Test
  public void removeAllListenerPartitionCachingProxyClientChecksOnlyTest() {
    doRemoveAllTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, false, true);
  }

  @Test
  public void destroyListenerPartitionCachingProxyClientChecksOnlyTest() {
    doDestroyTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, false, true);
  }

  @Test
  public void removeListenerPartitionCachingProxyClientChecksOnlyTest() {
    doRemoveTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, false, true);
  }

  // ================================================================================
  // replicate on server
  // caching proxy client
  // server concurrency checks enabled
  // client concurrency checks disabled
  @Test
  public void removeAllListenerReplicateCachingProxyServerChecksOnlyTest() {
    doRemoveAllTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, true, false);
  }

  @Test
  public void destroyListenerReplicateCachingProxyServerChecksOnlyTest() {
    doDestroyTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, true, false);
  }

  @Test
  public void removeListenerReplicateCachingProxyServerChecksOnlyTest() {
    doRemoveTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, true, false);
  }

  // ================================================================================
  // partitioned on server
  // caching proxy client
  // server concurrency checks enabled
  // client concurrency checks disabled
  @Test
  public void removeAllListenerPartitionCachingProxyServerChecksOnlyTest() {
    doRemoveAllTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, true, false);
  }

  @Test
  public void destroyListenerPartitionCachingProxyServerChecksOnlyTest() {
    doDestroyTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, true, false);
  }

  @Test
  public void removeListenerPartitionCachingProxyServerChecksOnlyTest() {
    doRemoveTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, true, false);
  }

  // ================================================================================
  // replicate on server
  // caching proxy client
  // server concurrency checks disabled
  // client concurrency checks disabled
  @Test
  public void removeAllListenerReplicateCachingProxyChecksDisabledTest() {
    doRemoveAllTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, false, false);
  }

  @Test
  public void destroyListenerReplicateCachingProxyChecksDisabledTest() {
    doDestroyTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, false, false);
  }

  @Test
  public void removeListenerReplicateCachingProxyChecksDisabledTest() {
    doRemoveTest(RegionShortcut.REPLICATE, ClientRegionShortcut.CACHING_PROXY, false, false);
  }

  // ================================================================================
  // partitioned on server
  // caching proxy client
  // server concurrency checks disabled
  // client concurrency checks disabled
  @Test
  public void removeAllListenerPartitionCachingProxyChecksDisabledTest() {
    doRemoveAllTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, false, false);
  }

  @Test
  public void destroyListenerPartitionCachingProxyChecksDisabledTest() {
    doDestroyTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, false, false);
  }

  @Test
  public void removeListenerPartitionCachingProxyChecksDisabledTest() {
    doRemoveTest(RegionShortcut.PARTITION, ClientRegionShortcut.CACHING_PROXY, false, false);
  }

  // ================================================================================
  private void setupGemFireCacheServer(RegionShortcut shortcut, boolean concChecks) {
    serverVM = Host.getHost(0).getVM(0);
    serverPort.set(AvailablePortHelper.getRandomAvailableTCPPort());
    String serverName = getClass().getSimpleName() + "_server";
    serverVM.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        try {
          Cache cache = new CacheFactory().set("name", serverName).set(MCAST_PORT, "0")
              .set(LOG_FILE, serverName + ".log").set(LOG_LEVEL, "config")
              .set("locators", "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]")
              .create();

          RegionFactory<String, String> regionFactory = cache.createRegionFactory(shortcut);
          regionFactory.addCacheListener(new TestListener());
          regionFactory.setConcurrencyChecksEnabled(concChecks);
          regionFactory.create(REGION_NAME);

          CacheServer cacheServer = cache.addCacheServer();
          cacheServer.setPort(serverPort.get());
          cacheServer.setMaxConnections(10);
          cacheServer.start();
          assertTrue("Cache Server is not running!", cacheServer.isRunning());
        } catch (UnknownHostException e) {
          throw new RuntimeException(e);
        } catch (IOException e) {
          throw new RuntimeException("Failed to start cache server " + serverName + " on port "
              + serverPort.get() + ": " + e.getStackTrace());
        }
      }
    });
  }

  private void setupGemFireClientCache(ClientRegionShortcut shortcut, boolean concChecks) {
    clientVM = Host.getHost(0).getVM(1);
    clientVM.invoke(() -> {
      ClientCache clientCache = new ClientCacheFactory().create();

      PoolFactory poolFactory = PoolManager.createFactory();
      poolFactory.setMaxConnections(10);
      poolFactory.setMinConnections(1);
      poolFactory.setReadTimeout(5000);
      poolFactory.setSubscriptionEnabled(true);
      poolFactory.addServer("localhost", serverPort.get());
      Pool pool = poolFactory.create("serverConnectionPool");
      assertNotNull("The 'serverConnectionPool' was not properly configured and initialized!",
          pool);

      ClientRegionFactory<String, String> regionFactory =
          clientCache.createClientRegionFactory(shortcut);
      regionFactory.setPoolName(pool.getName());
      regionFactory.addCacheListener(new TestListener());
      regionFactory.setConcurrencyChecksEnabled(concChecks);
      Region<String, String> aRegion = regionFactory.create(REGION_NAME);
      assertNotNull("The region " + REGION_NAME + " was not properly configured and initialized",
          aRegion);

      aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
    });
  }

  private class TestListener extends CacheListenerAdapter<String, String> {

    @Override
    public void afterDestroy(EntryEvent event) {
      if (event.getKey().equals(NON_EXISTENT_KEY)) {
        String errStr = "event fired for non-existent key " + event.getKey() + "; " + event + "\n"
            + getStackTrace();
        getBlackboard().setMailbox(BB_MAILBOX_KEY, errStr);
      }
    }

  }

  /**
   * Add values to a region, then do a removeAll using both existent and non-existent keys. If a
   * listener event is invoked on a non-existent key, the listener writes it to the blackboard. Read
   * from the blackboard and fail the test if a listener event was invoked for a non-existent key.
   */
  private void doRemoveAllTest(RegionShortcut serverShortcut, ClientRegionShortcut clientShortcut,
      boolean serverConcChecks, boolean clientConcChecks) {
    setupGemFireCacheServer(serverShortcut, serverConcChecks);
    setupGemFireClientCache(clientShortcut, clientConcChecks);
    clientVM.invoke(() -> {
      Region<String, String> aRegion = ClientCacheFactory.getAnyInstance().getRegion(REGION_NAME);
      aRegion.put("key1", "value1");
      aRegion.put("key2", "value2");
      aRegion.put("key3", "value3");

      Set<String> removeAllSet = new HashSet<>();
      removeAllSet.add("key1");
      removeAllSet.add("key2");
      removeAllSet.add(NON_EXISTENT_KEY);
      aRegion.removeAll(removeAllSet);
      Object error = getBlackboard().getMailbox(BB_MAILBOX_KEY);
      assertNotNull(error);
      assertEquals("", error);
    });
  }

  private void doDestroyTest(RegionShortcut serverShortcut, ClientRegionShortcut clientShortcut,
      boolean serverConcChecks, boolean clientConcChecks) {
    setupGemFireCacheServer(serverShortcut, serverConcChecks);
    setupGemFireClientCache(clientShortcut, clientConcChecks);
    clientVM.invoke(() -> {
      Region<String, String> aRegion = ClientCacheFactory.getAnyInstance().getRegion(REGION_NAME);
      try {
        aRegion.destroy(NON_EXISTENT_KEY);
      } catch (EntryNotFoundException e) {
        // expected
      }
      Object error = getBlackboard().getMailbox(BB_MAILBOX_KEY);
      assertNotNull(error);
      assertEquals("", error);
    });
  }

  private void doRemoveTest(RegionShortcut serverShortcut, ClientRegionShortcut clientShortcut,
      boolean serverConcChecks, boolean clientConcChecks) {
    setupGemFireCacheServer(serverShortcut, serverConcChecks);
    setupGemFireClientCache(clientShortcut, clientConcChecks);
    clientVM.invoke(() -> {
      getBlackboard().setMailbox(BB_MAILBOX_KEY, "");
      Region<String, String> aRegion = ClientCacheFactory.getAnyInstance().getRegion(REGION_NAME);
      Object returnedValue = aRegion.remove(NON_EXISTENT_KEY);
      assertNull(returnedValue);
      Object error = getBlackboard().getMailbox(BB_MAILBOX_KEY);
      assertNotNull(error);
      assertEquals("", error);
    });
  }

  /** Get a stack trace of the current stack and return it as a String */
  public static String getStackTrace() {
    try {
      throw new Exception("Exception to get stack trace");
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw, true));
      return sw.toString();
    }
  }

}
