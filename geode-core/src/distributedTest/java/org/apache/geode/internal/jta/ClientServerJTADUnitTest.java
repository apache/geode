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
package org.apache.geode.internal.jta;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.naming.NamingException;
import javax.transaction.NotSupportedException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.GemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.cache.tx.ClientTXStateStub;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;


public class ClientServerJTADUnitTest extends JUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();
  private String key = "key";
  private String value = "value";
  private String newKey = "newKey";
  private String newValue = "newValue";
  private final String REGION_NAME = "testRegion";
  final Host host = Host.getHost(0);
  final VM server = host.getVM(0);
  final VM client = host.getVM(1);


  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @After
  public void tearDown() throws Exception {
    closeCache();
  }

  @Test
  public void testClientTXStateStubBeforeCompletion() throws Exception {
    getBlackboard().initBlackboard();
    final Properties properties = getDistributedSystemProperties();

    final int port = server.invoke(() -> createServerRegion(REGION_NAME, properties));

    client.invoke(() -> createClientRegion(host, port, REGION_NAME, false, false));

    createClientRegion(host, port, REGION_NAME, false, false);

    Region region = getCache().getRegion(REGION_NAME);
    assertTrue(region.get(key).equals(value));

    String first = "one";
    String second = "two";

    client.invokeAsync(() -> commitTxWithBeforeCompletion(REGION_NAME, true, first, second));

    getBlackboard().waitForGate(first, 30, TimeUnit.SECONDS);
    TXManagerImpl mgr = (TXManagerImpl) getCache().getCacheTransactionManager();
    mgr.begin();
    region.put(key, newValue);
    TXStateProxyImpl tx = (TXStateProxyImpl) mgr.pauseTransaction();
    ClientTXStateStub txStub = (ClientTXStateStub) tx.getRealDeal(null, null);
    mgr.unpauseTransaction(tx);
    try {
      txStub.beforeCompletion();
      fail("expected to get CommitConflictException");
    } catch (GemFireException e) {
      // expected commit conflict exception thrown from server
      mgr.setTXState(null);
      getBlackboard().signalGate(second);
    }

    // GEODE commit apply the tx change to cache before releasing the locks held, so
    // the region could have the new value but still hold the locks.
    // Add the wait to check new JTA tx can be committed.
    await()
        .until(() -> ableToCommitNewTx(REGION_NAME, mgr));
  }

  private boolean expectionLogged = false;

  private boolean ableToCommitNewTx(final String regionName, TXManagerImpl mgr) {
    try {
      commitTxWithBeforeCompletion(regionName, false, null, null);
    } catch (Exception e) {
      if (!expectionLogged) {
        LogService.getLogger().info("got exception stack trace", e);
        expectionLogged = true;
      }
      mgr.setTXState(null);
      return false;
    }
    return true;
  }

  private CacheServer createCacheServer(Cache cache, int maxThreads) {
    CacheServer server = cache.addCacheServer();
    server.setMaxThreads(maxThreads);
    server.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    try {
      server.start();
    } catch (IOException e) {
      Assert.fail("got exception", e);
    }
    return server;
  }

  private void createClientRegion(final Host host, final int port0, String regionName,
      boolean disableSetOpToStartJTA, boolean isCacheProxy) {
    ClientCacheFactory cf = new ClientCacheFactory();
    cf.addPoolServer(host.getHostName(), port0);
    if (disableSetOpToStartJTA) {
      logger.info("setting system property {} to true ", RESTORE_SET_OPERATION_PROPERTY);
      System.setProperty(RESTORE_SET_OPERATION_PROPERTY, "true");
    }
    ClientCache cache = getClientCache(cf);
    if (isCacheProxy) {
      cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);
    } else {
      cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
    }
  }

  private void commitTxWithBeforeCompletion(String regionName, boolean withWait, String first,
      String second) throws TimeoutException, InterruptedException {
    Region region = getCache().getRegion(regionName);
    TXManagerImpl mgr = (TXManagerImpl) getCache().getCacheTransactionManager();
    mgr.begin();
    region.put(key, newValue);
    TXStateProxyImpl tx = (TXStateProxyImpl) mgr.pauseTransaction();
    ClientTXStateStub txStub = (ClientTXStateStub) tx.getRealDeal(null, null);
    mgr.unpauseTransaction(tx);
    txStub.beforeCompletion();
    if (withWait) {
      getBlackboard().signalGate(first);
      getBlackboard().waitForGate(second, 30, TimeUnit.SECONDS);
    }
    txStub.afterCompletion(Status.STATUS_COMMITTED);
  }

  @Test
  public void testJTAMaxThreads() throws TimeoutException, InterruptedException {
    testJTAWithMaxThreads(1);
  }

  @Test
  public void testJTANoMaxThreadsSetting() throws TimeoutException, InterruptedException {
    testJTAWithMaxThreads(0);
  }

  private void testJTAWithMaxThreads(int maxThreads) {
    getBlackboard().initBlackboard();
    final Properties properties = getDistributedSystemProperties();

    final int port = server.invoke("create cache", () -> {
      Cache cache = getCache(properties);
      CacheServer cacheServer = createCacheServer(cache, maxThreads);
      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
      region.put(key, value);

      return cacheServer.getPort();
    });

    createClientRegion(host, port, REGION_NAME, false, false);

    Region region = getCache().getRegion(REGION_NAME);
    assertTrue(region.get(key).equals(value));

    try {
      commitTxWithBeforeCompletion(REGION_NAME, false, null, null);
    } catch (Exception e) {
      Assert.fail("got unexpected exception", e);
    }
    assertTrue(region.get(key).equals(newValue));
  }

  @Test
  public void testClientCompletedJTAIsInFailoverMap() throws Exception {
    final Properties properties = getDistributedSystemProperties();

    final int port = server.invoke(() -> createServerRegion(REGION_NAME, properties));

    createClientRegion(host, port, REGION_NAME, false, false);

    Region region = getCache().getRegion(REGION_NAME);
    assertTrue(region.get(key).equals(value));

    TransactionManager JTAManager =
        (TransactionManager) getCache().getJNDIContext().lookup("java:/TransactionManager");
    assertNotNull(JTAManager);

    // commit
    JTAManager.begin();
    region.put(key, newValue);
    final TXId committedTXId = getTxId();
    JTAManager.commit();
    assertTrue(region.get(key).equals(newValue));

    server.invoke(() -> verifyJTAIsCompleted(properties, committedTXId));

    // rollback
    JTAManager.begin();
    region.put(key, "UncommittedValue");
    final TXId rolledBackTXId = getTxId();
    JTAManager.rollback();
    assertTrue(region.get(key).equals(newValue));

    server.invoke(() -> verifyJTAIsRollback(properties, rolledBackTXId));
  }

  private Integer createServerRegion(String regionName, Properties properties) {
    Cache cache = getCache(properties);
    CacheServer cacheServer = createCacheServer(cache, 0);
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    region.put(key, value);

    return cacheServer.getPort();
  }

  private TXId getTxId() {
    TXManagerImpl txManager = (TXManagerImpl) getCache().getCacheTransactionManager();
    TXStateProxy txStateProxy = txManager.getTXState();
    return txStateProxy.getTxId();
  }

  private void verifyJTAIsCompleted(Properties properties, TXId committedTXId) {
    Cache cache = getCache(properties);
    assertTrue(((TXManagerImpl) cache.getCacheTransactionManager())
        .isHostedTxRecentlyCompleted(committedTXId));
  }

  private void verifyJTAIsRollback(Properties properties, TXId rollbackTXId) {
    Cache cache = getCache(properties);
    assertEquals(TXCommitMessage.ROLLBACK_MSG, ((TXManagerImpl) cache.getCacheTransactionManager())
        .getRecentlyCompletedMessage(rollbackTXId));

  }

  final String restoreSetOperationTransactionBehavior = "restoreSetOperationTransactionBehavior";
  final String RESTORE_SET_OPERATION_PROPERTY =
      (System.currentTimeMillis() % 2 == 0 ? DistributionConfig.GEMFIRE_PREFIX : "geode.")
          + restoreSetOperationTransactionBehavior;

  @Test
  public void testValuesOnProxyWithPutWhenSetOperationDoesNotStartJTA() throws Exception {
    final Properties properties = getDistributedSystemProperties();
    final int port = server.invoke(() -> createServerRegion(REGION_NAME, properties));
    createClientRegion(host, port, REGION_NAME, true, false);

    Region region = getCache().getRegion(REGION_NAME);
    assertTrue(region.get(key).equals(value));

    verifyRegionValueFailsWhenSetOperationDoesNotStartJTA(region);
  }

  @Test
  public void testValuesOnCacheProxyWithPutWhenSetOperationDoesNotStartJTA() throws Exception {
    final Properties properties = getDistributedSystemProperties();
    final int port = server.invoke(() -> createServerRegion(REGION_NAME, properties));
    createClientRegion(host, port, REGION_NAME, true, true);

    Region region = getCache().getRegion(REGION_NAME);
    assertTrue(region.get(key).equals(value));
    region.localDestroy(key);

    verifyRegionValueFailsWhenSetOperationDoesNotStartJTA(region);
  }

  private void verifyRegionValueFailsWhenSetOperationDoesNotStartJTA(Region region)
      throws NamingException, NotSupportedException, SystemException {
    TransactionManager JTAManager =
        (TransactionManager) getCache().getJNDIContext().lookup("java:/TransactionManager");
    assertNotNull(JTAManager);

    try {
      JTAManager.begin();
      Collection values = region.values();
      assertFalse(values.contains(value));
      region.put(newKey, newValue);
      assertThatThrownBy(() -> values.contains(newValue)).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "The Region collection is not transactional but is being used in a transaction");
    } finally {
      JTAManager.rollback();
    }
  }

  @Test
  public void testValuesOnProxyRegionWithPutWhenSetOperationStartsJTA() throws Exception {
    final Properties properties = getDistributedSystemProperties();
    final int port = server.invoke(() -> createServerRegion(REGION_NAME, properties));
    createClientRegion(host, port, REGION_NAME, false, false);

    Region region = getCache().getRegion(REGION_NAME);
    assertTrue(region.get(key).equals(value));
    Collection collection = region.values();
    // local cache data, not all operation forwarded to server (see GEODE-1887)
    assertFalse(collection.contains(value));

    verifyValuesUsingJTAWhenSetOperationStartsTransaction(region);

    verifyValuesWhenSetOperationStartsTransaction(region);
  }

  @Test
  public void testValuesOnCacheProxyRegionWithPutWhenSetOperationStartsJTA() throws Exception {
    final Properties properties = getDistributedSystemProperties();
    final int port = server.invoke(() -> createServerRegion(REGION_NAME, properties));
    createClientRegion(host, port, REGION_NAME, false, true);

    Region region = getCache().getRegion(REGION_NAME);
    assertTrue(region.get(key).equals(value));
    // local cache data
    Collection values = region.values();
    assertTrue(values.contains(value));
    region.localDestroy(key);
    assertFalse(values.contains(value));

    verifyValuesUsingJTAWhenSetOperationStartsTransaction(region);

    verifyValuesWhenSetOperationStartsTransaction(region);
  }

  private void verifyValuesWhenSetOperationStartsTransaction(Region region) {
    TXManagerImpl txManager = getCache().getTxManager();
    try {
      txManager.begin();
      Collection values = region.values();
      values.contains(value);
      assertEquals(1, values.size());
      region.put(newKey, newValue);
      assertTrue(values.contains(value));
      assertTrue(values.contains(newValue));
      assertEquals(2, values.size());
    } finally {
      txManager.rollback();
    }
  }

  private void verifyValuesUsingJTAWhenSetOperationStartsTransaction(Region region)
      throws NamingException, NotSupportedException, SystemException {
    TransactionManager JTAManager =
        (TransactionManager) getCache().getJNDIContext().lookup("java:/TransactionManager");
    assertNotNull(JTAManager);
    try {
      JTAManager.begin();
      Collection values = region.values();
      // check server under JTA/Transaction
      assertTrue(values.contains(value));
      assertEquals(1, values.size());
      region.put(newKey, newValue);
      assertTrue(values.contains(value));
      assertTrue(values.contains(newValue));
      assertEquals(2, values.size());
    } finally {
      JTAManager.rollback();
    }
  }


  @Test
  public void testEntrySetOnProxyWithPutWhenSetOperationDoesNotStartJTA() throws Exception {
    final Properties properties = getDistributedSystemProperties();
    final int port = server.invoke(() -> createServerRegion(REGION_NAME, properties));
    createClientRegion(host, port, REGION_NAME, true, false);

    Region region = getCache().getRegion(REGION_NAME);
    assertTrue(region.get(key).equals(value));
    Map.Entry entry = region.getEntry(key);

    verifyEntrySetFailsWhenSetOperationDoesNotStartJTA(region, entry);
  }

  @Test
  public void testEntrySetOnCacheProxyWithPutWhenSetOperationDoesNotStartJTA() throws Exception {
    final Properties properties = getDistributedSystemProperties();
    final int port = server.invoke(() -> createServerRegion(REGION_NAME, properties));
    createClientRegion(host, port, REGION_NAME, true, true);

    Region region = getCache().getRegion(REGION_NAME);
    assertTrue(region.get(key).equals(value));
    Map.Entry entry = region.getEntry(key);
    region.localDestroy(key);

    verifyEntrySetFailsWhenSetOperationDoesNotStartJTA(region, entry);
  }

  private void verifyEntrySetFailsWhenSetOperationDoesNotStartJTA(Region region, Map.Entry entry)
      throws NamingException, NotSupportedException, SystemException {
    TransactionManager JTAManager =
        (TransactionManager) getCache().getJNDIContext().lookup("java:/TransactionManager");
    assertNotNull(JTAManager);

    try {
      JTAManager.begin();
      Collection<Map.Entry<String, String>> entrySet = region.entrySet();
      assertEquals(0, entrySet.size());
      region.put(newKey, newValue);
      assertThatThrownBy(() -> entrySet.contains(entry)).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "The Region collection is not transactional but is being used in a transaction");
    } finally {
      JTAManager.rollback();
    }
  }

  @Test
  public void testEntrySetOnProxyRegionWithPutWhenSetOperationStartsJTA() throws Exception {
    final Properties properties = getDistributedSystemProperties();
    final int port = server.invoke(() -> createServerRegion(REGION_NAME, properties));
    createClientRegion(host, port, REGION_NAME, false, false);

    Region region = getCache().getRegion(REGION_NAME);
    assertTrue(region.get(key).equals(value));
    Map.Entry entry = region.getEntry(key);

    verifyEntrySetUsingJTAWhenSetOperationStartsTransaction(region);
  }

  @Test
  public void testEntrySetOnCacheProxyRegionWithPutWhenSetOperationStartsJTA() throws Exception {
    final Properties properties = getDistributedSystemProperties();
    final int port = server.invoke(() -> createServerRegion(REGION_NAME, properties));
    createClientRegion(host, port, REGION_NAME, false, true);

    Region region = getCache().getRegion(REGION_NAME);
    assertTrue(region.get(key).equals(value));
    Map.Entry entry = region.getEntry(key);
    region.localDestroy(key);

    verifyEntrySetUsingJTAWhenSetOperationStartsTransaction(region);
  }

  private void verifyEntrySetUsingJTAWhenSetOperationStartsTransaction(Region region)
      throws NamingException, NotSupportedException, SystemException {
    TransactionManager JTAManager =
        (TransactionManager) getCache().getJNDIContext().lookup("java:/TransactionManager");
    assertNotNull(JTAManager);
    try {
      JTAManager.begin();
      Collection<Map.Entry<String, String>> entrySet = region.entrySet();
      entrySet.forEach((entry) -> {
        assertTrue(entry.getValue().contains(value));
        assertTrue(entry.getKey().contains(key));
      });
      assertEquals(1, entrySet.size());
      region.put(newKey, value);
      entrySet.forEach((entry) -> {
        assertTrue(entry.getValue().contains(value));
      });
      assertEquals(2, entrySet.size());
    } finally {
      JTAManager.rollback();
    }
  }

}
