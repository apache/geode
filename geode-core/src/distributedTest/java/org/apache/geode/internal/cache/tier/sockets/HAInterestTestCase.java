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

import static org.apache.geode.cache.InterestResultPolicy.KEYS;
import static org.apache.geode.cache.Region.Entry;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.client.internal.RegisterInterestTracker.interestListIndex;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.cache.tier.InterestType.KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Tests Interest Registration Functionality
 */
@Category({ClientSubscriptionTest.class})
@SuppressWarnings({"deprecation", "rawtypes", "serial", "unchecked"})
public class HAInterestTestCase extends JUnit4DistributedTestCase {

  protected static final int TIMEOUT_MILLIS = 60 * 1000;
  protected static final int INTERVAL_MILLIS = 10;

  protected static final String REGION_NAME = "HAInterestBaseTest_region";

  protected static final String k1 = "k1";
  protected static final String k2 = "k2";
  protected static final String client_k1 = "client-k1";
  protected static final String client_k2 = "client-k2";
  protected static final String server_k1 = "server-k1";
  protected static final String server_k2 = "server-k2";
  protected static final String server_k1_updated = "server_k1_updated";

  protected static Cache cache = null;
  protected static PoolImpl pool = null;
  protected static Connection conn = null;

  protected static int PORT1;
  protected static int PORT2;
  protected static int PORT3;

  protected static boolean isBeforeRegistrationCallbackCalled = false;
  protected static boolean isBeforeInterestRecoveryCallbackCalled = false;
  protected static boolean isAfterRegistrationCallbackCalled = false;

  protected static Host host = null;
  protected static VM server1 = null;
  protected static VM server2 = null;
  protected static VM server3 = null;

  protected static volatile boolean exceptionOccurred = false;

  @Override
  public final void postSetUp() throws Exception {
    host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server3 = host.getVM(2);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    // start servers first
    PORT1 = ((Integer) server1.invoke(() -> HAInterestTestCase.createServerCache())).intValue();
    PORT2 = ((Integer) server2.invoke(() -> HAInterestTestCase.createServerCache())).intValue();
    PORT3 = ((Integer) server3.invoke(() -> HAInterestTestCase.createServerCache())).intValue();
    exceptionOccurred = false;
    IgnoredException.addIgnoredException("java.net.ConnectException: Connection refused: connect");
  }

  @Override
  public final void preTearDown() throws Exception {
    // close the clients first
    closeCache();

    // then close the servers
    server1.invoke(() -> HAInterestTestCase.closeCache());
    server2.invoke(() -> HAInterestTestCase.closeCache());
    server3.invoke(() -> HAInterestTestCase.closeCache());
    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }

  public static void closeCache() {
    PoolImpl.AFTER_REGISTER_CALLBACK_FLAG = false;
    PoolImpl.BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;
    PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = false;
    PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = false;
    HAInterestTestCase.isAfterRegistrationCallbackCalled = false;
    HAInterestTestCase.isBeforeInterestRecoveryCallbackCalled = false;
    HAInterestTestCase.isBeforeRegistrationCallbackCalled = false;
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
    cache = null;
    pool = null;
    conn = null;
  }

  /**
   * Return the current primary waiting for a primary to exist.
   *
   * @since GemFire 5.7
   */
  public static VM getPrimaryVM() {
    return getPrimaryVM(null);
  }

  /**
   * Return the current primary waiting for a primary to exist and for it not to be the oldPrimary
   * (if oldPrimary is NOT null).
   *
   * @since GemFire 5.7
   */
  public static VM getPrimaryVM(final VM oldPrimary) {
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        int primaryPort = pool.getPrimaryPort();
        if (primaryPort == -1) {
          return false;
        }
        // we have a primary
        VM currentPrimary = getServerVM(primaryPort);
        if (currentPrimary != oldPrimary) {
          return true;
        }
        return false;
      }

      @Override
      public String description() {
        return "waiting for primary";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    int primaryPort = pool.getPrimaryPort();
    assertTrue(primaryPort != -1);
    VM currentPrimary = getServerVM(primaryPort);
    assertTrue(currentPrimary != oldPrimary);
    return currentPrimary;
  }

  public static VM getBackupVM() {
    return getBackupVM(null);
  }

  public static VM getBackupVM(VM stoppedBackup) {
    VM currentPrimary = getPrimaryVM(null);
    if (currentPrimary != server2 && server2 != stoppedBackup) {
      return server2;
    } else if (currentPrimary != server3 && server3 != stoppedBackup) {
      return server3;
    } else if (currentPrimary != server1 && server1 != stoppedBackup) {
      return server1;
    } else {
      fail("expected currentPrimary " + currentPrimary + " to be " + server1 + ", or " + server2
          + ", or " + server3);
      return null;
    }
  }

  /**
   * Given a server vm (server1, server2, or server3) return its port.
   *
   * @since GemFire 5.7
   */
  public static int getServerPort(VM vm) {
    if (vm == server1) {
      return PORT1;
    } else if (vm == server2) {
      return PORT2;
    } else if (vm == server3) {
      return PORT3;
    } else {
      fail("expected vm " + vm + " to be " + server1 + ", or " + server2 + ", or " + server3);
      return -1;
    }
  }

  /**
   * Given a server port (PORT1, PORT2, or PORT3) return its vm.
   *
   * @since GemFire 5.7
   */
  public static VM getServerVM(int port) {
    if (port == PORT1) {
      return server1;
    } else if (port == PORT2) {
      return server2;
    } else if (port == PORT3) {
      return server3;
    } else {
      fail("expected port " + port + " to be " + PORT1 + ", or " + PORT2 + ", or " + PORT3);
      return null;
    }
  }

  public static void verifyRefreshedEntriesFromServer() {
    final Region r1 = cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(r1);

    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        Entry re = r1.getEntry(k1);
        if (re == null)
          return false;
        Object val = re.getValue();
        return client_k1.equals(val);
      }

      @Override
      public String description() {
        return "waiting for client_k1 refresh from server";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    wc = new WaitCriterion() {
      @Override
      public boolean done() {
        Entry re = r1.getEntry(k2);
        if (re == null)
          return false;
        Object val = re.getValue();
        return client_k2.equals(val);
      }

      @Override
      public String description() {
        return "waiting for client_k2 refresh from server";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  public static void verifyDeadAndLiveServers(final int expectedDeadServers,
      final int expectedLiveServers) {
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return pool.getConnectedServerCount() == expectedLiveServers;
      }

      @Override
      public String description() {
        return "waiting for pool.getConnectedServerCount() == expectedLiveServer";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  public static void putK1andK2() {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    r1.put(k1, server_k1);
    r1.put(k2, server_k2);
  }

  public static void setClientServerObserverForBeforeInterestRecoveryFailure() {
    PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void beforeInterestRecovery() {
        synchronized (HAInterestTestCase.class) {
          Thread t = new Thread() {
            public void run() {
              getBackupVM().invoke(() -> HAInterestTestCase.startServer());
              getPrimaryVM().invoke(() -> HAInterestTestCase.stopServer());
            }
          };
          t.start();
          try {
            ThreadUtils.join(t, 30 * 1000);
          } catch (Exception ignore) {
            exceptionOccurred = true;
          }
          HAInterestTestCase.isBeforeInterestRecoveryCallbackCalled = true;
          HAInterestTestCase.class.notify();
          PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = false;
        }
      }
    });
  }

  public static void setClientServerObserverForBeforeInterestRecovery() {
    PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void beforeInterestRecovery() {
        synchronized (HAInterestTestCase.class) {
          Thread t = new Thread() {
            public void run() {
              Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
              assertNotNull(r1);
              r1.put(k1, server_k1_updated);
            }
          };
          t.start();

          HAInterestTestCase.isBeforeInterestRecoveryCallbackCalled = true;
          HAInterestTestCase.class.notify();
          PoolImpl.BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = false;
        }
      }
    });
  }

  public static void waitForBeforeInterestRecoveryCallBack() throws InterruptedException {
    assertNotNull(cache);
    synchronized (HAInterestTestCase.class) {
      while (!isBeforeInterestRecoveryCallbackCalled) {
        HAInterestTestCase.class.wait();
      }
    }
  }

  public static void setClientServerObserverForBeforeRegistration(final VM vm) {
    PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void beforeInterestRegistration() {
        synchronized (HAInterestTestCase.class) {
          vm.invoke(() -> HAInterestTestCase.startServer());
          HAInterestTestCase.isBeforeRegistrationCallbackCalled = true;
          HAInterestTestCase.class.notify();
          PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = false;
        }
      }
    });
  }

  public static void waitForBeforeRegistrationCallback() throws InterruptedException {
    assertNotNull(cache);
    synchronized (HAInterestTestCase.class) {
      while (!isBeforeRegistrationCallbackCalled) {
        HAInterestTestCase.class.wait();
      }
    }
  }

  public static void setClientServerObserverForAfterRegistration(final VM vm) {
    PoolImpl.AFTER_REGISTER_CALLBACK_FLAG = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      public void afterInterestRegistration() {
        synchronized (HAInterestTestCase.class) {
          vm.invoke(() -> HAInterestTestCase.startServer());
          HAInterestTestCase.isAfterRegistrationCallbackCalled = true;
          HAInterestTestCase.class.notify();
          PoolImpl.AFTER_REGISTER_CALLBACK_FLAG = false;
        }
      }
    });
  }

  public static void waitForAfterRegistrationCallback() throws InterruptedException {
    assertNotNull(cache);
    if (!isAfterRegistrationCallbackCalled) {
      synchronized (HAInterestTestCase.class) {
        while (!isAfterRegistrationCallbackCalled) {
          HAInterestTestCase.class.wait();
        }
      }
    }
  }

  public static void unSetClientServerObserverForRegistrationCallback() {
    synchronized (HAInterestTestCase.class) {
      PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG = false;
      PoolImpl.AFTER_REGISTER_CALLBACK_FLAG = false;
      HAInterestTestCase.isBeforeRegistrationCallbackCalled = false;
      HAInterestTestCase.isAfterRegistrationCallbackCalled = false;
    }
  }

  public static void verifyDispatcherIsAlive() {
    assertEquals("More than one BridgeServer", 1, cache.getCacheServers().size());

    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return cache.getCacheServers().size() == 1;
      }

      @Override
      public String description() {
        return "waiting for cache.getCacheServers().size() == 1";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertNotNull(bs);
    assertNotNull(bs.getAcceptor());
    assertNotNull(bs.getAcceptor().getCacheClientNotifier());
    final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();

    wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return ccn.getClientProxies().size() > 0;
      }

      @Override
      public String description() {
        return "waiting for ccn.getClientProxies().size() > 0";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    wc = new WaitCriterion() {
      Iterator iter_prox;
      CacheClientProxy proxy;

      @Override
      public boolean done() {
        iter_prox = ccn.getClientProxies().iterator();
        if (iter_prox.hasNext()) {
          proxy = (CacheClientProxy) iter_prox.next();
          return proxy._messageDispatcher.isAlive();
        } else {
          return false;
        }
      }

      @Override
      public String description() {
        return "waiting for CacheClientProxy _messageDispatcher to be alive";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  public static void verifyDispatcherIsNotAlive() {
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return cache.getCacheServers().size() == 1;
      }

      @Override
      public String description() {
        return "cache.getCacheServers().size() == 1";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertNotNull(bs);
    assertNotNull(bs.getAcceptor());
    assertNotNull(bs.getAcceptor().getCacheClientNotifier());
    final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();

    wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return ccn.getClientProxies().size() > 0;
      }

      @Override
      public String description() {
        return "waiting for ccn.getClientProxies().size() > 0";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    Iterator iter_prox = ccn.getClientProxies().iterator();
    if (iter_prox.hasNext()) {
      CacheClientProxy proxy = (CacheClientProxy) iter_prox.next();
      assertFalse("Dispatcher on secondary should not be alive",
          proxy._messageDispatcher.isAlive());
    }
  }

  public static void createEntriesK1andK2OnServer() {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    if (!r1.containsKey(k1)) {
      r1.create(k1, server_k1);
    }
    if (!r1.containsKey(k2)) {
      r1.create(k2, server_k2);
    }
    assertEquals(r1.getEntry(k1).getValue(), server_k1);
    assertEquals(r1.getEntry(k2).getValue(), server_k2);
  }

  public static void createEntriesK1andK2() {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    if (!r1.containsKey(k1)) {
      r1.create(k1, client_k1);
    }
    if (!r1.containsKey(k2)) {
      r1.create(k2, client_k2);
    }
    assertEquals(r1.getEntry(k1).getValue(), client_k1);
    assertEquals(r1.getEntry(k2).getValue(), client_k2);
  }

  public static void createServerEntriesK1andK2() {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    if (!r1.containsKey(k1)) {
      r1.create(k1, server_k1);
    }
    if (!r1.containsKey(k2)) {
      r1.create(k2, server_k2);
    }
    assertEquals(r1.getEntry(k1).getValue(), server_k1);
    assertEquals(r1.getEntry(k2).getValue(), server_k2);
  }

  public static void registerK1AndK2() {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r);
    List list = new ArrayList();
    list.add(k1);
    list.add(k2);
    r.registerInterest(list, InterestResultPolicy.KEYS_VALUES);
  }

  public static void reRegisterK1AndK2() {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r);
    List list = new ArrayList();
    list.add(k1);
    list.add(k2);
    r.registerInterest(list);
  }

  public static void startServer() throws IOException {
    Cache c = CacheFactory.getAnyInstance();
    assertEquals("More than one BridgeServer", 1, c.getCacheServers().size());
    CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
    assertNotNull(bs);
    bs.start();
  }

  public static void stopServer() {
    assertEquals("More than one BridgeServer", 1, cache.getCacheServers().size());
    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertNotNull(bs);
    bs.stop();
  }

  public static void stopPrimaryAndRegisterK1AndK2AndVerifyResponse() {
    LocalRegion r = (LocalRegion) cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(r);
    ServerRegionProxy srp = new ServerRegionProxy(r);

    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return pool.getConnectedServerCount() == 3;
      }

      @Override
      public String description() {
        return "connected server count never became 3";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    // close primaryEP
    getPrimaryVM().invoke(() -> stopServer());
    List list = new ArrayList();
    list.add(k1);
    list.add(k2);
    List serverKeys = srp.registerInterest(list, KEY, KEYS, false,
        r.getAttributes().getDataPolicy().ordinal);
    assertNotNull(serverKeys);
    List resultKeys = (List) serverKeys.get(0);
    assertEquals(2, resultKeys.size());
    assertTrue(resultKeys.contains(k1));
    assertTrue(resultKeys.contains(k2));
  }

  public static void stopPrimaryAndUnregisterRegisterK1() {
    LocalRegion r = (LocalRegion) cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(r);
    ServerRegionProxy srp = new ServerRegionProxy(r);

    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return pool.getConnectedServerCount() == 3;
      }

      @Override
      public String description() {
        return "connected server count never became 3";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    // close primaryEP
    getPrimaryVM().invoke(() -> stopServer());
    List list = new ArrayList();
    list.add(k1);
    srp.unregisterInterest(list, KEY, false, false);
  }

  public static void stopBothPrimaryAndSecondaryAndRegisterK1AndK2AndVerifyResponse() {
    LocalRegion r = (LocalRegion) cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(r);
    ServerRegionProxy srp = new ServerRegionProxy(r);

    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return pool.getConnectedServerCount() == 3;
      }

      @Override
      public String description() {
        return "connected server count never became 3";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    // close primaryEP
    VM backup = getBackupVM();
    getPrimaryVM().invoke(() -> stopServer());
    // close secondary
    backup.invoke(() -> stopServer());
    List list = new ArrayList();
    list.add(k1);
    list.add(k2);
    List serverKeys = srp.registerInterest(list, KEY, KEYS, false,
        r.getAttributes().getDataPolicy().ordinal);

    assertNotNull(serverKeys);
    List resultKeys = (List) serverKeys.get(0);
    assertEquals(2, resultKeys.size());
    assertTrue(resultKeys.contains(k1));
    assertTrue(resultKeys.contains(k2));
  }

  /**
   * returns the secondary that was stopped
   */
  public static VM stopSecondaryAndRegisterK1AndK2AndVerifyResponse() {
    LocalRegion r = (LocalRegion) cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(r);
    ServerRegionProxy srp = new ServerRegionProxy(r);

    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return pool.getConnectedServerCount() == 3;
      }

      @Override
      public String description() {
        return "Never got three connected servers";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    // close secondary EP
    VM result = getBackupVM();
    result.invoke(() -> stopServer());
    List list = new ArrayList();
    list.add(k1);
    list.add(k2);
    List serverKeys = srp.registerInterest(list, KEY, KEYS, false,
        r.getAttributes().getDataPolicy().ordinal);

    assertNotNull(serverKeys);
    List resultKeys = (List) serverKeys.get(0);
    assertEquals(2, resultKeys.size());
    assertTrue(resultKeys.contains(k1));
    assertTrue(resultKeys.contains(k2));
    return result;
  }

  /**
   * returns the secondary that was stopped
   */
  public static VM stopSecondaryAndUNregisterK1() {
    LocalRegion r = (LocalRegion) cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(r);
    ServerRegionProxy srp = new ServerRegionProxy(r);

    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return pool.getConnectedServerCount() == 3;
      }

      @Override
      public String description() {
        return "connected server count never became 3";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    // close secondary EP
    VM result = getBackupVM();
    result.invoke(() -> stopServer());
    List list = new ArrayList();
    list.add(k1);
    srp.unregisterInterest(list, KEY, false, false);
    return result;
  }

  public static void registerK1AndK2OnPrimaryAndSecondaryAndVerifyResponse() {
    ServerLocation primary = pool.getPrimary();
    ServerLocation secondary = (ServerLocation) pool.getRedundants().get(0);
    LocalRegion r = (LocalRegion) cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r);
    ServerRegionProxy srp = new ServerRegionProxy(r);
    List list = new ArrayList();
    list.add(k1);
    list.add(k2);

    // Primary server
    List serverKeys1 = srp.registerInterestOn(primary, list, InterestType.KEY,
        InterestResultPolicy.KEYS, false, r.getAttributes().getDataPolicy().ordinal);
    assertNotNull(serverKeys1);
    // expect serverKeys in response from primary
    List resultKeys = (List) serverKeys1.get(0);
    assertEquals(2, resultKeys.size());
    assertTrue(resultKeys.contains(k1));
    assertTrue(resultKeys.contains(k2));

    // Secondary server
    List serverKeys2 = srp.registerInterestOn(secondary, list, InterestType.KEY,
        InterestResultPolicy.KEYS, false, r.getAttributes().getDataPolicy().ordinal);
    // if the list is null then it is empty
    if (serverKeys2 != null) {
      // no serverKeys in response from secondary
      assertTrue(serverKeys2.isEmpty());
    }
  }

  public static void verifyInterestRegistration() {
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return cache.getCacheServers().size() == 1;
      }

      @Override
      public String description() {
        return "waiting for cache.getCacheServers().size() == 1";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertNotNull(bs);
    assertNotNull(bs.getAcceptor());
    assertNotNull(bs.getAcceptor().getCacheClientNotifier());
    final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();

    wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return ccn.getClientProxies().size() > 0;
      }

      @Override
      public String description() {
        return "waiting for ccn.getClientProxies().size() > 0";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    Iterator iter_prox = ccn.getClientProxies().iterator();

    if (iter_prox.hasNext()) {
      final CacheClientProxy ccp = (CacheClientProxy) iter_prox.next();

      wc = new WaitCriterion() {
        @Override
        public boolean done() {
          Set keysMap = (Set) ccp.cils[interestListIndex]
              .getProfile(SEPARATOR + REGION_NAME).getKeysOfInterestFor(ccp.getProxyID());
          return keysMap != null && keysMap.size() == 2;
        }

        @Override
        public String description() {
          return "waiting for keys of interest to include 2 keys";
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);

      Set keysMap = (Set) ccp.cils[interestListIndex]
          .getProfile(SEPARATOR + REGION_NAME).getKeysOfInterestFor(ccp.getProxyID());
      assertNotNull(keysMap);
      assertEquals(2, keysMap.size());
      assertTrue(keysMap.contains(k1));
      assertTrue(keysMap.contains(k2));
    }
  }

  public static void verifyInterestUNRegistration() {
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return cache.getCacheServers().size() == 1;
      }

      @Override
      public String description() {
        return "waiting for cache.getCacheServers().size() == 1";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertNotNull(bs);
    assertNotNull(bs.getAcceptor());
    assertNotNull(bs.getAcceptor().getCacheClientNotifier());
    final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();

    wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return ccn.getClientProxies().size() > 0;
      }

      @Override
      public String description() {
        return "waiting for ccn.getClientProxies().size() > 0";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);

    Iterator iter_prox = ccn.getClientProxies().iterator();
    if (iter_prox.hasNext()) {
      final CacheClientProxy ccp = (CacheClientProxy) iter_prox.next();

      wc = new WaitCriterion() {
        @Override
        public boolean done() {
          Set keysMap = (Set) ccp.cils[interestListIndex]
              .getProfile(SEPARATOR + REGION_NAME).getKeysOfInterestFor(ccp.getProxyID());
          return keysMap != null;
        }

        @Override
        public String description() {
          return "waiting for keys of interest to not be null";
        }
      };
      GeodeAwaitility.await().untilAsserted(wc);

      Set keysMap = (Set) ccp.cils[interestListIndex]
          .getProfile(SEPARATOR + REGION_NAME).getKeysOfInterestFor(ccp.getProxyID());
      assertNotNull(keysMap);
      assertEquals(1, keysMap.size());
      assertFalse(keysMap.contains(k1));
      assertTrue(keysMap.contains(k2));
    }
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientPoolCache(String testName, String host) throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HAInterestTestCase().createCache(props);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl p;
    try {
      p = (PoolImpl) PoolManager.createFactory().addServer(host, PORT1).addServer(host, PORT2)
          .addServer(host, PORT3).setSubscriptionEnabled(true).setSubscriptionRedundancy(-1)
          .setReadTimeout(10000).setPingInterval(1000)
          // retryInterval should be more so that only registerInterste thread
          // will initiate failover
          // .setRetryInterval(20000)
          .create("HAInterestBaseTestPool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    factory.setPoolName(p.getName());

    cache.createRegion(REGION_NAME, factory.create());
    pool = p;
    conn = pool.acquireConnection();
    assertNotNull(conn);
  }

  public static void createClientPoolCacheWithSmallRetryInterval(String testName, String host)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HAInterestTestCase().createCache(props);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    PoolImpl p;
    try {
      p = (PoolImpl) PoolManager.createFactory().addServer(host, PORT1).addServer(host, PORT2)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(10000)
          .setSocketBufferSize(32768).setMinConnections(6).setPingInterval(200)
          // .setRetryInterval(200)
          // retryAttempts 3
          .create("HAInterestBaseTestPool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    factory.setPoolName(p.getName());

    cache.createRegion(REGION_NAME, factory.create());

    pool = p;
    conn = pool.acquireConnection();
    assertNotNull(conn);
  }

  public static void createClientPoolCacheConnectionToSingleServer(String testName, String hostName)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new HAInterestTestCase().createCache(props);
    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(hostName, PORT1)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(10000)
        // .setRetryInterval(20)
        .create("HAInterestBaseTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    factory.setPoolName(p.getName());

    cache.createRegion(REGION_NAME, factory.create());

    pool = p;
    conn = pool.acquireConnection();
    assertNotNull(conn);
  }

  public static Integer createServerCache() throws Exception {
    new HAInterestTestCase().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableBridgeConflation(true);
    factory.setMirrorType(MirrorType.KEYS_VALUES);
    factory.setConcurrencyChecksEnabled(true);
    cache.createRegion(REGION_NAME, factory.create());

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setMaximumTimeBetweenPings(180000);
    // ensures updates to be sent instead of invalidations
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  public static Integer createServerCacheWithLocalRegion() throws Exception {
    new HAInterestTestCase().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    // ensures updates to be sent instead of invalidations
    server.setNotifyBySubscription(true);
    server.setMaximumTimeBetweenPings(180000);
    server.start();
    return new Integer(server.getPort());
  }
}
