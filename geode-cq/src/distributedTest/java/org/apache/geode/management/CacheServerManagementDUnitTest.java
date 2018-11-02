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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.cache.GemFireCacheImpl.getInstance;
import static org.apache.geode.management.ManagementService.getManagementService;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getClientServiceMBeanName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.internal.LocatorTestBase;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.cq.dunit.CqQueryDUnitTest;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.JmxManagerLocatorRequest;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Cache Server related management test cases
 */
@Category({ClientSubscriptionTest.class})
public class CacheServerManagementDUnitTest extends LocatorTestBase {

  private static final long serialVersionUID = 1L;

  private static int CONNECT_LOCATOR_TIMEOUT_MS = 30000;

  private ManagementTestBase helper;

  private static final String queryName = "testClientWithFeederAndCQ_0";

  private static final String indexName = "testIndex";

  private static MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;

  protected CqQueryDUnitTest cqDUnitTest = new CqQueryDUnitTest();

  public CacheServerManagementDUnitTest() {
    super();
    this.helper = new ManagementTestBase() {
      {
      }
    };

  }

  @Override
  public final void preSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  protected final void postTearDownLocatorTestBase() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void testCacheServerMBean() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    VM managingNode = host.getVM(2);

    // Managing Node is created first
    helper.createManagementCache(managingNode);
    helper.startManagingNode(managingNode);
    // helper.createCache(server);
    int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cqDUnitTest.createServer(server, serverPort);

    DistributedMember member = helper.getMember(server);

    verifyCacheServer(server, serverPort);

    final int port = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    // Create client.
    cqDUnitTest.createClient(client, port, host0);

    cqDUnitTest.createCQ(client, queryName, cqDUnitTest.cqs[0]);
    cqDUnitTest.executeCQ(client, queryName, false, null);

    final int size = 10;
    cqDUnitTest.createValues(client, cqDUnitTest.regions[0], size);
    cqDUnitTest.waitForCreated(client, queryName, CqQueryDUnitTest.KEY + size);

    cqDUnitTest.validateCQ(client, queryName, /* resultSize: */CqQueryDUnitTest.noTest,
        /* creates: */size, /* updates: */0, /* deletes; */0, /* queryInserts: */size,
        /* queryUpdates: */0, /* queryDeletes: */0, /* totalEvents: */size);

    // Close.

    Wait.pause(2000);
    checkNavigation(managingNode, member, serverPort);
    verifyIndex(server, serverPort);
    // This will test all CQs and will close the cq in its final step
    verifyCacheServerRemote(managingNode, member, serverPort);

    verifyClosedCQ(server);

    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
    helper.stopManagingNode(managingNode);
    helper.closeCache(client);
    helper.closeCache(server);
    helper.closeCache(managingNode);
  }

  /**
   * Test for client server connection related management artifacts like notifications
   *
   */

  @Test
  public void testCacheClient() throws Exception {

    final Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    VM server = host.getVM(1);
    VM client = host.getVM(2);

    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    locator.invoke("Start Locator", () -> startLocator(locator.getHost(), locatorPort, ""));

    String locators = NetworkUtils.getServerHostName(locator.getHost()) + "[" + locatorPort + "]";

    int serverPort = server.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    addClientNotifListener(server, serverPort);

    // Start a client and make sure that proper notification is received
    client.invoke("Start BridgeClient", () -> startBridgeClient(null,
        NetworkUtils.getServerHostName(locator.getHost()), locatorPort));

    // stop the client and make sure the cache server notifies
    stopBridgeMemberVM(client);
    helper.closeCache(locator);
    helper.closeCache(server);
    helper.closeCache(client);

  }

  /**
   * Intention of this test is to check if a node becomes manager after all the nodes are alive it
   * should have all the information of all the members.
   * <p>
   * Thats why used service.getLocalManager().runManagementTaskAdhoc() to make node ready for
   * federation when manager node comes up
   *
   */

  // renable when bug 46138
  @Ignore("Bug46049")
  @Test
  public void testBug46049() throws Exception {
    final Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    VM server = host.getVM(1);

    // Step 1:
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    locator.invoke("Start Locator", () -> startLocator(locator.getHost(), locatorPort, ""));

    String locators = NetworkUtils.getServerHostName(locator.getHost()) + "[" + locatorPort + "]";

    // Step 2:
    server.invoke("Start BridgeServer", () -> startBridgeServer(null, locators));

    // Step 3:
    server.invoke("Check Server", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      assertNotNull(cache);
      SystemManagementService service =
          (SystemManagementService) ManagementService.getExistingManagementService(cache);
      assertNotNull(service);
      assertFalse(service.isManager());
      assertNotNull(service.getMemberMXBean());
      service.getLocalManager().runManagementTaskAdhoc();
    });

    // Step 4:
    JmxManagerLocatorRequest.send(locator.getHost().getHostName(), locatorPort,
        CONNECT_LOCATOR_TIMEOUT_MS, new Properties());

    // Step 5:
    locator.invoke("Check locator", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      assertNotNull(cache);
      ManagementService service = ManagementService.getExistingManagementService(cache);
      assertNotNull(service);
      assertTrue(service.isManager());
      LocatorMXBean bean = service.getLocalLocatorMXBean();
      assertEquals(locatorPort, bean.getPort());
      DistributedSystemMXBean dsBean = service.getDistributedSystemMXBean();

      assertEquals(2, dsBean.listMemberObjectNames().length);
    });

    helper.closeCache(locator);
    helper.closeCache(server);

  }

  protected void startLocator(Host vmHost, final int locatorPort, final String otherLocators) {
    disconnectFromDS();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, String.valueOf(0));
    props.setProperty(LOCATORS, otherLocators);
    props.setProperty(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    props.setProperty(JMX_MANAGER_HTTP_PORT, "0");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");
    File logFile = new File(getUniqueName() + "-locator" + locatorPort + ".log");
    try {
      InetAddress bindAddr = InetAddress.getByName(NetworkUtils.getServerHostName(vmHost));
      Locator locator = Locator.startLocatorAndDS(locatorPort, logFile, bindAddr, props);
      remoteObjects.put(LOCATOR_KEY, locator);
    } catch (UnknownHostException uhe) {
      Assert.fail("While resolving bind address ", uhe);
    } catch (IOException ex) {
      Assert.fail("While starting locator on port " + locatorPort, ex);
    }
  }

  protected void checkNavigation(final VM vm, final DistributedMember cacheServerMember,
      final int serverPort) {
    SerializableRunnable checkNavigation = new SerializableRunnable("Check Navigation") {
      public void run() {

        final ManagementService service = helper.getManagementService();

        DistributedSystemMXBean disMBean = service.getDistributedSystemMXBean();
        try {
          ObjectName expected =
              MBeanJMXAdapter.getClientServiceMBeanName(serverPort, cacheServerMember.getId());
          ObjectName actual =
              disMBean.fetchCacheServerObjectName(cacheServerMember.getId(), serverPort);
          assertEquals(expected, actual);
        } catch (Exception e) {
          fail("Cache Server Navigation Failed " + e);
        }

        try {
          assertEquals(1, disMBean.listCacheServerObjectNames().length);
        } catch (Exception e) {
          fail("Cache Server Navigation Failed " + e);
        }

      }
    };
    vm.invoke(checkNavigation);
  }

  /**
   * Verify the Cache Server details
   *
   */
  @SuppressWarnings("serial")
  protected void addClientNotifListener(final VM vm, final int serverPort) throws Exception {
    SerializableRunnable addClientNotifListener =
        new SerializableRunnable("Add Client Notif Listener") {
          public void run() {
            GemFireCacheImpl cache = getInstance();
            ManagementService service = getManagementService(cache);
            final CacheServerMXBean bean = service.getLocalCacheServerMXBean(serverPort);
            assertNotNull(bean);
            WaitCriterion ev = new WaitCriterion() {
              public boolean done() {
                if (bean.isRunning())
                  return true;
                return false;
              }

              public String description() {
                return null;
              }
            };
            GeodeAwaitility.await().untilAsserted(ev);
            assertTrue(bean.isRunning());
            TestCacheServerNotif nt = new TestCacheServerNotif();
            try {
              mbeanServer.addNotificationListener(getClientServiceMBeanName(
                  serverPort, cache.getDistributedSystem().getMemberId()), nt, null, null);
            } catch (InstanceNotFoundException e) {
              fail("Failed With Exception " + e);
            }

          }
        };
    vm.invoke(addClientNotifListener);
  }

  /**
   * Verify the closed CQ which is closed from Managing Node
   *
   */
  @SuppressWarnings("serial")
  protected void verifyIndex(final VM vm, final int serverPort) throws Exception {
    SerializableRunnable verifyIndex = new SerializableRunnable("Verify Index ") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService.getManagementService(cache);
        QueryService qs = cache.getQueryService();
        try {
          qs.createIndex(indexName, "p.ID", "/root/" + cqDUnitTest.regions[0]);
        } catch (RegionNotFoundException e) {
          fail("Failed With Exception " + e);
        } catch (IndexInvalidException e) {
          fail("Failed With Exception " + e);
        } catch (IndexNameConflictException e) {
          fail("Failed With Exception " + e);
        } catch (IndexExistsException e) {
          fail("Failed With Exception " + e);
        } catch (UnsupportedOperationException e) {
          fail("Failed With Exception " + e);
        }

        CacheServerMXBean bean = service.getLocalCacheServerMXBean(serverPort);
        assertEquals(bean.getIndexCount(), 1);
        LogWriterUtils.getLogWriter()
            .info("<ExpectedString> Index is   " + bean.getIndexList()[0] + "</ExpectedString> ");
        try {
          bean.removeIndex(indexName);
        } catch (Exception e) {
          fail("Failed With Exception " + e);

        }
        assertEquals(bean.getIndexCount(), 0);

      }
    };
    vm.invoke(verifyIndex);
  }

  /**
   * Verify the closed CQ which is closed from Managing Node
   *
   */
  @SuppressWarnings("serial")
  protected void verifyClosedCQ(final VM vm) throws Exception {
    SerializableRunnable verifyClosedCQ = new SerializableRunnable("Verify Closed CQ") {
      public void run() {
        CqService cqService = GemFireCacheImpl.getInstance().getCqService();
        if (cqService != null) {
          assertNull(cqService.getCq(queryName));
        }

      }
    };
    vm.invoke(verifyClosedCQ);
  }

  /**
   * Verify the Cache Server details
   *
   */
  @SuppressWarnings("serial")
  protected void verifyCacheServer(final VM vm, final int serverPort) throws Exception {
    SerializableRunnable verifyCacheServer = new SerializableRunnable("Verify Cache Server") {
      public void run() {
        GemFireCacheImpl cache = getInstance();
        ManagementService service = getManagementService(cache);
        final CacheServerMXBean bean = service.getLocalCacheServerMXBean(serverPort);
        assertNotNull(bean);
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            if (bean.isRunning())
              return true;
            return false;
          }

          public String description() {
            return null;
          }
        };
        GeodeAwaitility.await().untilAsserted(ev);
        assertTrue(bean.isRunning());
        assertCacheServerConfig(bean);

      }
    };
    vm.invoke(verifyCacheServer);
  }

  protected void assertCacheServerConfig(CacheServerMXBean bean) {
    // assertIndexDetailsEquals(ServerInfo.getInstance().getServerPort(), bean.getPort());
    assertEquals(CacheServer.DEFAULT_BIND_ADDRESS, bean.getBindAddress());
    assertEquals(CacheServer.DEFAULT_HOSTNAME_FOR_CLIENTS, bean.getHostNameForClients());
    assertEquals(CacheServer.DEFAULT_SOCKET_BUFFER_SIZE, bean.getSocketBufferSize());
    assertEquals(CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, bean.getMaximumTimeBetweenPings());
    assertEquals(CacheServer.DEFAULT_MAX_CONNECTIONS, bean.getMaxConnections());
    assertEquals(CacheServer.DEFAULT_MAX_THREADS, bean.getMaxThreads());
    assertEquals(CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, bean.getMaximumMessageCount());
    assertEquals(CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE, bean.getMessageTimeToLive());
    assertEquals(CacheServer.DEFAULT_LOAD_POLL_INTERVAL, bean.getLoadPollInterval());
    LogWriterUtils.getLogWriter().info("<ExpectedString> LoadProbe of the Server is  "
        + bean.fetchLoadProbe().toString() + "</ExpectedString> ");
  }

  /**
   * Verify the Cache Server details
   *
   */
  @SuppressWarnings("serial")
  protected void verifyCacheServerRemote(final VM vm, final DistributedMember serverMember,
      final int serverPort) {
    SerializableRunnable verifyCacheServerRemote =
        new SerializableRunnable("Verify Cache Server Remote") {
          public void run() {
            GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
            try {

              CacheServerMXBean bean = MBeanUtil.getCacheServerMbeanProxy(serverMember, serverPort);

              // Check for bean configuration
              assertCacheServerConfig(bean);

              String clientId = bean.getClientIds()[0];
              assertNotNull(clientId);
              LogWriterUtils.getLogWriter().info(
                  "<ExpectedString> ClientId of the Server is  " + clientId + "</ExpectedString> ");
              LogWriterUtils.getLogWriter().info("<ExpectedString> Active Query Count  "
                  + bean.getActiveCQCount() + "</ExpectedString> ");

              LogWriterUtils.getLogWriter().info("<ExpectedString> Registered Query Count  "
                  + bean.getRegisteredCQCount() + "</ExpectedString> ");

              assertTrue(bean.showAllClientStats()[0].getClientCQCount() == 1);
              int numQueues = bean.getNumSubscriptions();
              assertEquals(numQueues, 1);

              bean.getContinuousQueryList();
              // Only temporarily stops the query
              bean.stopContinuousQuery("testClientWithFeederAndCQ_0");

              // Start a stopped query
              bean.executeContinuousQuery("testClientWithFeederAndCQ_0");

              // Close the continuous query
              bean.closeContinuousQuery("testClientWithFeederAndCQ_0");
            } catch (Exception e) {
              fail("Error while verifying cache server from remote member " + e);
            }

          }
        };
    vm.invoke(verifyCacheServerRemote);
  }

  /**
   * Notification handler
   */
  private static class TestCacheServerNotif implements NotificationListener {

    @Override
    public void handleNotification(Notification notification, Object handback) {
      assertNotNull(notification);
      LogWriterUtils.getLogWriter().info("Expected String :" + notification.toString());
    }

  }

}
