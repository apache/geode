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
package com.gemstone.gemfire.management;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Properties;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.client.internal.LocatorTestBase;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryDUnitTest;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.JmxManagerLocatorRequest;
import com.gemstone.gemfire.management.internal.JmxManagerLocatorResponse;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * Cache Server related management test cases
 * 
 * @author rishim
 * 
 */
public class CacheServerManagementDUnitTest extends LocatorTestBase {

  private static final long serialVersionUID = 1L;
  
  private static int CONNECT_LOCATOR_TIMEOUT_MS = 30000; 

  private ManagementTestBase helper;

  private static final String queryName = "testClientWithFeederAndCQ_0";

  private static final String indexName = "testIndex";
  
  private static MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;
  

  protected CqQueryDUnitTest cqDUnitTest = new CqQueryDUnitTest(
      "CqDataDUnitTest");

  public CacheServerManagementDUnitTest(String name) {
    super(name);
    this.helper = new ManagementTestBase(name);
    
  }

  public void setUp() throws Exception {
    disconnectAllFromDS();
    super.setUp();
  }

  @Override
  protected final void postTearDownLocatorTestBase() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * 
   * @throws Exception
   */
  public void testCacheServerMBean() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    VM managingNode = host.getVM(2);

    // Managing Node is created first
    helper.createManagementCache(managingNode);
    helper.startManagingNode(managingNode);
    //helper.createCache(server);
    int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cqDUnitTest.createServer(server,serverPort);
    
   
    DistributedMember member = helper.getMember(server);
    
    verifyCacheServer(server,serverPort);

    final int port = server.invokeInt(CqQueryDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    // Create client.
    cqDUnitTest.createClient(client, port, host0);

    cqDUnitTest.createCQ(client, queryName, cqDUnitTest.cqs[0]);
    cqDUnitTest.executeCQ(client, queryName, false, null);

    final int size = 10;
    cqDUnitTest.createValues(client, cqDUnitTest.regions[0], size);
    cqDUnitTest.waitForCreated(client, queryName, CqQueryDUnitTest.KEY + size);

    cqDUnitTest.validateCQ(client, queryName,
    /* resultSize: */CqQueryDUnitTest.noTest,
    /* creates: */size,
    /* updates: */0,
    /* deletes; */0,
    /* queryInserts: */size,
    /* queryUpdates: */0,
    /* queryDeletes: */0,
    /* totalEvents: */size);

    // Close.

    Wait.pause(2000);
    checkNavigation(managingNode,member,serverPort);
    verifyIndex(server,serverPort);
    // This will test all CQs and will close the cq in its final step
    verifyCacheServerRemote(managingNode, member,serverPort);

    verifyClosedCQ(server);

    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
    helper.stopManagingNode(managingNode);
    helper.closeCache(client);
    helper.closeCache(server);
    helper.closeCache(managingNode);
  }

  /**
   * Test for client server connection related management artifacts
   * like notifications 
   * @throws Exception
   */
  
  public void testCacheClient() throws Exception {
    
    final Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    VM server = host.getVM(1);
    VM client = host.getVM(2);
    
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocatorInVM(locator, locatorPort, "");
    
    String locators = NetworkUtils.getServerHostName(locator.getHost())+ "[" + locatorPort + "]";
    
   
    int serverPort = startBridgeServerInVM(server, null, locators);
    
    addClientNotifListener(server,serverPort);

    // Start a client and make sure that proper notification is received
    startBridgeClientInVM(client, null, NetworkUtils.getServerHostName(locator.getHost()), locatorPort);
    
    //stop the client and make sure the bridge server notifies
    stopBridgeMemberVM(client);
    helper.closeCache(locator);
    helper.closeCache(server);
    helper.closeCache(client);

  }
  
  /**
   * Intention of this test is to check if a node becomes manager after all the nodes are alive
   * it should have all the information of all  the members.
   * 
   * Thats why used  service.getLocalManager().runManagementTaskAdhoc() to make node
   * ready for federation when manager node comes up
   * @throws Exception
   */

  // renable when bug 46138
  public void DISABLEDtestBug46049() throws Exception {
    final Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    VM server = host.getVM(1);
    
    //Step 1:
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startLocator(locator, locatorPort, "");

    String locators = NetworkUtils.getServerHostName(locator.getHost())+ "[" + locatorPort + "]";
    
    //Step 2:
    int serverPort = startBridgeServerInVM(server, null, locators);
    
    //Step 3:
    server.invoke(new SerializableRunnable("Check Server") {

      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        assertNotNull(cache);
        SystemManagementService service = (SystemManagementService)ManagementService
            .getExistingManagementService(cache);
        assertNotNull(service);
        assertFalse(service.isManager());
        assertNotNull(service.getMemberMXBean());
        service.getLocalManager().runManagementTaskAdhoc();


      }
    });
    
  //Step 4:
    JmxManagerLocatorResponse locRes = JmxManagerLocatorRequest.send(locator
        .getHost().getHostName(), locatorPort, CONNECT_LOCATOR_TIMEOUT_MS, Collections.<String, String> emptyMap());
    
  //Step 5:
    locator.invoke(new SerializableRunnable("Check locator") {

      public void run() {
        Cache cache = GemFireCacheImpl.getInstance();
        assertNotNull(cache);
        ManagementService service = ManagementService
            .getExistingManagementService(cache);
        assertNotNull(service);
        assertTrue(service.isManager());
        LocatorMXBean bean = service.getLocalLocatorMXBean();
        assertEquals(locatorPort, bean.getPort());
        DistributedSystemMXBean dsBean = service.getDistributedSystemMXBean();
        ObjectName[] names = dsBean.listMemberObjectNames();
       
        assertEquals(2,dsBean.listMemberObjectNames().length);

      }
    });
    


    helper.closeCache(locator);
    helper.closeCache(server);
    
 
  }
  
  
  protected void startLocator(final VM vm, final int locatorPort, final String otherLocators) {
    vm.invoke(new SerializableRunnable("Create Locator") {

      final String testName= getUniqueName();
      public void run() {
        disconnectFromDS();
        Properties props = new Properties();
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, otherLocators);
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());
        props.setProperty(DistributionConfig.JMX_MANAGER_HTTP_PORT_NAME, "0");
        props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
        try {
          File logFile = new File(testName + "-locator" + locatorPort
              + ".log");
          InetAddress bindAddr = null;
          try {
            bindAddr = InetAddress.getByName(NetworkUtils.getServerHostName(vm.getHost()));
          } catch (UnknownHostException uhe) {
            Assert.fail("While resolving bind address ", uhe);
          }
          Locator locator = Locator.startLocatorAndDS(locatorPort, logFile, bindAddr, props);
          remoteObjects.put(LOCATOR_KEY, locator);
        } catch (IOException ex) {
          Assert.fail("While starting locator on port " + locatorPort, ex);
        }
      }
    });
  }
  
  protected void checkNavigation(final VM vm,
      final DistributedMember cacheServerMember, final int serverPort) {
    SerializableRunnable checkNavigation = new SerializableRunnable(
        "Check Navigation") {
      public void run() {

        final ManagementService service = helper.getManagementService();

        DistributedSystemMXBean disMBean = service.getDistributedSystemMXBean();
        try {
          ObjectName expected = MBeanJMXAdapter.getClientServiceMBeanName(
              serverPort, cacheServerMember.getId());
          ObjectName actual = disMBean.fetchCacheServerObjectName(
              cacheServerMember.getId(), serverPort);
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
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void addClientNotifListener(final VM vm , final int serverPort) throws Exception {
    SerializableRunnable addClientNotifListener = new SerializableRunnable(
        "Add Client Notif Listener") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService
            .getManagementService(cache);
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
        Wait.waitForCriterion(ev, 10 * 1000, 200, true);
        assertTrue(bean.isRunning());
        TestCacheServerNotif nt = new TestCacheServerNotif();
        try {
          mbeanServer.addNotificationListener(MBeanJMXAdapter
              .getClientServiceMBeanName(serverPort,cache.getDistributedSystem().getMemberId()), nt, null, null);
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
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void verifyIndex(final VM vm, final int serverPort) throws Exception {
    SerializableRunnable verifyIndex = new SerializableRunnable("Verify Index ") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService
            .getManagementService(cache);
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

        CacheServerMXBean bean = service
            .getLocalCacheServerMXBean(serverPort);
        assertEquals(bean.getIndexCount(), 1);
        LogWriterUtils.getLogWriter().info(
            "<ExpectedString> Index is   " + bean.getIndexList()[0]
                + "</ExpectedString> ");
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
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void verifyClosedCQ(final VM vm) throws Exception {
    SerializableRunnable verifyClosedCQ = new SerializableRunnable(
        "Verify Closed CQ") {
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
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void verifyCacheServer(final VM vm, final int serverPort) throws Exception {
    SerializableRunnable verifyCacheServer = new SerializableRunnable(
        "Verify Cache Server") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        ManagementService service = ManagementService
            .getManagementService(cache);
        final CacheServerMXBean bean = service
            .getLocalCacheServerMXBean(serverPort);
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
        Wait.waitForCriterion(ev, 10 * 1000, 200, true);
        assertTrue(bean.isRunning());
        assertCacheServerConfig(bean);

      }
    };
    vm.invoke(verifyCacheServer);
  }

  protected void assertCacheServerConfig(CacheServerMXBean bean) {
    // assertEquals(ServerInfo.getInstance().getServerPort(), bean.getPort());
    assertEquals(CacheServer.DEFAULT_BIND_ADDRESS, bean.getBindAddress());
    assertEquals(CacheServer.DEFAULT_HOSTNAME_FOR_CLIENTS, bean
        .getHostNameForClients());
    assertEquals(CacheServer.DEFAULT_SOCKET_BUFFER_SIZE, bean
        .getSocketBufferSize());
    assertEquals(CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS, bean
        .getMaximumTimeBetweenPings());
    assertEquals(CacheServer.DEFAULT_MAX_CONNECTIONS, bean.getMaxConnections());
    assertEquals(CacheServer.DEFAULT_MAX_THREADS, bean.getMaxThreads());
    assertEquals(CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT, bean
        .getMaximumMessageCount());
    assertEquals(CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE, bean
        .getMessageTimeToLive());
    assertEquals(CacheServer.DEFAULT_LOAD_POLL_INTERVAL, bean
        .getLoadPollInterval());
    LogWriterUtils.getLogWriter().info(
        "<ExpectedString> LoadProbe of the Server is  "
            + bean.fetchLoadProbe().toString() + "</ExpectedString> ");
  }

  /**
   * Verify the Cache Server details
   * 
   * @param vm
   */
  @SuppressWarnings("serial")
  protected void verifyCacheServerRemote(final VM vm,
      final DistributedMember serverMember, final int serverPort) {
    SerializableRunnable verifyCacheServerRemote = new SerializableRunnable(
        "Verify Cache Server Remote") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        try {

          CacheServerMXBean bean = MBeanUtil.getCacheServerMbeanProxy(
              serverMember, serverPort);

          // Check for bean configuration
          assertCacheServerConfig(bean);

          String clientId = bean.getClientIds()[0];
          assertNotNull(clientId);
          LogWriterUtils.getLogWriter().info(
              "<ExpectedString> ClientId of the Server is  " + clientId
                  + "</ExpectedString> ");
          LogWriterUtils.getLogWriter().info(
              "<ExpectedString> Active Query Count  "
                  + bean.getActiveCQCount() + "</ExpectedString> ");
          
          LogWriterUtils.getLogWriter().info(
              "<ExpectedString> Registered Query Count  "
                  + bean.getRegisteredCQCount() + "</ExpectedString> ");

          assertTrue(bean.showAllClientStats()[0].getClientCQCount() == 1); 
          int numQueues = bean.getNumSubscriptions(); 
          assertEquals(numQueues, 1); 
          // test for client connection Count
          
          /* @TODO */
          //assertTrue(bean.getClientConnectionCount() > 0);

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
   * 
   * @author rishim
   * 
   */
  private static class TestCacheServerNotif implements
      NotificationListener {

    @Override
    public void handleNotification(Notification notification, Object handback) {
      assertNotNull(notification);
      LogWriterUtils.getLogWriter().info("Expected String :" + notification.toString());
    }

  }

}
