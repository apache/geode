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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category({DistributedTest.class, FlakyTest.class}) // GEODE-908 GEODE-3530
@SuppressWarnings("serial")
public class ListClientCommandDUnitTest extends CliCommandTestBase {

  private final String regionName = "stocks";
  private int port0 = 0;
  private int port1 = 0;

  @Test // FlakyTest: GEODE-908
  public void testListClient() throws Exception {
    setupSystemForListClient();

    final VM manager = Host.getHost(0).getVM(0);

    String commandString = CliStrings.LIST_CLIENTS;
    getLogWriter().info("testListClient commandStr=" + commandString);

    waitForListClientMbean();

    final VM server1 = Host.getHost(0).getVM(1);

    final DistributedMember serverMember = ClientCommandsTestUtils.getMember(server1);

    String[] clientIds = manager.invoke("get client Ids", () -> {
      final SystemManagementService service =
          (SystemManagementService) ManagementService.getManagementService(getCache());
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds();
    });

    String serverName = server1.invoke("get distributed member Id",
        ClientCommandsTestUtils::getDistributedMemberId);

    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("testListClient commandResult=" + commandResult);

    String resultAsString = commandResultToString(commandResult);
    getLogWriter().info("testListClient resultAsString=" + resultAsString);
    assertTrue(Result.Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    CompositeResultData.SectionResultData section = resultData.retrieveSection("section1");
    assertNotNull(section);
    TabularResultData tableResultData = section.retrieveTable("TableForClientList");
    assertNotNull(tableResultData);

    List<String> serverNames =
        tableResultData.retrieveAllValues(CliStrings.LIST_CLIENT_COLUMN_SERVERS);
    List<String> clientNames =
        tableResultData.retrieveAllValues(CliStrings.LIST_CLIENT_COLUMN_Clients);

    getLogWriter().info("testListClients serverNames : " + serverNames);
    getLogWriter().info("testListClients clientNames : " + clientNames);
    assertEquals(2, serverNames.size());
    assertEquals(2, clientNames.size());
    assertTrue(clientNames.contains(clientIds[0]));
    assertTrue(clientNames.contains(clientIds[1]));
    serverName = serverName.replace(":", "-");
    getLogWriter().info("testListClients serverName : " + serverName);
    for (String str : serverNames) {
      assertTrue(str.contains(serverName));
    }
    ClientCommandsTestUtils.closeNonDurableClient(Host.getHost(0).getVM(2));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(1));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(3));
  }

  @Test
  public void testListClientForServers() throws Exception {
    setupSystem();

    final VM manager = Host.getHost(0).getVM(0);

    String commandString = CliStrings.LIST_CLIENTS;
    System.out.println("testListClientForServers commandStr=" + commandString);

    final VM server1 = Host.getHost(0).getVM(1);
    final VM server2 = Host.getHost(0).getVM(3);

    final DistributedMember serverMember = ClientCommandsTestUtils.getMember(server1);

    String[] clientIds = manager.invoke("get client Ids", () -> {
      final SystemManagementService service =
          (SystemManagementService) ManagementService.getManagementService(getCache());
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds();
    });

    String serverName1 = server1.invoke("get distributed member Id",
        ClientCommandsTestUtils::getDistributedMemberId);

    String serverName2 = server2.invoke("get distributed member Id",
        ClientCommandsTestUtils::getDistributedMemberId);

    CommandResult commandResult = executeCommand(commandString);
    System.out.println("testListClientForServers commandResult=" + commandResult);

    String resultAsString = commandResultToString(commandResult);
    System.out.println("testListClientForServers resultAsString=" + resultAsString);
    assertTrue(Result.Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    CompositeResultData.SectionResultData section = resultData.retrieveSection("section1");
    assertNotNull(section);
    TabularResultData tableResultData = section.retrieveTable("TableForClientList");
    assertNotNull(tableResultData);

    List<String> serverNames =
        tableResultData.retrieveAllValues(CliStrings.LIST_CLIENT_COLUMN_SERVERS);
    List<String> clientNames =
        tableResultData.retrieveAllValues(CliStrings.LIST_CLIENT_COLUMN_Clients);

    serverName1 = serverName1.replace(":", "-");
    serverName2 = serverName2.replace(":", "-");

    System.out.println("testListClientForServers serverNames : " + serverNames);
    System.out.println("testListClientForServers serverName1 : " + serverName1);
    System.out.println("testListClientForServers serverName2 : " + serverName2);
    System.out.println("testListClientForServers clientNames : " + clientNames);

    for (String client : clientIds) {
      assertTrue(clientNames.contains(client));
    }

    for (String server : serverNames) {
      assertTrue(server.contains(serverName1) || server.contains(serverName2));
    }

    ClientCommandsTestUtils.closeNonDurableClient(Host.getHost(0).getVM(2));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(1));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(3));
  }

  private void setupSystem() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(ClientCommandsTestUtils.getServerProperties());

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM server2 = Host.getHost(0).getVM(3);

    port0 = startCacheServer(server1, regionName);
    port1 = startCacheServer(server2, regionName);
    startNonDurableClient(client1, server1, port0);
    startNonDurableClient(client1, server2, port1);

    waitForListClientMbean3();

    manager.invoke("get client Id", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      SystemManagementService service =
          (SystemManagementService) ManagementService.getExistingManagementService(cache);
      DistributedMember serverMember = ClientCommandsTestUtils.getMember(server1);
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds()[0];
    });
  }

  private void setupSystemForListClient() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(ClientCommandsTestUtils.getServerProperties());

    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM client2 = Host.getHost(0).getVM(3);

    port0 = startCacheServer(server1, regionName);
    startNonDurableClient(client1, server1, port0);
    startNonDurableClient(client2, server1, port0);
  }

  private int startCacheServer(VM server, final String regionName) {

    return server.invoke("setup CacheServer", () -> {
      getSystem(ClientCommandsTestUtils.getServerProperties());

      GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      Region region = createRootRegion(regionName, factory.create());
      assertTrue(region instanceof DistributedRegion);
      CacheServer cacheServer = cache.addCacheServer();
      cacheServer.setPort(0);
      cacheServer.start();
      return cacheServer.getPort();
    });
  }

  private void waitForListClientMbean3() {

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM server2 = Host.getHost(0).getVM(3);

    final DistributedMember serverMember1 = ClientCommandsTestUtils.getMember(server1);
    final DistributedMember serverMember2 = ClientCommandsTestUtils.getMember(server2);

    assertNotNull(serverMember1);

    manager.invoke(() -> Awaitility.waitAtMost(2 * 60, TimeUnit.SECONDS)
        .pollDelay(2, TimeUnit.SECONDS).until(() -> {
          final SystemManagementService service =
              (SystemManagementService) ManagementService.getManagementService(getCache());
          if (service == null) {
            getLogWriter().info("waitForListClientMbean3 Still probing for service");
            return false;
          } else {
            final ObjectName cacheServerMBeanName1 =
                service.getCacheServerMBeanName(port0, serverMember1);
            final ObjectName cacheServerMBeanName2 =
                service.getCacheServerMBeanName(port1, serverMember2);
            CacheServerMXBean bean1 =
                service.getMBeanProxy(cacheServerMBeanName1, CacheServerMXBean.class);
            CacheServerMXBean bean2 =
                service.getMBeanProxy(cacheServerMBeanName2, CacheServerMXBean.class);
            try {
              if (bean1 != null && bean2 != null) {
                if (bean1.getClientIds().length > 0 && bean2.getClientIds().length > 0) {
                  return true;
                }
              }
              return false;

            } catch (Exception e) {
              LogWrapper.getInstance(cache)
                  .warning("waitForListClientMbean3 Exception in waitForListClientMbean ::: "
                      + ExceptionUtils.getStackTrace(e));
            }
            return false;
          }
        }));
  }

  private void startNonDurableClient(VM client, final VM server, final int port) {
    client.invoke("start non-durable client", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      if (cache == null) {

        Properties props = ClientCommandsTestUtils.getNonDurableClientProps();
        props.setProperty(LOG_FILE, "client_" + OSProcess.getId() + ".log");
        props.setProperty(LOG_LEVEL, "fine");
        props.setProperty(STATISTIC_ARCHIVE_FILE, "client_" + OSProcess.getId() + ".gfs");
        props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");

        getSystem(props);

        final ClientCacheFactory ccf = new ClientCacheFactory(props);
        ccf.addPoolServer(getServerHostName(server.getHost()), port);
        ccf.setPoolSubscriptionEnabled(true);
        ccf.setPoolPingInterval(1);
        ccf.setPoolStatisticInterval(1);
        ccf.setPoolSubscriptionRedundancy(1);
        ccf.setPoolMinConnections(1);

        ClientCache clientCache = getClientCache(ccf);
        // Create region
        if (clientCache.getRegion(Region.SEPARATOR + regionName) == null
            && clientCache.getRegion(regionName) == null) {
          ClientRegionFactory<Object, Object> regionFactory =
              clientCache.createClientRegionFactory(ClientRegionShortcut.LOCAL)
                  .setPoolName(clientCache.getDefaultPool().getName());
          Region<Object, Object> dataRegion = regionFactory.create(regionName);
          assertNotNull(dataRegion);
          dataRegion.put("k1", "v1");
          dataRegion.put("k2", "v2");
        }
      } else {
        String poolName = "new_pool_" + System.currentTimeMillis();
        try {
          PoolImpl p = (PoolImpl) PoolManager.createFactory()
              .addServer(getServerHostName(server.getHost()), port).setThreadLocalConnections(true)
              .setMinConnections(1).setSubscriptionEnabled(true).setPingInterval(1)
              .setStatisticInterval(1).setMinConnections(1).setSubscriptionRedundancy(1)
              .create(poolName);
          System.out.println("Created new pool pool " + poolName);
          assertNotNull(p);
        } catch (Exception eee) {
          System.err.println("Exception in creating pool " + poolName + "    Exception =="
              + ExceptionUtils.getStackTrace(eee));
        }
      }
    });
  }

  private void waitForListClientMbean() {
    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final DistributedMember serverMember = ClientCommandsTestUtils.getMember(server1);
    assertNotNull(serverMember);
    manager.invoke(() -> Awaitility.waitAtMost(2 * 60, TimeUnit.SECONDS)
        .pollDelay(2, TimeUnit.SECONDS).until(() -> {
          final SystemManagementService service =
              (SystemManagementService) ManagementService.getManagementService(getCache());
          if (service == null) {
            getLogWriter().info("waitForListClientMbean Still probing for service");
            return false;
          } else {
            final ObjectName cacheServerMBeanName =
                service.getCacheServerMBeanName(port0, serverMember);
            CacheServerMXBean bean =
                service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
            try {
              if (bean != null) {
                if (bean.getClientIds().length > 1) {
                  return true;
                }
              }
              return false;
            } catch (Exception e) {
              LogWrapper.getInstance(cache)
                  .warning("waitForListClientMbean Exception in waitForListClientMbean ::: "
                      + ExceptionUtils.getStackTrace(e));
            }
            return false;
          }
        }));
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Host.getHost(0).getVM(0).invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    Host.getHost(0).getVM(1).invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    Host.getHost(0).getVM(2).invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    Host.getHost(0).getVM(3).invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }
}
