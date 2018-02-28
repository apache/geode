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
import static org.apache.geode.management.internal.cli.commands.ClientCommandsTestUtils.getMember;
import static org.apache.geode.management.internal.cli.commands.ClientCommandsTestUtils.getNonDurableClientProps;
import static org.apache.geode.management.internal.cli.commands.ClientCommandsTestUtils.getServerProperties;
import static org.apache.geode.management.internal.cli.commands.ClientCommandsTestUtils.setupCqsOnVM;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.awaitility.Awaitility;
import org.junit.Ignore;
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
import org.apache.geode.management.ClientHealthStatus;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category({DistributedTest.class, FlakyTest.class}) // GEODE-910 GEODE-3530
@SuppressWarnings("serial")
public class DescribeClientCommandDUnitTest extends CliCommandTestBase {

  private final String regionName = "stocks";
  private final String cq1 = "cq1";
  private final String cq2 = "cq2";
  private final String cq3 = "cq3";
  private String clientId = "";
  private int port0 = 0;
  private int port1 = 0;

  @Ignore("disabled for unknown reason")
  @Test
  public void testDescribeClientWithServers3() throws Exception {
    setupSystem3();
    String commandString;

    final VM server1 = Host.getHost(0).getVM(1);
    final VM server2 = Host.getHost(0).getVM(3);
    final VM manager = Host.getHost(0).getVM(0);

    String serverName1 =
        server1.invoke("get DistributedMemberID ", ClientCommandsTestUtils::getDistributedMemberId);
    String serverName2 =
        server2.invoke("get DistributedMemberID ", ClientCommandsTestUtils::getDistributedMemberId);

    final DistributedMember serverMember1 = getMember(server1);

    String[] clientIds = manager.invoke("get Client Ids", () -> {
      final SystemManagementService service =
          (SystemManagementService) ManagementService.getManagementService(getCache());
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember1);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds();
    });

    String clientId1 = "";

    for (String str : clientIds) {
      clientId1 = str;
      getLogWriter().info("testDescribeClientWithServers clientIds for server1 =" + str);
    }

    final DistributedMember serverMember2 = getMember(server2);

    String[] clientIds2 = manager.invoke("get Client Ids", () -> {
      final SystemManagementService service =
          (SystemManagementService) ManagementService.getManagementService(getCache());
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port1, serverMember2);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds();
    });

    String clientId2 = "";

    for (String str : clientIds2) {
      clientId2 = str;
      getLogWriter().info("testDescribeClientWithServers clientIds for server2 =" + str);
    }

    commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID + "=\""
        + clientId1 + "\"";
    getLogWriter().info("testDescribeClientWithServers commandStr clientId1 =" + commandString);
    CommandResult commandResultForClient1 = executeCommand(commandString);
    getLogWriter()
        .info("testDescribeClientWithServers commandStr clientId1=" + commandResultForClient1);
    String resultAsString = commandResultToString(commandResultForClient1);
    getLogWriter().info("testDescribeClientWithServers commandStr clientId1 =" + resultAsString);
    assertTrue(Result.Status.OK.equals(commandResultForClient1.getStatus()));
    ClientCommandsTestUtils.verifyClientStats(commandResultForClient1, serverName1);
    commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID + "=\""
        + clientId2 + "\"";
    getLogWriter().info("testDescribeClientWithServers commandStr1=" + commandString);
    CommandResult commandResultForClient2 = executeCommand(commandString);
    getLogWriter().info("testDescribeClientWithServers commandResult1=" + commandResultForClient2);
    resultAsString = commandResultToString(commandResultForClient2);
    getLogWriter().info("testDescribeClientWithServers resultAsString1=" + resultAsString);
    assertTrue(Result.Status.OK.equals(commandResultForClient2.getStatus()));
    ClientCommandsTestUtils.verifyClientStats(commandResultForClient2, serverName2);
    ClientCommandsTestUtils.closeNonDurableClient(Host.getHost(0).getVM(2));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(3));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(1));
  }

  @Ignore("disabled for unknown reason")
  @Test
  public void testDescribeClient() throws Exception {
    setupSystem1();

    getLogWriter().info("testDescribeClient clientId=" + clientId);
    assertNotNull(clientId);

    String commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID
        + "=\"" + clientId + "\"";
    getLogWriter().info("testDescribeClient commandStr=" + commandString);

    final VM server1 = Host.getHost(0).getVM(1);

    String serverName = server1.invoke("get distributed member Id",
        ClientCommandsTestUtils::getDistributedMemberId);

    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("testDescribeClient commandResult=" + commandResult);
    String resultAsString = commandResultToString(commandResult);
    getLogWriter().info("testDescribeClient resultAsString=" + resultAsString);
    assertTrue(Result.Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    CompositeResultData.SectionResultData section = resultData.retrieveSection("InfoSection");
    assertNotNull(section);
    TabularResultData tableResultData = section.retrieveTable("Pool Stats For Pool Name = DEFAULT");
    assertNotNull(tableResultData);

    List<String> minConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MIN_CONN);
    List<String> maxConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MAX_CONN);
    List<String> redundancy =
        tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_REDUNDANCY);
    List<String> numCqs = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_CQs);

    assertTrue(minConn.contains("1"));
    assertTrue(maxConn.contains("-1"));
    assertTrue(redundancy.contains("1"));
    assertTrue(numCqs.contains("3"));
    String puts = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS);
    assertTrue(puts.equals("2"));
    String queue = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_QUEUE_SIZE);
    assertTrue(queue.equals("1"));
    String calls = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTENER_CALLS);
    assertTrue(calls.equals("1"));
    String primServer = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PRIMARY_SERVERS);
    assertTrue(primServer.equals(serverName));
    String durable = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_DURABLE);
    assertTrue(durable.equals("No"));
    String threads = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_THREADS);
    assertTrue(Integer.parseInt(threads) > 0);
    String cpu = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_CPU);
    assertTrue(Integer.parseInt(cpu) > 0);
    String upTime = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_UP_TIME);
    assertTrue(Integer.parseInt(upTime) >= 0);
    String prcTime = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PROCESS_CPU_TIME);
    assertTrue(Long.parseLong(prcTime) > 0);

    ClientCommandsTestUtils.closeNonDurableClient(Host.getHost(0).getVM(2));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(1));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(3));
  }

  @Test
  public void testDescribeClientWithServers() throws Exception {
    setupSystem2();

    String commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID
        + "=\"" + clientId + "\"";
    getLogWriter().info("testDescribeClientWithServers commandStr=" + commandString);

    final VM server1 = Host.getHost(0).getVM(1);

    String serverName = server1.invoke("get Distributed Member Id",
        ClientCommandsTestUtils::getDistributedMemberId);
    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("testDescribeClientWithServers commandResult=" + commandResult);

    String resultAsString = commandResultToString(commandResult);
    getLogWriter().info("testDescribeClientWithServers resultAsString=" + resultAsString);
    assertTrue(Result.Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    CompositeResultData.SectionResultData section = resultData.retrieveSection("InfoSection");
    assertNotNull(section);

    TabularResultData tableResultData = section.retrieveTable("Pool Stats For Pool Name = DEFAULT");
    assertNotNull(tableResultData);

    List<String> minConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MIN_CONN);
    List<String> maxConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MAX_CONN);
    List<String> redundancy =
        tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_REDUNDANCY);
    List<String> numCqs = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_CQs);

    assertTrue(minConn.contains("1"));
    assertTrue(maxConn.contains("-1"));
    assertTrue(redundancy.contains("1"));
    assertTrue(numCqs.contains("3"));
    String puts = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS);
    assertTrue(puts.equals("2"));
    String queue = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_QUEUE_SIZE);
    assertTrue(queue.equals("1"));
    String calls = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTENER_CALLS);
    assertTrue(calls.equals("1"));
    String primServer = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PRIMARY_SERVERS);
    assertTrue(primServer.equals(serverName));
    String durable = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_DURABLE);
    assertTrue(durable.equals("No"));
    String threads = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_THREADS);
    assertTrue(Integer.parseInt(threads) > 0);
    String cpu = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_CPU);
    assertTrue(Integer.parseInt(cpu) > 0);
    String upTime = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_UP_TIME);
    assertTrue(Integer.parseInt(upTime) >= 0);
    String prcTime = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PROCESS_CPU_TIME);
    assertTrue(Long.parseLong(prcTime) > 0);

    ClientCommandsTestUtils.closeNonDurableClient(Host.getHost(0).getVM(2));
    ClientCommandsTestUtils.closeNonDurableClient(Host.getHost(0).getVM(3));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(1));
  }

  @Test // FlakyTest: GEODE-910
  public void testDescribeClientForNonSubscribedClient() throws Exception {
    setUpNonSubscribedClient();

    getLogWriter().info("testDescribeClientForNonSubscribedClient clientId=" + clientId);
    assertNotNull(clientId);

    String commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID
        + "=\"" + clientId + "\"";
    getLogWriter().info("testDescribeClientForNonSubscribedClient commandStr=" + commandString);
    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("testDescribeClientForNonSubscribedClient commandResult=" + commandResult);
    String resultAsString = commandResultToString(commandResult);
    getLogWriter()
        .info("testDescribeClientForNonSubscribedClient resultAsString=" + resultAsString);
    assertTrue(Result.Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    CompositeResultData.SectionResultData section = resultData.retrieveSection("InfoSection");
    assertNotNull(section);

    TabularResultData tableResultData = section.retrieveTable("Pool Stats For Pool Name = DEFAULT");
    assertNotNull(tableResultData);

    List<String> minConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MIN_CONN);
    List<String> maxConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MAX_CONN);
    List<String> redundancy =
        tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_REDUNDANCY);

    assertTrue(minConn.contains("1"));
    assertTrue(maxConn.contains("-1"));
    assertTrue(redundancy.contains("1"));
    String puts = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS);
    assertTrue(puts.equals("2"));
    String calls = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTENER_CALLS);
    assertTrue(calls.equals("1"));
    String primServer = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PRIMARY_SERVERS);
    assertTrue(primServer.equals("N.A."));
    String durable = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_DURABLE);
    assertTrue(durable.equals("No"));
    String threads = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_THREADS);
    assertTrue(Integer.parseInt(threads) > 0);
    String cpu = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_CPU);
    assertTrue(Integer.parseInt(cpu) > 0);
    String upTime = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_UP_TIME);
    assertTrue(Integer.parseInt(upTime) == 0);
    String prcTime = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PROCESS_CPU_TIME);
    assertTrue(Long.parseLong(prcTime) > 0);

    ClientCommandsTestUtils.closeNonDurableClient(Host.getHost(0).getVM(2));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(1));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(3));
  }

  @Test
  public void testDescribeMixClientWithServers() throws Exception {
    String[] clientIds = setupSystemWithSubAndNonSubClient();
    final VM server1 = Host.getHost(0).getVM(1);
    String serverName =
        server1.invoke("Get DistributedMember Id", ClientCommandsTestUtils::getDistributedMemberId);
    String commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID
        + "=\"" + clientIds[0] + "\"";
    getLogWriter().info("testDescribeMixClientWithServers commandStr=" + commandString);
    executeAndVerifyResultsForMixedClients(commandString, serverName);
    String commandString2 = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID
        + "=\"" + clientIds[1] + "\"";
    getLogWriter().info("testDescribeMixClientWithServers commandString2=" + commandString2);
    executeAndVerifyResultsForMixedClients(commandString2, serverName);

    ClientCommandsTestUtils.closeNonDurableClient(Host.getHost(0).getVM(2));
    ClientCommandsTestUtils.closeNonDurableClient(Host.getHost(0).getVM(3));
    ClientCommandsTestUtils.closeCacheServer(Host.getHost(0).getVM(1));
  }

  private void executeAndVerifyResultsForMixedClients(String commandString, String serverName) {
    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("testDescribeMixClientWithServers commandResult=" + commandResult);
    String resultAsString = commandResultToString(commandResult);
    getLogWriter().info("testDescribeMixClientWithServers resultAsString=" + resultAsString);
    assertTrue(Result.Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    CompositeResultData.SectionResultData section = resultData.retrieveSection("InfoSection");
    assertNotNull(section);
    TabularResultData tableResultData = section.retrieveTable("Pool Stats For Pool Name = DEFAULT");
    assertNotNull(tableResultData);

    List<String> minConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MIN_CONN);
    List<String> maxConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MAX_CONN);
    List<String> redundancy =
        tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_REDUNDANCY);

    assertTrue(minConn.contains("1"));
    assertTrue(maxConn.contains("-1"));
    assertTrue(redundancy.contains("1"));
    String puts = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS);
    assertTrue(puts.equals("2"));
    String calls = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTENER_CALLS);
    assertTrue(calls.equals("1"));
    String primServer = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PRIMARY_SERVERS);
    assertTrue(primServer.equals(serverName) || primServer.equals("N.A."));
    String durable = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_DURABLE);
    assertTrue(durable.equals("No"));
    String threads = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_THREADS);
    assertTrue(Integer.parseInt(threads) > 0);
    String cpu = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_CPU);
    assertTrue(Integer.parseInt(cpu) > 0);
    String upTime = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_UP_TIME);
    assertTrue(Integer.parseInt(upTime) >= 0);
    String prcTime = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PROCESS_CPU_TIME);
    assertTrue(Long.parseLong(prcTime) > 0);
  }

  private String[] setupSystemWithSubAndNonSubClient() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());
    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM client2 = Host.getHost(0).getVM(3);
    port0 = startCacheServer(server1, regionName);
    startNonDurableClient(client1, server1, port0);
    startNonSubscribedClient(client2, server1, port0);
    waitForMixedClients();
    return manager.invoke("get client Ids", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      SystemManagementService service =
          (SystemManagementService) ManagementService.getExistingManagementService(cache);
      DistributedMember serverMember = getMember(server1);
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds();
    });
  }

  private int startCacheServer(VM server, final String regionName) {
    return server.invoke("setup CacheServer", () -> {
      getSystem(getServerProperties());
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

  private void waitForMixedClients() {
    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final DistributedMember serverMember = getMember(server1);
    assertNotNull(serverMember);
    manager.invoke(() -> Awaitility.waitAtMost(5 * 60, TimeUnit.SECONDS)
        .pollDelay(2, TimeUnit.SECONDS).until(() -> {
          try {
            final SystemManagementService service =
                (SystemManagementService) ManagementService.getManagementService(getCache());
            if (service == null) {
              getLogWriter().info("waitForMixedClients Still probing for service");
              return false;
            } else {
              getLogWriter().info("waitForMixedClients 1");
              final ObjectName cacheServerMBeanName =
                  service.getCacheServerMBeanName(port0, serverMember);
              getLogWriter()
                  .info("waitForMixedClients 2 cacheServerMBeanName " + cacheServerMBeanName);
              CacheServerMXBean bean =
                  service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
              getLogWriter().info("waitForMixedClients 2 bean " + bean);
              if (bean.getClientIds().length > 1) {
                return true;
              }
            }
          } catch (Exception e) {
            LogWrapper.getInstance().warning("waitForMixedClients Exception in waitForMBean ::: "
                + ExceptionUtils.getStackTrace(e));
          }
          return false;
        }));
  }


  private void setupSystem1() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM server2 = Host.getHost(0).getVM(3);

    port0 = startCacheServer(server1, regionName);
    startCacheServer(server2, regionName);

    startNonDurableClient(client1, server1, port0);
    setupCqsOnVM(cq1, cq2, cq3, regionName, client1);
    waitForMBean();

    clientId = manager.invoke("get client Id", () -> getClientIdString(server1));
  }

  private void setupSystem2() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM client2 = Host.getHost(0).getVM(3);

    port0 = startCacheServer(server1, regionName);
    startNonDurableClient(client1, server1, port0);
    startNonDurableClient(client2, server1, port0);

    setupCqsOnVM(cq1, cq2, cq3, regionName, client1);
    setupCqsOnVM(cq1, cq2, cq3, regionName, client2);
    waitForMBean();

    clientId = manager.invoke("get client Id", () -> getClientIdString(server1));
  }

  private void setupSystem3() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM server2 = Host.getHost(0).getVM(3);

    port0 = startCacheServer(server1, regionName);
    port1 = startCacheServer(server2, regionName);
    startNonDurableClient(client1, server1, port0);
    startNonDurableClient(client1, server2, port1);

    setupCqsOnVM(cq1, cq2, cq3, regionName, client1);
    waitForListClientMBean3();

    clientId = manager.invoke("get client Id", () -> getClientIdString(server1));
  }

  private void startNonDurableClient(VM client, final VM server, final int port) {
    client.invoke("start non-durable client", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      if (cache == null) {

        Properties props = getNonDurableClientProps();
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

  private void waitForListClientMBean3() {
    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM server2 = Host.getHost(0).getVM(3);
    final DistributedMember serverMember1 = getMember(server1);
    final DistributedMember serverMember2 = getMember(server2);
    assertNotNull(serverMember1);
    manager.invoke(() -> Awaitility.waitAtMost(2 * 60, TimeUnit.SECONDS)
        .pollDelay(2, TimeUnit.SECONDS).until(() -> {
          final SystemManagementService service =
              (SystemManagementService) ManagementService.getManagementService(getCache());
          if (service == null) {
            getLogWriter().info("waitForListClientMBean3 Still probing for service");
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
              LogWrapper.getInstance()
                  .warning("waitForListClientMBean3 Exception in waitForListClientMbean ::: "
                      + ExceptionUtils.getStackTrace(e));
            }
            return false;
          }
        }));
  }

  private void waitForMBean() {
    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final DistributedMember serverMember = getMember(server1);
    assertNotNull(serverMember);
    manager.invoke(() -> Awaitility.waitAtMost(2 * 60, TimeUnit.SECONDS)
        .pollDelay(2, TimeUnit.SECONDS).until(() -> {
          final SystemManagementService service =
              (SystemManagementService) ManagementService.getManagementService(getCache());
          if (service == null) {
            getLogWriter().info("waitForMBean Still probing for service");
            return false;
          } else {
            final ObjectName cacheServerMBeanName =
                service.getCacheServerMBeanName(port0, serverMember);
            CacheServerMXBean bean =
                service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
            try {
              ClientHealthStatus stats = bean.showClientStats(bean.getClientIds()[0]);
              Map<String, String> poolStats = stats.getPoolStats();
              if (poolStats.size() > 0) {
                for (Map.Entry<String, String> entry : poolStats.entrySet()) {
                  String poolStatsStr = entry.getValue();
                  String str[] = poolStatsStr.split(";");
                  int numCqs = Integer.parseInt(str[3].substring(str[3].indexOf("=") + 1));
                  if (numCqs == 3) {
                    return true;
                  }
                }
              }
              return false;
            } catch (Exception e) {
              LogWrapper.getInstance().warning(
                  "waitForMBean Exception in waitForMBean ::: " + ExceptionUtils.getStackTrace(e));
            }
            return false;
          }
        }));
  }

  private void startNonSubscribedClient(VM client, final VM server, final int port) {
    client.invoke("Start client", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      if (cache == null) {
        Properties props = getNonDurableClientProps();
        props.setProperty(LOG_FILE, "client_" + OSProcess.getId() + ".log");
        props.setProperty(LOG_LEVEL, "fine");
        props.setProperty(STATISTIC_ARCHIVE_FILE, "client_" + OSProcess.getId() + ".gfs");
        props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
        getSystem(props);
        final ClientCacheFactory ccf = new ClientCacheFactory(props);
        ccf.addPoolServer(getServerHostName(server.getHost()), port);
        ccf.setPoolSubscriptionEnabled(false);
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
              .setMinConnections(1).setSubscriptionEnabled(false).setPingInterval(1)
              .setStatisticInterval(1).setMinConnections(1).setSubscriptionRedundancy(1)
              .create(poolName);
          cache.getLogger().info("Created new pool pool " + poolName);
          assertNotNull(p);
        } catch (Exception eee) {
          cache.getLogger().info("Exception in creating pool " + poolName + "    Exception =="
              + ExceptionUtils.getStackTrace(eee));
        }
      }
    });
  }

  private void setUpNonSubscribedClient() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());
    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM server2 = Host.getHost(0).getVM(3);
    port0 = startCacheServer(server1, regionName);
    startCacheServer(server2, regionName);
    startNonSubscribedClient(client1, server1, port0);
    setupCqsOnVM(cq1, cq2, cq3, regionName, client1);
    waitForNonSubCliMBean();
    clientId = (String) manager.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return getClientIdString(server1);
      }
    });
  }

  private void waitForNonSubCliMBean() {
    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final DistributedMember serverMember = getMember(server1);
    assertNotNull(serverMember);
    manager.invoke(() -> Awaitility.waitAtMost(5 * 60, TimeUnit.SECONDS)
        .pollDelay(2, TimeUnit.SECONDS).until(() -> {
          try {
            final SystemManagementService service =
                (SystemManagementService) ManagementService.getManagementService(getCache());
            if (service == null) {
              getLogWriter().info("waitForNonSubScribedClientMBean Still probing for service");
              return false;
            } else {
              getLogWriter().info("waitForNonSubScribedClientMBean 1");
              final ObjectName cacheServerMBeanName =
                  service.getCacheServerMBeanName(port0, serverMember);
              getLogWriter().info(
                  "waitForNonSubScribedClientMBean 2 cacheServerMBeanName " + cacheServerMBeanName);
              CacheServerMXBean bean =
                  service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
              getLogWriter().info("waitForNonSubScribedClientMBean 2 bean " + bean);
              if (bean.getClientIds().length > 0) {
                return true;
              }
            }
          } catch (Exception e) {
            LogWrapper.getInstance()
                .warning("waitForNonSubScribedClientMBean Exception in waitForMBean ::: "
                    + ExceptionUtils.getStackTrace(e));
          }
          return false;
        }));
  }

  private String getClientIdString(VM server1) throws Exception {
    Cache cache = GemFireCacheImpl.getInstance();
    SystemManagementService service =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);
    DistributedMember serverMember = getMember(server1);
    final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
    CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
    return bean.getClientIds()[0];
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Host.getHost(0).getVM(0).invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    Host.getHost(0).getVM(1).invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    Host.getHost(0).getVM(2).invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
    Host.getHost(0).getVM(3).invoke((SerializableRunnableIF) CacheServerTestUtil::closeCache);
  }
}
