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
package com.gemstone.gemfire.management.internal.cli.commands;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.management.CacheServerMXBean;
import com.gemstone.gemfire.management.ClientHealthStatus;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData.SectionResultData;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.FlakyTest;
import com.jayway.awaitility.Awaitility;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.management.ObjectName;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static com.gemstone.gemfire.test.dunit.Assert.*;
import static com.gemstone.gemfire.test.dunit.DistributedTestUtils.getDUnitLocatorPort;
import static com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter;
import static com.gemstone.gemfire.test.dunit.NetworkUtils.getServerHostName;

/**
 * Dunit class for testing gemfire Client commands : list client , describe client
 *
 * @since GemFire 8.0
 */
@Category(DistributedTest.class)
public class ClientCommandsDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;
  final String regionName = "stocks";
  final String cq1 = "cq1";
  final String cq2 = "cq2";
  final String cq3 = "cq3";
  String clientId = "";
  int port0 = 0;
  int port1 = 0;

  public void waitForListClientMbean() {

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);

    final DistributedMember serverMember = getMember(server1);

    assertNotNull(serverMember);

    manager.invoke(() -> {
      Awaitility.waitAtMost(2 * 60, TimeUnit.SECONDS).pollDelay(2, TimeUnit.SECONDS).until(() -> {
        final SystemManagementService service = (SystemManagementService) ManagementService.getManagementService(getCache());
        if (service == null) {
          getLogWriter().info("waitForListClientMbean Still probing for service");
          return false;
        } else {
          final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
          CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
          try {
            if (bean != null) {
              if (bean.getClientIds().length > 1) {
                return true;
              }
            }
            return false;

          } catch (Exception e) {
            LogWrapper.getInstance().warning("waitForListClientMbean Exception in waitForListClientMbean ::: " + CliUtil.stackTraceAsString(e));
          }
          return false;
        }
      });
    });
  }

  public void waitForMbean() {

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);

    final DistributedMember serverMember = getMember(server1);

    assertNotNull(serverMember);

    manager.invoke(() -> {
      Awaitility.waitAtMost(2 * 60, TimeUnit.SECONDS).pollDelay(2, TimeUnit.SECONDS).until(() -> {
        final SystemManagementService service = (SystemManagementService) ManagementService.getManagementService(getCache());
        if (service == null) {
          getLogWriter().info("waitForMbean Still probing for service");
          return false;
        } else {
          final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
          CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
          try {
            ClientHealthStatus stats = bean.showClientStats(bean.getClientIds()[0]);
            Map<String, String> poolStats = stats.getPoolStats();
            if (poolStats.size() > 0) {
              Iterator<Entry<String, String>> it = poolStats.entrySet().iterator();
              while (it.hasNext()) {
                Entry<String, String> entry = it.next();
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
            LogWrapper.getInstance().warning("waitForMbean Exception in waitForMbean ::: " + CliUtil.stackTraceAsString(e));
          }
          return false;

        }
      });
    });
  }

  public void waitForListClientMbean3() {

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM server2 = Host.getHost(0).getVM(3);

    final DistributedMember serverMember1 = getMember(server1);
    final DistributedMember serverMember2 = getMember(server2);

    assertNotNull(serverMember1);

    manager.invoke(() -> {
      Awaitility.waitAtMost(2 * 60, TimeUnit.SECONDS).pollDelay(2, TimeUnit.SECONDS).until(() -> {
        final SystemManagementService service = (SystemManagementService) ManagementService.getManagementService(getCache());
        if (service == null) {
          getLogWriter().info("waitForListClientMbean3 Still probing for service");
          return false;
        } else {
          final ObjectName cacheServerMBeanName1 = service.getCacheServerMBeanName(port0, serverMember1);
          final ObjectName cacheServerMBeanName2 = service.getCacheServerMBeanName(port1, serverMember2);
          CacheServerMXBean bean1 = service.getMBeanProxy(cacheServerMBeanName1, CacheServerMXBean.class);
          CacheServerMXBean bean2 = service.getMBeanProxy(cacheServerMBeanName2, CacheServerMXBean.class);
          try {
            if (bean1 != null && bean2 != null) {
              if (bean1.getClientIds().length > 0 && bean2.getClientIds().length > 0) {
                return true;
              }
            }
            return false;

          } catch (Exception e) {
            LogWrapper.getInstance().warning("waitForListClientMbean3 Exception in waitForListClientMbean ::: " + CliUtil.stackTraceAsString(e));
          }
          return false;
        }
      });
    });
  }

  @Ignore("disabled for unknown reason")
  @Test
  public void testDescribeClientWithServers3() throws Exception {
    setupSystem3();
    String commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID + "=\"" + clientId + "\"";
    final VM server1 = Host.getHost(0).getVM(1);
    final VM server2 = Host.getHost(0).getVM(3);
    final VM manager = Host.getHost(0).getVM(0);
    String serverName1 = (String) server1.invoke("get DistributedMemberID ", () ->
        getDistributedMemberId());

    String serverName2 = (String) server2.invoke("get DistributedMemberID ", () ->
        getDistributedMemberId());

    final DistributedMember serverMember1 = getMember(server1);

    String[] clientIds = (String[]) manager.invoke("get Client Ids", () -> {
      final SystemManagementService service = (SystemManagementService) ManagementService.getManagementService(getCache());

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

    String[] clientIds2 = (String[]) manager.invoke("get Client Ids", () -> {
      final SystemManagementService service = (SystemManagementService) ManagementService.getManagementService(getCache());

      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port1, serverMember2);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);

      return bean.getClientIds();
    });

    String clientId2 = "";

    for (String str : clientIds2) {
      clientId2 = str;
      getLogWriter().info("testDescribeClientWithServers clientIds for server2 =" + str);
    }

    commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID + "=\"" + clientId1 + "\"";

    getLogWriter().info("testDescribeClientWithServers commandStr clientId1 =" + commandString);

    CommandResult commandResultForClient1 = executeCommand(commandString);
    getLogWriter().info("testDescribeClientWithServers commandStr clientId1=" + commandResultForClient1);

    String resultAsString = commandResultToString(commandResultForClient1);
    getLogWriter().info("testDescribeClientWithServers commandStr clientId1 =" + resultAsString);
    assertTrue(Status.OK.equals(commandResultForClient1.getStatus()));

    verifyClientStats(commandResultForClient1, serverName1);

    commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID + "=\"" + clientId2 + "\"";

    getLogWriter().info("testDescribeClientWithServers commandStr1=" + commandString);

    CommandResult commandResultForClient2 = executeCommand(commandString);
    getLogWriter().info("testDescribeClientWithServers commandResult1=" + commandResultForClient2);

    resultAsString = commandResultToString(commandResultForClient2);
    getLogWriter().info("testDescribeClientWithServers resultAsString1=" + resultAsString);
    assertTrue(Status.OK.equals(commandResultForClient2.getStatus()));

    verifyClientStats(commandResultForClient2, serverName2);

    closeNonDurableClient(Host.getHost(0).getVM(2));
    closeCacheServer(Host.getHost(0).getVM(3));
    closeCacheServer(Host.getHost(0).getVM(1));

  }

  public void verifyClientStats(CommandResult commandResultForClient, String serverName) {
    CompositeResultData resultData = (CompositeResultData) commandResultForClient.getResultData();
    SectionResultData section = resultData.retrieveSection("InfoSection");
    assertNotNull(section);
    for (int i = 0; i < 1; i++) {
      TabularResultData tableRsultData = section.retrieveTableByIndex(i);
      getLogWriter().info("testDescribeClientWithServers getHeader=" + tableRsultData.getHeader());
      assertNotNull(tableRsultData);

      List<String> minConn = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MIN_CONN);
      List<String> maxConn = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MAX_CONN);
      List<String> redudancy = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_REDUDANCY);
      List<String> numCqs = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_CQs);

      getLogWriter().info("testDescribeClientWithServers getHeader numCqs =" + numCqs);

      assertTrue(minConn.contains("1"));
      assertTrue(maxConn.contains("-1"));
      assertTrue(redudancy.contains("1"));
      assertTrue(numCqs.contains("3"));
      String puts = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS);
      assertTrue(puts.equals("2"));
      String queue = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_QUEUE_SIZE);
      assertTrue(queue.equals("1"));
      String calls = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTNER_CALLS);
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

    }
  }

  @Ignore("disabled for unknown reason")
  @Test
  public void testDescribeClient() throws Exception {
    setupSystem();

    getLogWriter().info("testDescribeClient clientId=" + clientId);
    assertNotNull(clientId);

    String commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID + "=\"" + clientId + "\"";
    getLogWriter().info("testDescribeClient commandStr=" + commandString);

    final VM server1 = Host.getHost(0).getVM(1);
    String serverName = (String) server1.invoke("get distributed member Id", () -> getDistributedMemberId());

    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("testDescribeClient commandResult=" + commandResult);

    String resultAsString = commandResultToString(commandResult);
    getLogWriter().info("testDescribeClient resultAsString=" + resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    SectionResultData section = resultData.retrieveSection("InfoSection");
    assertNotNull(section);
    TabularResultData tableRsultData = section.retrieveTable("Pool Stats For Pool Name = DEFAULT");
    assertNotNull(tableRsultData);

    List<String> minConn = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MIN_CONN);
    List<String> maxConn = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MAX_CONN);
    List<String> redudancy = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_REDUDANCY);
    List<String> numCqs = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_CQs);

    assertTrue(minConn.contains("1"));
    assertTrue(maxConn.contains("-1"));
    assertTrue(redudancy.contains("1"));
    assertTrue(numCqs.contains("3"));
    String puts = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS);
    assertTrue(puts.equals("2"));
    String queue = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_QUEUE_SIZE);
    assertTrue(queue.equals("1"));
    String calls = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTNER_CALLS);
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

    closeNonDurableClient(Host.getHost(0).getVM(2));
    closeCacheServer(Host.getHost(0).getVM(1));
    closeCacheServer(Host.getHost(0).getVM(3));

  }

  @Test
  public void testDescribeClientWithServers() throws Exception {
    setupSystem2();

    String commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID + "=\"" + clientId + "\"";
    getLogWriter().info("testDescribeClientWithServers commandStr=" + commandString);

    final VM server1 = Host.getHost(0).getVM(1);
    String serverName = (String) server1.invoke("get Distributed Member Id", () ->
        getDistributedMemberId());

    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("testDescribeClientWithServers commandResult=" + commandResult);

    String resultAsString = commandResultToString(commandResult);
    getLogWriter().info("testDescribeClientWithServers resultAsString=" + resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    SectionResultData section = resultData.retrieveSection("InfoSection");
    assertNotNull(section);
    TabularResultData tableRsultData = section.retrieveTable("Pool Stats For Pool Name = DEFAULT");
    assertNotNull(tableRsultData);

    List<String> minConn = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MIN_CONN);
    List<String> maxConn = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MAX_CONN);
    List<String> redudancy = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_REDUDANCY);
    List<String> numCqs = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_CQs);

    assertTrue(minConn.contains("1"));
    assertTrue(maxConn.contains("-1"));
    assertTrue(redudancy.contains("1"));
    assertTrue(numCqs.contains("3"));
    String puts = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS);
    assertTrue(puts.equals("2"));
    String queue = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_QUEUE_SIZE);
    assertTrue(queue.equals("1"));
    String calls = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTNER_CALLS);
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

    closeNonDurableClient(Host.getHost(0).getVM(2));
    closeNonDurableClient(Host.getHost(0).getVM(3));
    closeCacheServer(Host.getHost(0).getVM(1));

  }

  @Category(FlakyTest.class) // GEODE-908: random ports, BindException, time sensitive, HeadlessGfsh
  @Test
  public void testListClient() throws Exception {
    setupSystemForListClient();

    final VM manager = Host.getHost(0).getVM(0);

    String commandString = CliStrings.LIST_CLIENTS;
    getLogWriter().info("testListClient commandStr=" + commandString);

    waitForListClientMbean();

    final VM server1 = Host.getHost(0).getVM(1);

    final DistributedMember serverMember = getMember(server1);

    String[] clientIds = (String[]) manager.invoke("get client Ids", () -> {
      final SystemManagementService service = (SystemManagementService) ManagementService.getManagementService(getCache());
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds();
    });

    String serverName = (String) server1.invoke("get distributed member Id", () ->
        getDistributedMemberId());

    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("testListClient commandResult=" + commandResult);

    String resultAsString = commandResultToString(commandResult);
    getLogWriter().info("testListClient resultAsString=" + resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    SectionResultData section = resultData.retrieveSection("section1");
    assertNotNull(section);
    TabularResultData tableRsultData = section.retrieveTable("TableForClientList");
    assertNotNull(tableRsultData);

    List<String> serverNames = tableRsultData.retrieveAllValues(CliStrings.LIST_CLIENT_COLUMN_SERVERS);
    List<String> clientNames = tableRsultData.retrieveAllValues(CliStrings.LIST_CLIENT_COLUMN_Clients);

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
    closeNonDurableClient(Host.getHost(0).getVM(2));
    closeCacheServer(Host.getHost(0).getVM(1));
    closeCacheServer(Host.getHost(0).getVM(3));

  }

  @Test
  public void testListClientForServers() throws Exception {
    setupSystem3();

    final VM manager = Host.getHost(0).getVM(0);

    String commandString = CliStrings.LIST_CLIENTS;
    System.out.println("testListClientForServers commandStr=" + commandString);

    final VM server1 = Host.getHost(0).getVM(1);
    final VM server2 = Host.getHost(0).getVM(3);

    final DistributedMember serverMember = getMember(server1);

    String[] clientIds = (String[]) manager.invoke("get client Ids", () -> {
      final SystemManagementService service = (SystemManagementService) ManagementService.getManagementService(getCache());
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds();
    });

    String serverName1 = (String) server1.invoke("get distributed member Id", () ->
        getDistributedMemberId());

    String serverName2 = (String) server2.invoke("get distributed member Id", () ->
        getDistributedMemberId());

    CommandResult commandResult = executeCommand(commandString);
    System.out.println("testListClientForServers commandResult=" + commandResult);

    String resultAsString = commandResultToString(commandResult);
    System.out.println("testListClientForServers resultAsString=" + resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    SectionResultData section = resultData.retrieveSection("section1");
    assertNotNull(section);
    TabularResultData tableRsultData = section.retrieveTable("TableForClientList");
    assertNotNull(tableRsultData);

    List<String> serverNames = tableRsultData.retrieveAllValues(CliStrings.LIST_CLIENT_COLUMN_SERVERS);
    List<String> clientNames = tableRsultData.retrieveAllValues(CliStrings.LIST_CLIENT_COLUMN_Clients);

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

    closeNonDurableClient(Host.getHost(0).getVM(2));
    closeCacheServer(Host.getHost(0).getVM(1));
    closeCacheServer(Host.getHost(0).getVM(3));

  }

  private String getDistributedMemberId() {
    return GemFireCacheImpl.getInstance().getDistributedSystem().getDistributedMember().getId();
  }

  public DistributedMember getMember(final VM vm) {
    return (DistributedMember) vm.invoke("Get Member", () -> GemFireCacheImpl.getInstance().getDistributedSystem().getDistributedMember());
  }

  private void setupSystemForListClient() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());

    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM client2 = Host.getHost(0).getVM(3);

    port0 = startCacheServer(server1, 0, false, regionName);
    startNonDurableClient(client1, server1, port0);
    startNonDurableClient(client2, server1, port0);
  }

  private void setupSystem() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM server2 = Host.getHost(0).getVM(3);

    port0 = startCacheServer(server1, 0, false, regionName);
    startCacheServer(server2, 0, false, regionName);

    startNonDurableClient(client1, server1, port0);
    setupCqsOnVM(client1);
    waitForMbean();

    clientId = (String) manager.invoke("get client Id", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      SystemManagementService service = (SystemManagementService) ManagementService.getExistingManagementService(cache);
      DistributedMember serverMember = getMember(server1);
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds()[0];
    });
  }

  private void setupSystem2() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM client2 = Host.getHost(0).getVM(3);

    port0 = startCacheServer(server1, 0, false, regionName);
    startNonDurableClient(client1, server1, port0);
    startNonDurableClient(client2, server1, port0);

    setupCqsOnVM(client1);
    setupCqsOnVM(client2);

    waitForMbean();

    clientId = (String) manager.invoke("get client Id", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      SystemManagementService service = (SystemManagementService) ManagementService.getExistingManagementService(cache);
      DistributedMember serverMember = getMember(server1);
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds()[0];
    });
  }

  private void setupSystem3() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM server2 = Host.getHost(0).getVM(3);

    port0 = startCacheServer(server1, 0, false, regionName);
    port1 = startCacheServer(server2, 0, false, regionName);
    startNonDurableClient(client1, server1, port0);
    startNonDurableClient(client1, server2, port1);

    setupCqsOnVM(client1);
    waitForListClientMbean3();

    clientId = (String) manager.invoke("get client Id", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      SystemManagementService service = (SystemManagementService) ManagementService.getExistingManagementService(cache);
      DistributedMember serverMember = getMember(server1);
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds()[0];
    });

  }

  private void setupCqsOnVM(VM vm) {
    vm.invoke("setup CQs", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      QueryService qs = cache.getQueryService();
      CqAttributesFactory cqAf = new CqAttributesFactory();
      try {
        qs.newCq(cq1, "select * from /" + regionName, cqAf.create(), true).execute();
        qs.newCq(cq2, "select * from /" + regionName + " where id = 1", cqAf.create(), true).execute();
        qs.newCq(cq3, "select * from /" + regionName + " where id > 2", cqAf.create(), true).execute();
        cache.getLogger().info("setupCqs on vm created cqs = " + cache.getQueryService().getCqs().length);
      } catch (Exception e) {
        cache.getLogger().info("setupCqs on vm Exception " + CliUtil.stackTraceAsString(e));
      }
      return true;
    });
  }

  private int startCacheServer(VM server, final int port,
      final boolean createPR, final String regionName) throws Exception {

    return server.invoke("setup CacheServer", () -> {
      getSystem(getServerProperties());

      GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
      AttributesFactory factory = new AttributesFactory();
      if (createPR) {
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(11);
        factory.setPartitionAttributes(paf.create());
      } else {
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.REPLICATE);
      }
      Region region = createRootRegion(regionName, factory.create());
      if (createPR) {
        assertTrue(region instanceof PartitionedRegion);
      } else {
        assertTrue(region instanceof DistributedRegion);
      }
      CacheServer cacheServer = cache.addCacheServer();
      cacheServer.setPort(port);
      cacheServer.start();
      return cacheServer.getPort();
    });
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

        ClientCache clientCache = (ClientCache) getClientCache(ccf);
        //Create region
        if (clientCache.getRegion(Region.SEPARATOR + regionName) == null && clientCache.getRegion(regionName) == null) {
          ClientRegionFactory regionFactory = clientCache.createClientRegionFactory(ClientRegionShortcut.LOCAL)
              .setPoolName(clientCache.getDefaultPool().getName());
          Region dataRegion = regionFactory.create(regionName);
          assertNotNull(dataRegion);
          dataRegion.put("k1", "v1");
          dataRegion.put("k2", "v2");

        }
      } else {
        String poolName = "new_pool_" + System.currentTimeMillis();
        try {
          PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(getServerHostName(server.getHost()), port)
              .setThreadLocalConnections(true)
              .setMinConnections(1)
              .setSubscriptionEnabled(true)
              .setPingInterval(1)
              .setStatisticInterval(1)
              .setMinConnections(1)
              .setSubscriptionRedundancy(1)
              .create(poolName);
          System.out.println("Created new pool pool " + poolName);
          assertNotNull(p);
        } catch (Exception eee) {
          System.err.println("Exception in creating pool " + poolName + "    Exception ==" + CliUtil.stackTraceAsString(eee));
        }
      }
    });
  }

  //Closes the non-durable-client from the client side.
  private void closeNonDurableClient(final VM vm) {
    vm.invoke("Stop client", () -> ClientCacheFactory.getAnyInstance().close(true));
  }

  private void closeCacheServer(final VM vm) {
    vm.invoke("Stop client", () -> {
      Iterator<CacheServer> it = CacheFactory.getAnyInstance().getCacheServers().iterator();
      while (it.hasNext()) {
        CacheServer cacheServer = it.next();
        cacheServer.stop();
      }
    });
  }

  protected Properties getNonDurableClientProps() {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(LOCATORS, "");
    return p;
  }

  protected Properties getServerProperties() {
    Properties p = new Properties();
    p.setProperty(LOCATORS, "localhost[" + getDUnitLocatorPort() + "]");
    return p;
  }

  public void waitForNonSubCliMBean() {
    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final DistributedMember serverMember = getMember(server1);
    assertNotNull(serverMember);

    manager.invoke(() -> {
      Awaitility.waitAtMost(5 * 60, TimeUnit.SECONDS).pollDelay(2, TimeUnit.SECONDS).until(() -> {
        try {
          final SystemManagementService service = (SystemManagementService) ManagementService.getManagementService(getCache());
          if (service == null) {
            getLogWriter().info("waitForNonSubScribedClientMBean Still probing for service");
            return false;
          } else {
            getLogWriter().info("waitForNonSubScribedClientMBean 1");
            final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
            getLogWriter().info("waitForNonSubScribedClientMBean 2 cacheServerMBeanName " + cacheServerMBeanName);
            CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
            getLogWriter().info("waitForNonSubScribedClientMBean 2 bean " + bean);
            if (bean.getClientIds().length > 0) {
              return true;
            }
          }
        } catch (Exception e) {
          LogWrapper.getInstance().warning("waitForNonSubScribedClientMBean Exception in waitForMbean ::: " + CliUtil.stackTraceAsString(e));
        }
        return false;
      });
    });
  }

  public void waitForMixedClients() {

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);

    final DistributedMember serverMember = getMember(server1);

    assertNotNull(serverMember);

    manager.invoke(() -> {
      Awaitility.waitAtMost(5 * 60, TimeUnit.SECONDS).pollDelay(2, TimeUnit.SECONDS).until(() -> {
        try {
          final SystemManagementService service = (SystemManagementService) ManagementService.getManagementService(getCache());
          if (service == null) {
            getLogWriter().info("waitForMixedClients Still probing for service");
            return false;
          } else {
            getLogWriter().info("waitForMixedClients 1");
            final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
            getLogWriter().info("waitForMixedClients 2 cacheServerMBeanName " + cacheServerMBeanName);
            CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
            getLogWriter().info("waitForMixedClients 2 bean " + bean);
            if (bean.getClientIds().length > 1) {
              return true;
            }
          }
        } catch (Exception e) {
          LogWrapper.getInstance().warning("waitForMixedClients Exception in waitForMbean ::: " + CliUtil.stackTraceAsString(e));
        }
        return false;
      });
    });
  }

  @Category(FlakyTest.class) // GEODE-910: random ports, HeadlessGfsh
  @Test
  public void testDescribeClientForNonSubscribedClient() throws Exception {
    setUpNonSubscribedClient();

    getLogWriter().info("testDescribeClientForNonSubscribedClient clientId=" + clientId);
    assertNotNull(clientId);

    String commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID + "=\"" + clientId + "\"";
    getLogWriter().info("testDescribeClientForNonSubscribedClient commandStr=" + commandString);

    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("testDescribeClientForNonSubscribedClient commandResult=" + commandResult);

    String resultAsString = commandResultToString(commandResult);
    getLogWriter().info("testDescribeClientForNonSubscribedClient resultAsString=" + resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    SectionResultData section = resultData.retrieveSection("InfoSection");
    assertNotNull(section);
    TabularResultData tableRsultData = section.retrieveTable("Pool Stats For Pool Name = DEFAULT");
    assertNotNull(tableRsultData);

    List<String> minConn = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MIN_CONN);
    List<String> maxConn = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MAX_CONN);
    List<String> redudancy = tableRsultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_REDUDANCY);

    assertTrue(minConn.contains("1"));
    assertTrue(maxConn.contains("-1"));
    assertTrue(redudancy.contains("1"));

    String puts = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS);
    assertTrue(puts.equals("2"));

    String calls = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTNER_CALLS);
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

    closeNonDurableClient(Host.getHost(0).getVM(2));
    closeCacheServer(Host.getHost(0).getVM(1));
    closeCacheServer(Host.getHost(0).getVM(3));

  }

  @Category(FlakyTest.class) // GEODE-1227: HeadlessGFSH, random ports, BindException
  @Test
  public void testDescribeMixClientWithServers() throws Exception {
    String[] clientIds = setupSystemWithSubAndNonSubClient();

    final VM server1 = Host.getHost(0).getVM(1);
    String serverName = (String) server1.invoke("Get DistributedMember Id", () ->
        getDistributedMemberId());

    String commandString = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID + "=\"" + clientIds[0] + "\"";
    getLogWriter().info("testDescribeMixClientWithServers commandStr=" + commandString);

    executeAndVerifyResultsForMixedClients(commandString, serverName);

    String commandString2 = CliStrings.DESCRIBE_CLIENT + " --" + CliStrings.DESCRIBE_CLIENT__ID + "=\"" + clientIds[1] + "\"";
    getLogWriter().info("testDescribeMixClientWithServers commandString2=" + commandString2);

    executeAndVerifyResultsForMixedClients(commandString2, serverName);

    closeNonDurableClient(Host.getHost(0).getVM(2));
    closeNonDurableClient(Host.getHost(0).getVM(3));
    closeCacheServer(Host.getHost(0).getVM(1));

  }

  void executeAndVerifyResultsForMixedClients(String commandString, String serverName) {
    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("testDescribeMixClientWithServers commandResult=" + commandResult);

    String resultAsString = commandResultToString(commandResult);
    getLogWriter().info("testDescribeMixClientWithServers resultAsString=" + resultAsString);

    assertTrue(Status.OK.equals(commandResult.getStatus()));

    CompositeResultData resultData = (CompositeResultData) commandResult.getResultData();
    SectionResultData section = resultData.retrieveSection("InfoSection");
    assertNotNull(section);
    TabularResultData tableResultData = section.retrieveTable("Pool Stats For Pool Name = DEFAULT");
    assertNotNull(tableResultData);

    List<String> minConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MIN_CONN);
    List<String> maxConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MAX_CONN);
    List<String> redudancy = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_REDUDANCY);

    assertTrue(minConn.contains("1"));
    assertTrue(maxConn.contains("-1"));
    assertTrue(redudancy.contains("1"));

    String puts = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS);
    assertTrue(puts.equals("2"));

    String calls = section.retrieveString(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTNER_CALLS);
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

  private void setUpNonSubscribedClient() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM server2 = Host.getHost(0).getVM(3);

    port0 = startCacheServer(server1, 0, false, regionName);
    startCacheServer(server2, 0, false, regionName);

    startNonSubscribedClient(client1, server1, port0);
    setupCqsOnVM(client1);
    waitForNonSubCliMBean();

    clientId = (String) manager.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Cache cache = GemFireCacheImpl.getInstance();
        SystemManagementService service = (SystemManagementService) ManagementService.getExistingManagementService(cache);
        DistributedMember serverMember = getMember(server1);
        final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
        CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
        return bean.getClientIds()[0];
      }
    });
  }

  private String[] setupSystemWithSubAndNonSubClient() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());

    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    final VM client2 = Host.getHost(0).getVM(3);

    port0 = startCacheServer(server1, 0, false, regionName);
    startNonDurableClient(client1, server1, port0);
    startNonSubscribedClient(client2, server1, port0);

    waitForMixedClients();

    String[] cliendIds = (String[]) manager.invoke("get client Ids", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      SystemManagementService service = (SystemManagementService) ManagementService.getExistingManagementService(cache);
      DistributedMember serverMember = getMember(server1);
      final ObjectName cacheServerMBeanName = service.getCacheServerMBeanName(port0, serverMember);
      CacheServerMXBean bean = service.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
      return bean.getClientIds();
    });

    return cliendIds;
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

        ClientCache clientCache = (ClientCache) getClientCache(ccf);
        //Create region
        if (clientCache.getRegion(Region.SEPARATOR + regionName) == null && clientCache.getRegion(regionName) == null) {
          ClientRegionFactory regionFactory = clientCache.createClientRegionFactory(ClientRegionShortcut.LOCAL)
              .setPoolName(clientCache.getDefaultPool().getName());
          Region dataRegion = regionFactory.create(regionName);
          assertNotNull(dataRegion);
          dataRegion.put("k1", "v1");
          dataRegion.put("k2", "v2");

        }
      } else {
        String poolName = "new_pool_" + System.currentTimeMillis();
        try {
          PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer(getServerHostName(server.getHost()), port)
              .setThreadLocalConnections(true)
              .setMinConnections(1)
              .setSubscriptionEnabled(false)
              .setPingInterval(1)
              .setStatisticInterval(1)
              .setMinConnections(1)
              .setSubscriptionRedundancy(1)
              .create(poolName);
          cache.getLogger().info("Created new pool pool " + poolName);
          assertNotNull(p);
        } catch (Exception eee) {
          cache.getLogger().info("Exception in creating pool " + poolName + "    Exception ==" + CliUtil.stackTraceAsString(eee));
        }
      }
    });
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Host.getHost(0).getVM(0).invoke(() -> CacheServerTestUtil.closeCache());
    Host.getHost(0).getVM(1).invoke(() -> CacheServerTestUtil.closeCache());
    Host.getHost(0).getVM(2).invoke(() -> CacheServerTestUtil.closeCache());
    Host.getHost(0).getVM(3).invoke(() -> CacheServerTestUtil.closeCache());
  }
}
