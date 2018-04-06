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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.DistributedTestUtils.getDUnitLocatorPort;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.exception.ExceptionUtils;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.VM;

class ClientCommandsTestUtils extends CliCommandTestBase {
  static Properties getServerProperties() {
    Properties p = new Properties();
    p.setProperty(LOCATORS, "localhost[" + getDUnitLocatorPort() + "]");
    p.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    return p;
  }

  static String getDistributedMemberId() {
    return GemFireCacheImpl.getInstance().getDistributedSystem().getDistributedMember().getId();
  }

  static DistributedMember getMember(final VM vm) {
    return vm.invoke("Get Member",
        () -> GemFireCacheImpl.getInstance().getDistributedSystem().getDistributedMember());
  }

  static void closeNonDurableClient(final VM vm) {
    vm.invoke("Stop client", () -> ClientCacheFactory.getAnyInstance().close(true));
  }

  static void closeCacheServer(final VM vm) {
    vm.invoke("Stop client", () -> {
      for (CacheServer cacheServer : CacheFactory.getAnyInstance().getCacheServers()) {
        cacheServer.stop();
      }
    });
  }

  static Properties getNonDurableClientProps() {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(LOCATORS, "");
    return p;
  }

  static void setupCqsOnVM(String cq1, String cq2, String cq3, String regionName, VM vm) {
    vm.invoke("setup CQs", () -> {
      Cache cache = GemFireCacheImpl.getInstance();
      QueryService qs = cache.getQueryService();
      CqAttributesFactory cqAf = new CqAttributesFactory();
      try {
        qs.newCq(cq1, "select * from /" + regionName, cqAf.create(), true).execute();
        qs.newCq(cq2, "select * from /" + regionName + " where id = 1", cqAf.create(), true)
            .execute();
        qs.newCq(cq3, "select * from /" + regionName + " where id > 2", cqAf.create(), true)
            .execute();
        cache.getLogger()
            .info("setupCqs on vm created cqs = " + cache.getQueryService().getCqs().length);
      } catch (Exception e) {
        cache.getLogger().info("setupCqs on vm Exception " + ExceptionUtils.getStackTrace(e));
      }
      return true;
    });
  }

  static void verifyClientStats(CommandResult commandResultForClient, String serverName) {
    CompositeResultData resultData = (CompositeResultData) commandResultForClient.getResultData();
    CompositeResultData.SectionResultData section = resultData.retrieveSection("InfoSection");
    assertNotNull(section);

    for (int i = 0; i < 1; i++) {
      TabularResultData tableResultData = section.retrieveTableByIndex(i);
      getLogWriter().info("testDescribeClientWithServers getHeader=" + tableResultData.getHeader());
      assertNotNull(tableResultData);
      List<String> minConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MIN_CONN);
      List<String> maxConn = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_MAX_CONN);
      List<String> redundancy =
          tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_REDUNDANCY);
      List<String> numCqs = tableResultData.retrieveAllValues(CliStrings.DESCRIBE_CLIENT_CQs);
      getLogWriter().info("testDescribeClientWithServers getHeader numCqs =" + numCqs);

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
    }
  }
}
