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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.SerializableConsumerIF;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class, GfshTest.class})
@SuppressWarnings("serial")
public class DescribeClientCommandDUnitTest {

  private MemberVM locatorVm0;
  private MemberVM server1Vm1;
  private MemberVM server2Vm2;
  private ClientVM client1Vm3;
  private ClientVM client2Vm4;

  private static final String STOCKS_REGION = "stocks";

  @Rule
  public ClusterStartupRule rule = new ClusterStartupRule(5);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void setup() throws Exception {
    locatorVm0 = rule.startLocatorVM(0);
    server1Vm1 = rule.startServerVM(1, locatorVm0.getPort());
    server2Vm2 = rule.startServerVM(2, locatorVm0.getPort());

    server1Vm1.invoke(() -> {
      RegionFactory factory =
          ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
      factory.create(STOCKS_REGION);
    });

    server2Vm2.invoke(() -> {
      RegionFactory factory =
          ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
      factory.create(STOCKS_REGION);
    });

    gfsh.connectAndVerify(locatorVm0);
  }

  @Test
  public void describeClient() throws Exception {
    boolean subscriptionEnabled = true;
    client1Vm3 = createClient(3, subscriptionEnabled);
    setupCqsOnVM(client1Vm3, STOCKS_REGION, "cq1", "cq2", "cq3");

    client2Vm4 = createClient(4, subscriptionEnabled);
    setupCqsOnVM(client2Vm4, STOCKS_REGION, "cq1", "cq2", "cq3");

    waitForClientReady(3);

    validateResults(subscriptionEnabled);
  }

  @Test
  public void describeClientWithoutSubscription() throws Exception {
    boolean subscriptionEnabled = false;
    client1Vm3 = createClient(3, subscriptionEnabled);
    setupCqsOnVM(client1Vm3, STOCKS_REGION, "cq1", "cq2", "cq3");

    client2Vm4 = createClient(4, subscriptionEnabled);
    setupCqsOnVM(client2Vm4, STOCKS_REGION, "cq1", "cq2", "cq3");

    waitForClientReady(1);

    validateResults(subscriptionEnabled);
  }

  private void validateResults(boolean subscriptionEnabled) {
    CommandResult result = gfsh.executeCommand("list members");
    // list is always locator-0, server-1, server-2
    String server1 = result.getTableColumnValues("members", "Id").get(1);

    result = gfsh.executeCommand("list clients");
    String clientId = result.getColumnFromTableContent(CliStrings.LIST_CLIENT_COLUMN_Clients,
        "section1", "TableForClientList").get(0);

    result = gfsh.executeCommand("describe client --clientID=" + clientId);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    Map<String, List<String>> table =
        result.getMapFromTableContent("Pool Stats For Pool Name = DEFAULT");
    Map<String, String> data = result.getMapFromSection("InfoSection");

    assertThat(table.get(CliStrings.DESCRIBE_CLIENT_MIN_CONN).get(0)).isEqualTo("1");
    assertThat(table.get(CliStrings.DESCRIBE_CLIENT_MAX_CONN).get(0)).isEqualTo("-1");
    assertThat(table.get(CliStrings.DESCRIBE_CLIENT_REDUNDANCY).get(0)).isEqualTo("1");

    if (subscriptionEnabled) {
      assertThat(table.get(CliStrings.DESCRIBE_CLIENT_CQs).get(0)).isEqualTo("3");
      assertThat(Integer.parseInt(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_QUEUE_SIZE)))
          .isGreaterThanOrEqualTo(1);
      assertThat(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_PRIMARY_SERVERS)).isEqualTo(server1);
    } else {
      assertThat(table.get(CliStrings.DESCRIBE_CLIENT_CQs).get(0)).isEqualTo("1");
      assertThat(Integer.parseInt(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_QUEUE_SIZE)))
          .isEqualTo(0);
      assertThat(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_PRIMARY_SERVERS)).isEqualTo("N.A.");
    }

    assertThat(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS)).isEqualTo("2");
    assertThat(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTENER_CALLS)).isEqualTo("1");
    assertThat(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_DURABLE)).isEqualTo("No");
    assertThat(Integer.parseInt(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_THREADS)))
        .isGreaterThan(0);
    assertThat(Integer.parseInt(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_CPU))).isGreaterThan(0);
    assertThat(Integer.parseInt(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_UP_TIME)))
        .isGreaterThanOrEqualTo(0);
    assertThat(Long.parseLong(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_PROCESS_CPU_TIME)))
        .isGreaterThan(0);
  }

  void waitForClientReady(int cqsToWaitFor) {
    // Wait until all CQs are ready
    Awaitility.waitAtMost(20, TimeUnit.SECONDS).until(() -> {
      CommandResult r = gfsh.executeCommand("list clients");
      if (r.getStatus() != Result.Status.OK) {
        return false;
      }

      String clientId = r.getColumnFromTableContent(CliStrings.LIST_CLIENT_COLUMN_Clients,
          "section1", "TableForClientList").get(0);
      r = gfsh.executeCommand("describe client --clientID=" + clientId);
      Map<String, List<String>> table =
          r.getMapFromTableContent("Pool Stats For Pool Name = DEFAULT");

      if (table.size() == 0 || table.get(CliStrings.DESCRIBE_CLIENT_CQs).size() == 0) {
        return false;
      }

      return table.get(CliStrings.DESCRIBE_CLIENT_CQs).get(0).equals(cqsToWaitFor + "");
    });
  }

  private ClientVM createClient(int vmId, boolean subscriptionEnabled) throws Exception {
    int server1Port = server1Vm1.getPort();
    SerializableConsumerIF<ClientCacheFactory> cacheSetup = cf -> {
      cf.addPoolServer("localhost", server1Port);
      cf.setPoolSubscriptionEnabled(subscriptionEnabled);
      cf.setPoolPingInterval(100);
      cf.setPoolStatisticInterval(100);
      cf.setPoolSubscriptionRedundancy(1);
      cf.setPoolMinConnections(1);
      // TODO: Remove this once GEODE-5157 is fixed
      SocketCreator.use_client_host_name = false;
    };

    Properties clientProps = new Properties();
    clientProps.setProperty("statistic-archive-file", "client.gfs");
    clientProps.setProperty("statistic-sampling-enabled", "true");
    ClientVM vm = rule.startClientVM(vmId, clientProps, cacheSetup);

    vm.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      ClientRegionFactory crf = cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      crf.setPoolName(cache.getDefaultPool().getName());

      Region region = crf.create(STOCKS_REGION);
      region.put("k1", "v1");
      region.put("k2", "v2");
    });

    return vm;
  }

  private void setupCqsOnVM(ClientVM vm, String regionName, String cq1, String cq2, String cq3) {
    vm.invoke(() -> {
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
}
