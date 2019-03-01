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


import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ClientCacheRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class DescribeClientCommandAcceptanceTest {

  @Rule
  public GfshRule realGfsh = new GfshRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClientCacheRule client = new ClientCacheRule();

  @Test
  public void describeClient() throws Exception {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int locatorPort = ports[0];
    int serverPort = ports[1];
    GfshScript.of("start locator --name=locator --port=" + locatorPort)
        .and("start server --name=server --server-port=" + serverPort)
        .and("create region --name=stocks --type=REPLICATE")
        .execute(realGfsh);

    ClientCache clientCache = client.withProperty("statistic-archive-file", "client.gfs")
        .withProperty("statistic-sampling-enabled", "true")
        .withCacheSetup(cf -> {
          cf.addPoolServer("localhost", serverPort);
          cf.setPoolSubscriptionEnabled(true);
          cf.setPoolPingInterval(100);
          cf.setPoolStatisticInterval(100);
          cf.setPoolSubscriptionRedundancy(1);
          cf.setPoolMinConnections(1);
        }).createCache();

    Region stocks = client.createProxyRegion("stocks");
    stocks.put("k1", "v1");
    stocks.put("k2", "v2");

    QueryService qs = clientCache.getQueryService();
    CqAttributesFactory cqAf = new CqAttributesFactory();

    qs.newCq("cq1", "select * from /stocks", cqAf.create(), true).execute();
    qs.newCq("cq2", "select * from /stocks where id = 1", cqAf.create(), true)
        .execute();
    qs.newCq("cq3", "select * from /stocks where id > 2", cqAf.create(), true)
        .execute();

    gfsh.connect(locatorPort, GfshCommandRule.PortType.locator);

    waitForClientReady(3);

    validateResults(locatorPort);
  }

  void waitForClientReady(int cqsToWaitFor) {
    // Wait until all CQs are ready
    GeodeAwaitility.await().until(() -> {
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

  private void validateResults(int locatorPort) {
    CommandResult result = gfsh.executeCommand("list members");
    Map<String, List<String>> members = result.getMapFromTableContent("members");
    int server1Idx = members.get("Name").indexOf("server");
    String server1 = members.get("Id").get(server1Idx);

    result = gfsh.executeCommand("list clients");
    String clientId = result.getColumnFromTableContent(CliStrings.LIST_CLIENT_COLUMN_Clients,
        "section1", "TableForClientList").get(0);

    result = gfsh.executeCommand("describe client --clientID=" + clientId);
    Assertions.assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    Map<String, List<String>> table =
        result.getMapFromTableContent("Pool Stats For Pool Name = DEFAULT");
    Map<String, String> data = result.getMapFromSection("InfoSection");

    Assertions.assertThat(table.get(CliStrings.DESCRIBE_CLIENT_MIN_CONN).get(0)).isEqualTo("1");
    Assertions.assertThat(table.get(CliStrings.DESCRIBE_CLIENT_MAX_CONN).get(0)).isEqualTo("-1");
    Assertions.assertThat(table.get(CliStrings.DESCRIBE_CLIENT_REDUNDANCY).get(0)).isEqualTo("1");


    Assertions.assertThat(table.get(CliStrings.DESCRIBE_CLIENT_CQs).get(0)).isEqualTo("3");
    Assertions.assertThat(Integer.parseInt(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_QUEUE_SIZE)))
        .isGreaterThanOrEqualTo(1);
    Assertions.assertThat(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_PRIMARY_SERVERS))
        .isEqualTo(server1);

    Assertions.assertThat(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS)).isEqualTo("2");
    Assertions.assertThat(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTENER_CALLS))
        .isEqualTo("0");
    Assertions.assertThat(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_DURABLE)).isEqualTo("No");
    Assertions.assertThat(Integer.parseInt(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_THREADS)))
        .isGreaterThan(0);
    Assertions.assertThat(Integer.parseInt(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_CPU)))
        .isGreaterThan(0);
    Assertions.assertThat(Integer.parseInt(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_UP_TIME)))
        .isGreaterThanOrEqualTo(0);
    Assertions
        .assertThat(Long.parseLong(data.get(CliStrings.DESCRIBE_CLIENT_COLUMN_PROCESS_CPU_TIME)))
        .isGreaterThan(0);

    // execute the same command using real gfsh
    GfshScript.of("connect --locator=localhost[" + locatorPort + "]")
        .and("list members")
        .and("list clients").and("describe client --clientID=" + clientId)
        .execute(realGfsh);
  }
}
