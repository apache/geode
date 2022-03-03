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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({GfshTest.class}) // GEODE-1705 GEODE-3404
                            // GEODE-3359
public class DurableClientCommandsDUnitTest {

  private static final String STOCKS_REGION = "stocks";
  private static final String BONDS_REGION = "bonds";
  private static final String CQ1 = "cq1";
  private static final String CQ2 = "cq2";
  private static final String CQ3 = "cq3";
  private static final String CLIENT_NAME = "dc1";
  private static final String CQ_GROUP = "cq-group";

  private MemberVM locator;

  private ClientVM client;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Before
  public void setup() throws Exception {
    locator = lsRule.startLocatorVM(0);

    int locatorPort = locator.getPort();
    lsRule.startServerVM(1,
        thisServer -> thisServer.withRegion(RegionShortcut.REPLICATE, STOCKS_REGION)
            .withProperty("groups", CQ_GROUP)
            .withConnectionToLocator(locatorPort));

    lsRule.startServerVM(2,
        thisServer -> thisServer.withRegion(RegionShortcut.REPLICATE, BONDS_REGION)
            .withConnectionToLocator(locatorPort));

    client = lsRule.startClientVM(3, getClientProps(CLIENT_NAME, "300"), (ccf) -> {
      ccf.setPoolSubscriptionEnabled(true);
      ccf.addPoolLocator("localhost", locatorPort);
    });

    gfsh.connectAndVerify(locator);
  }


  @Test
  public void testListDurableClientCqsForOneGroup() {
    setupCqs();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_DURABLE_CQS);
    csb.addOption(CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, CLIENT_NAME);
    csb.addOption(CliStrings.GROUP, CQ_GROUP);
    String commandString = csb.toString();

    gfsh.executeAndAssertThat(commandString).statusIsSuccess()
        .hasTableSection()
        .hasColumn("CQ Name")
        .containsExactlyInAnyOrder("cq1", "cq2", "cq3");

    closeCq(CQ1);
    closeCq(CQ2);
    closeCq(CQ3);

    csb = new CommandStringBuilder(CliStrings.LIST_DURABLE_CQS);
    csb.addOption(CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, CLIENT_NAME);
    commandString = csb.toString();

    gfsh.executeAndAssertThat(commandString).statusIsSuccess()
        .hasTableSection()
        .hasColumn("Status").containsExactly("IGNORED", "IGNORED")
        .hasColumn("CQ Name")
        .containsExactlyInAnyOrder(
            CliStrings.format(CliStrings.LIST_DURABLE_CQS__NO__CQS__FOR__CLIENT, CLIENT_NAME),
            CliStrings.format(CliStrings.LIST_DURABLE_CQS__NO__CQS__REGISTERED));
  }

  @Test
  public void testListDurableClientCqsWhenNoneExist() {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_DURABLE_CQS);
    csb.addOption(CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, CLIENT_NAME);
    String commandString = csb.toString();

    gfsh.executeAndAssertThat(commandString).statusIsSuccess()
        .hasTableSection()
        .hasColumn("Status").containsExactlyInAnyOrder("IGNORED", "IGNORED")
        .hasColumn("CQ Name").containsExactlyInAnyOrder(
            CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, CLIENT_NAME),
            CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, CLIENT_NAME));
  }

  @Test
  public void testListDurableClientCqsWithMixedResults() {
    setupCqs();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_DURABLE_CQS);
    csb.addOption(CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, CLIENT_NAME);
    String commandString = csb.toString();

    gfsh.executeAndAssertThat(commandString).statusIsSuccess()
        .hasTableSection()
        .hasColumn("Status").containsExactlyInAnyOrder("IGNORED", "OK", "OK", "OK")
        .hasColumn("CQ Name")
        .containsExactlyInAnyOrder("cq1", "cq2", "cq3", "No client found with client-id : dc1");

    closeCq(CQ1);
    closeCq(CQ2);
    closeCq(CQ3);
  }

  @Test
  public void testCloseDurableClients() {
    setupCqs();
    closeDurableClient();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CLIENTS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID, CLIENT_NAME);
    csb.addOption(CliStrings.GROUP, CQ_GROUP);
    String commandString = csb.toString();

    await().untilAsserted(() -> {
      gfsh.executeAndAssertThat(commandString).statusIsSuccess()
          .hasTableSection()
          .hasColumn("Message")
          .containsExactlyInAnyOrder("Closed the durable client : \"dc1\".");
    });

    String errorMessage = CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, CLIENT_NAME);
    gfsh.executeAndAssertThat(commandString).statusIsError().containsOutput(errorMessage);
  }

  @Test
  public void testCloseDurableCQ() {
    setupCqs();
    closeDurableClient();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CQS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID, CLIENT_NAME);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__NAME, CQ1);
    String commandString = csb.toString();

    CommandResult result = gfsh.executeCommand(commandString);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getResultData().getTableSections().get(0).getValuesInColumn("Message"))
        .containsExactlyInAnyOrder(
            "Closed the durable cq : \"cq1\" for the durable client : \"dc1\".",
            "No client found with client-id : dc1");

    result = gfsh.executeCommand(commandString);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testCountSubscriptionQueueSize() {
    setupCqs();

    doPuts(STOCKS_REGION, Host.getHost(0).getVM(1));

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    csb.addOption(CliStrings.GROUP, CQ_GROUP);
    String commandString = csb.toString();

    gfsh.executeAndAssertThat(commandString).statusIsSuccess()
        .hasTableSection()
        .hasColumn("Queue Size")
        .containsExactlyInAnyOrder("4");

    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME, CQ3);
    csb.addOption(CliStrings.GROUP, CQ_GROUP);
    commandString = csb.toString();

    gfsh.executeAndAssertThat(commandString).statusIsSuccess()
        .hasTableSection()
        .hasColumn("Queue Size")
        .containsExactlyInAnyOrder("0");

    // CLOSE all the cqs
    closeCq(CQ1);
    closeCq(CQ2);
    closeCq(CQ3);

    // Run the commands again
    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME, CQ1);
    commandString = csb.toString();

    String errorMessage = CliStrings
        .format(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE_CQ_NOT_FOUND, CLIENT_NAME, CQ1);
    gfsh.executeAndAssertThat(commandString).statusIsError().containsOutput(errorMessage);

    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    commandString = csb.toString();

    gfsh.executeAndAssertThat(commandString).statusIsSuccess();

    // Disconnect the client
    closeDurableClient();

    // Close the client
    csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CLIENTS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID, CLIENT_NAME);
    String commandString1 = csb.toString();

    await().untilAsserted(() -> {
      gfsh.executeAndAssertThat(commandString1).statusIsSuccess();
    });

    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    commandString = csb.toString();

    errorMessage = CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, CLIENT_NAME);

    gfsh.executeAndAssertThat(commandString).statusIsError().containsOutput(errorMessage);
  }

  /**
   * Close the cq from the client-side
   *
   * @param cqName , Name of the cq which is to be close.
   */
  private void closeCq(final String cqName) {
    client.invoke(() -> {
      QueryService qs =
          InternalDistributedSystem.getConnectedInstance().getCache().getQueryService();

      try {
        qs.getCq(cqName).close();
      } catch (CqException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void setupCqs() {
    int locatorPort = locator.getPort();
    client.invoke(() -> {
      PoolFactory poolFactory = PoolManager.createFactory().setServerGroup(CQ_GROUP);
      poolFactory.addLocator("localhost", locatorPort);
      poolFactory.setSubscriptionEnabled(true);
      Pool pool = poolFactory.create("DEFAULT");

      QueryService qs = pool.getQueryService();
      CqAttributesFactory cqAf = new CqAttributesFactory();

      try {
        qs.newCq(CQ1, "select * from " + SEPARATOR + STOCKS_REGION, cqAf.create(), true).execute();
        qs.newCq(CQ2, "select * from " + SEPARATOR + STOCKS_REGION + " where id = 1", cqAf.create(),
            true)
            .execute();
        qs.newCq(CQ3, "select * from " + SEPARATOR + STOCKS_REGION + " where id > 2", cqAf.create(),
            true)
            .execute();
      } catch (CqException | CqExistsException | RegionNotFoundException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Does few puts on the region on the server
   */
  private void doPuts(final String regionName, final VM serverVM) {
    serverVM.invoke(() -> {
      InternalCache cache = InternalDistributedSystem.getConnectedInstance().getCache();
      Region region = cache.getRegion(regionName);

      Portfolio p1 = new Portfolio();
      p1.ID = 1;
      p1.names = new String[] {"AAPL", "VMW"};

      Portfolio p2 = new Portfolio();
      p2.ID = 2;
      p2.names = new String[] {"EMC", "IBM"};

      Portfolio p3 = new Portfolio();
      p3.ID = 5;
      p3.names = new String[] {"DOW", "TON"};

      Portfolio p4 = new Portfolio();
      p4.ID = 5;
      p4.names = new String[] {"ABC", "EBAY"};

      region.put("p1", p1);
      region.put("p2", p2);
      region.put("p3", p3);
      region.put("p4", p4);
    });
  }

  private void closeDurableClient() {
    client.invoke(() -> ClientCacheFactory.getAnyInstance().close(true));
  }

  protected Properties getClientProps(String durableClientId, String durableClientTimeout) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(DURABLE_CLIENT_ID, durableClientId);
    config.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(durableClientTimeout));
    return config;
  }

}
