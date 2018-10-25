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

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({GfshTest.class}) // GEODE-1705 GEODE-3404
                            // GEODE-3359
@SuppressWarnings("serial")
public class DurableClientCommandsDUnitTest {

  private static final String REGION_NAME = "stocks";
  private static final String CQ1 = "cq1";
  private static final String CQ2 = "cq2";
  private static final String CQ3 = "cq3";
  private static final String CLIENT_NAME = "dc1";

  private MemberVM locator, server;

  private ClientVM client;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Before
  public void setup() throws Exception {
    int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "fine");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + jmxPort);
    locator = lsRule.startLocatorVM(0, props);

    int locatorPort = locator.getPort();
    server = lsRule.startServerVM(1,
        thisServer -> thisServer.withRegion(RegionShortcut.REPLICATE, REGION_NAME)
            .withConnectionToLocator(locatorPort));

    client = lsRule.startClientVM(2, getClientProps(CLIENT_NAME, "300"), (ccf) -> {
      ccf.setPoolSubscriptionEnabled(true);
      ccf.addPoolLocator("localhost", locatorPort);
    });

    gfsh.connectAndVerify(locator);
  }


  @Test
  public void testListDurableClientCqs() {
    setupCqs();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_DURABLE_CQS);
    csb.addOption(CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, CLIENT_NAME);
    String commandString = csb.toString();

    gfsh.executeAndAssertThat(commandString).statusIsSuccess().containsOutput(CQ1)
        .containsOutput(CQ2).containsOutput(CQ3);

    closeCq(CQ1);
    closeCq(CQ2);
    closeCq(CQ3);

    csb = new CommandStringBuilder(CliStrings.LIST_DURABLE_CQS);
    csb.addOption(CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, CLIENT_NAME);
    commandString = csb.toString();
    String errorMessage =
        CliStrings.format(CliStrings.LIST_DURABLE_CQS__NO__CQS__FOR__CLIENT, CLIENT_NAME);
    gfsh.executeAndAssertThat(commandString).statusIsError().containsOutput(errorMessage);
  }

  @Test
  public void testCloseDurableClients() {
    setupCqs();
    closeDurableClient();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CLIENTS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID, CLIENT_NAME);
    String commandString = csb.toString();

    await().untilAsserted(() -> {
      gfsh.executeAndAssertThat(commandString).statusIsSuccess();
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

    gfsh.executeAndAssertThat(commandString).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CQS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID, CLIENT_NAME);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__NAME, CQ1);
    commandString = csb.toString();

    gfsh.executeAndAssertThat(commandString).statusIsError();
  }

  @Test
  public void testCountSubscriptionQueueSize() {
    setupCqs();

    doPuts(REGION_NAME, Host.getHost(0).getVM(1));

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    String commandString = csb.toString();

    gfsh.executeAndAssertThat(commandString).statusIsSuccess().containsOutput("4");


    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME, CQ3);
    commandString = csb.toString();

    gfsh.executeAndAssertThat(commandString).statusIsSuccess();

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
    client.invoke(() -> {
      QueryService qs = ClusterStartupRule.getClientCache().getQueryService();
      CqAttributesFactory cqAf = new CqAttributesFactory();

      try {
        qs.newCq(CQ1, "select * from /" + REGION_NAME, cqAf.create(), true).execute();
        qs.newCq(CQ2, "select * from /" + REGION_NAME + " where id = 1", cqAf.create(), true)
            .execute();
        qs.newCq(CQ3, "select * from /" + REGION_NAME + " where id > 2", cqAf.create(), true)
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
