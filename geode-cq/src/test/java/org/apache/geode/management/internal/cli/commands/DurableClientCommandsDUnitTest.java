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
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.DistributedTestUtils.getDUnitLocatorPort;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.GfshTest;

@Category({DistributedTest.class, FlakyTest.class, GfshTest.class}) // See GEODE-3530
@SuppressWarnings("serial")
public class DurableClientCommandsDUnitTest extends CliCommandTestBase {

  private static final String REGION_NAME = "stocks";
  private static final String CQ1 = "cq1";
  private static final String CQ2 = "cq2";
  private static final String CQ3 = "cq3";
  private static final String CLIENT_NAME = "dc1";

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Host.getHost(0).getVM(0).invoke(() -> CacheServerTestUtil.closeCache());
    Host.getHost(0).getVM(1).invoke(() -> CacheServerTestUtil.closeCache());
    Host.getHost(0).getVM(2).invoke(() -> CacheServerTestUtil.closeCache());
  }

  @Test
  public void testListDurableClientCqs() throws Exception {
    setupSystem();
    setupCqs();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_DURABLE_CQS);
    csb.addOption(CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, CLIENT_NAME);
    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));
    assertTrue(resultAsString.contains(CQ1));
    assertTrue(resultAsString.contains(CQ2));
    assertTrue(resultAsString.contains(CQ3));

    closeCq(CQ1);
    closeCq(CQ2);
    closeCq(CQ3);

    csb = new CommandStringBuilder(CliStrings.LIST_DURABLE_CQS);
    csb.addOption(CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, CLIENT_NAME);
    commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.ERROR.equals(commandResult.getStatus()));
    String errorMessage =
        CliStrings.format(CliStrings.LIST_DURABLE_CQS__NO__CQS__FOR__CLIENT, CLIENT_NAME);
    assertTrue(resultAsString.contains(errorMessage));
  }

  @Test
  public void testCloseDurableClients() throws Exception {
    setupSystem();
    setupCqs();
    closeDurableClient();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CLIENTS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID, CLIENT_NAME);
    String commandString = csb.toString();

    long giveUpTime = System.currentTimeMillis() + 20000;
    CommandResult commandResult = null;
    String resultAsString = null;

    do {
      writeToLog("Command String : ", commandString);
      commandResult = executeCommand(commandString);
      resultAsString = commandResultToString(commandResult);
    } while (resultAsString.contains("Cannot close a running durable client")
        && giveUpTime > System.currentTimeMillis());

    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));

    // Execute again to see the error condition
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.ERROR.equals(commandResult.getStatus()));
    String errorMessage = CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, CLIENT_NAME);
    assertTrue(resultAsString.contains(errorMessage));
  }

  @Test
  public void testCloseDurableCQ() throws Exception {
    setupSystem();
    setupCqs();

    closeDurableClient();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CQS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID, CLIENT_NAME);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__NAME, CQ1);
    String commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));

    csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CQS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID, CLIENT_NAME);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__NAME, CQ1);
    commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result : ", resultAsString);
    assertTrue(Status.ERROR.equals(commandResult.getStatus()));

  }

  @Test
  public void testCountSubscriptionQueueSize() throws Exception {
    setupSystem();
    setupCqs();
    doPuts(REGION_NAME, Host.getHost(0).getVM(1));

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    String commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));
    assertTrue(resultAsString.contains("4"));

    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME, CQ3);
    commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));

    // CLOSE all the cqs
    closeCq(CQ1);
    closeCq(CQ2);
    closeCq(CQ3);

    // Run the commands again
    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME, CQ1);
    commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.ERROR.equals(commandResult.getStatus()));
    String errorMessage = CliStrings
        .format(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE_CQ_NOT_FOUND, CLIENT_NAME, CQ1);
    assertTrue(resultAsString.contains(errorMessage));

    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));

    // Disconnect the client
    closeDurableClient();

    // Close the client
    csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CLIENTS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID, CLIENT_NAME);
    commandString = csb.toString();

    // since it can take the server a bit to know that the client has disconnected
    // we loop here
    long giveUpTime = System.currentTimeMillis() + 20000;
    do {
      writeToLog("Command String : ", commandString);
      commandResult = executeCommand(commandString);
      resultAsString = commandResultToString(commandResult);
    } while (resultAsString.contains("Cannot close a running durable client")
        && giveUpTime > System.currentTimeMillis());

    writeToLog("Command Result :\n", resultAsString);
    assertTrue("failed executing" + commandString + "; result = " + resultAsString,
        Status.OK.equals(commandResult.getStatus()));

    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, CLIENT_NAME);
    commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.ERROR.equals(commandResult.getStatus()));
    assertTrue(resultAsString
        .contains(CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, CLIENT_NAME)));
  }

  private void writeToLog(String text, String resultAsString) {
    getLogWriter().info(getUniqueName() + ": " + text + "\n" + resultAsString);
  }

  private void setupSystem() throws Exception {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(getServerProperties());

    VM manager = Host.getHost(0).getVM(0);
    VM server1 = Host.getHost(0).getVM(1);
    VM client1 = Host.getHost(0).getVM(2);

    int listeningPort = startCacheServer(server1, 0, false, REGION_NAME);
    startDurableClient(client1, server1, listeningPort, CLIENT_NAME, "300");
  }

  /**
   * Close the cq from the client-side
   *
   * @param cqName , Name of the cq which is to be close.
   */
  private void closeCq(final String cqName) {
    VM vm2 = Host.getHost(0).getVM(2);
    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        QueryService qs = getCache().getQueryService();
        CqAttributesFactory cqAf = new CqAttributesFactory();

        try {
          qs.getCq(cqName).close();
        } catch (CqException e) {
          throw new RuntimeException(e);
        }

        return true;
      }
    });
  }

  private void setupCqs() {
    VM vm2 = Host.getHost(0).getVM(2);

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        QueryService qs = getCache().getQueryService();
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

        return true;
      }
    });
  }

  private int startCacheServer(final VM serverVM, final int port, final boolean createPR,
      final String regionName) {
    return serverVM.invoke(() -> {
      getSystem(getServerProperties());

      InternalCache cache = getCache();
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

      CacheServer cacheServer = getCache().addCacheServer();
      cacheServer.setPort(port);
      cacheServer.start();

      return cacheServer.getPort();
    });
  }

  private void startDurableClient(final VM clientVM, final VM serverVM, final int port,
      final String durableClientId, final String durableClientTimeout) {
    clientVM.invoke(new CacheSerializableRunnable("Start client") {
      @Override
      public void run2() throws CacheException {
        Properties props = getClientProps(durableClientId, durableClientTimeout);
        getSystem(props);

        ClientCacheFactory ccf = new ClientCacheFactory(props);
        ccf.addPoolServer(getServerHostName(serverVM.getHost()), port);
        ccf.setPoolSubscriptionEnabled(true);

        ClientCache cache = getClientCache(ccf);
      }
    });
  }

  /**
   * Does few puts on the region on the server
   */
  private void doPuts(final String regionName, final VM serverVM) {
    serverVM.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        InternalCache cache = getCache();
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
      }
    });
  }

  // Closes the durable-client from the client side.
  private void closeDurableClient() {
    VM clientVM = Host.getHost(0).getVM(2);

    clientVM.invoke(new CacheSerializableRunnable("Stop client") {
      @Override
      public void run2() throws CacheException {
        ClientCacheFactory.getAnyInstance().close(true);
      }
    });
  }

  protected Properties getClientProps(String durableClientId, String durableClientTimeout) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.setProperty(DURABLE_CLIENT_ID, durableClientId);
    config.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(durableClientTimeout));
    return config;
  }

  protected Properties getServerProperties() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "localhost[" + getDUnitLocatorPort() + "]");
    return config;
  }
}
