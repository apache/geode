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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(WanTest.class)
public class AlterRegionCommandWithRemoteLocator {
  private static MemberVM locator1;
  private static MemberVM locator2;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;
  private static MemberVM server4;
  private static MemberVM server5;
  private static MemberVM server6;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TemporaryFolder temporaryFolder2 = new TemporaryFolder();

  IgnoredException expectedEx;

  @Before
  public void before() throws Exception {
    expectedEx =
        IgnoredException.addIgnoredException(DiskAccessException.class.getName());
    Properties prop = new Properties();
    prop.setProperty("distributed-system-id", "1");
    prop.setProperty("mcast-port", "0");
    locator1 = lsRule.startLocatorVM(0, prop);
    int port = locator1.getPort();
    Properties prop2 = new Properties();
    prop2.setProperty("distributed-system-id", "2");
    prop2.setProperty("mcast-port", "0");
    prop2.setProperty("remote-locators", "localhost[" + String.valueOf(port) + "]");
    locator2 = lsRule.startLocatorVM(1, prop2);
    gfsh.connectAndVerify(locator2);
    gfsh.execute("configure pdx --auto-serializable-classes=Trade --read-serialized=true");
    int locator2Port = locator2.getPort();
    Properties serverProp = new Properties();
    serverProp.setProperty("off-heap-memory-size", "5m");
    server1 = lsRule.startServerVM(2, server -> server.withConnectionToLocator(locator2Port)
        .withSystemProperty("gemfire.preAllocateDisk", "false").withProperties(serverProp));
    server2 = lsRule.startServerVM(3, server -> server.withConnectionToLocator(locator2Port)
        .withSystemProperty("gemfire.preAllocateDisk", "false").withProperties(serverProp));
    server3 = lsRule.startServerVM(4, server -> server.withConnectionToLocator(locator2Port)
        .withSystemProperty("gemfire.preAllocateDisk", "false").withProperties(serverProp));
    server4 = lsRule.startServerVM(5, server -> server.withConnectionToLocator(locator2Port)
        .withSystemProperty("gemfire.preAllocateDisk", "false").withProperties(serverProp));
    server5 = lsRule.startServerVM(6, server -> server.withConnectionToLocator(locator2Port)
        .withSystemProperty("gemfire.preAllocateDisk", "false").withProperties(serverProp));
    server6 = lsRule.startServerVM(7, server -> server.withConnectionToLocator(locator2Port)
        .withSystemProperty("gemfire.preAllocateDisk", "false").withProperties(serverProp));
  }

  @After
  public void cleanup() throws Exception {
    gfsh.connectAndVerify(locator2);
    gfsh.execute("destroy region --name=/Positions");
    gfsh.execute("destroy region --name=/RealTimePositions");
    gfsh.execute("destroy region --name=/Transactions");
    gfsh.execute("destroy region --name=/Accounts");
    gfsh.execute("destroy region --name=/RealTimeTransactions");
    gfsh.execute("destroy region --name=/AccountBalances");
    gfsh.execute("destroy region --name=/FxRates");
    gfsh.execute("destroy region --name=/AssetClasses");
    gfsh.execute("destroy region --name=/MarketPrices");
    gfsh.execute("destroy region --name=/Currency");
    gfsh.execute("destroy region --name=/Securities");
    gfsh.execute("destroy region --name=/SecurityCrossReferences");
    gfsh.execute("destroy region --name=/Visibility");

    gfsh.execute("destroy gateway-sender --id=serialSender1");
    gfsh.execute("destroy gateway-sender --id=serialSender2");
    gfsh.execute("destroy gateway-sender --id=parallelPositions");
    gfsh.execute("destroy gateway-sender --id=parallelTransactions");
    gfsh.execute("destroy gateway-sender --id=parallelAccountBalances");
    gfsh.execute("destroy gateway-sender --id=parallelRealTimePositions");
    gfsh.execute("destroy gateway-sender --id=parallelRealTimeTransactions");
    gfsh.execute("destroy gateway-sender --id=parallelAccounts");

    gfsh.execute("destroy disk-store --name=DEFAULT");
    gfsh.execute("destroy disk-store --name=gateway_store");
    expectedEx.remove();
  }

  @Test
  public void whenAlteringMultipleRegionWithAlterCommandToAddGatewaySendersThenItShouldReturnSuccess()
      throws Exception {
    gfsh.connectAndVerify(locator2);
    gfsh.execute(
        "create region --name=/Positions --redundant-copies=2 --type=PARTITION_PERSISTENT --off-heap=true");
    gfsh.execute(
        "create region --name=/RealTimePositions --redundant-copies=2 --type=PARTITION_PERSISTENT --off-heap=true");
    gfsh.execute(
        "create region --name=/Transactions --redundant-copies=2 --type=PARTITION_PERSISTENT --off-heap=true");
    gfsh.execute(
        "create region --name=/Accounts --redundant-copies=2 --type=PARTITION_PERSISTENT --off-heap=true");
    gfsh.execute(
        "create region --name=/RealTimeTransactions --redundant-copies=2 --type=PARTITION_PERSISTENT --off-heap=true");
    gfsh.execute(
        "create region --name=/AccountBalances --redundant-copies=2 --type=PARTITION_PERSISTENT --off-heap=true");
    gfsh.execute("create region --name=/FxRates --type=REPLICATE_PERSISTENT --off-heap=true");
    gfsh.execute("create region --name=/AssetClasses --type=REPLICATE_PERSISTENT --off-heap=true");
    gfsh.execute("create region --name=/MarketPrices --type=REPLICATE_PERSISTENT --off-heap=true");
    gfsh.execute("create region --name=/Currency --type=REPLICATE_PERSISTENT --off-heap=true");
    gfsh.execute("create region --name=/Securities --type=REPLICATE_PERSISTENT --off-heap=true");
    gfsh.execute(
        "create region --name=/SecurityCrossReferences --type=REPLICATE_PERSISTENT --off-heap=true");
    gfsh.execute(
        "create region --name=/Visibility --type=REPLICATE --enable-statistics=true  --entry-time-to-live-expiration-action=destroy --entry-time-to-live-expiration=300");

    gfsh.execute("create disk-store --name=gateway_store --dir=gateway_store");
    gfsh.execute(
        "create gateway-sender --id=serialSender1 --remote-distributed-system-id=1 --parallel=false --enable-persistence=true --disk-store-name=gateway_store");
    gfsh.execute(
        "create gateway-sender --id=serialSender2 --remote-distributed-system-id=1 --parallel=false --enable-persistence=true --disk-store-name=gateway_store");
    gfsh.execute(
        "create gateway-sender --id=parallelPositions --remote-distributed-system-id=1 --parallel=true --enable-persistence=true --disk-store-name=gateway_store");
    gfsh.execute(
        "create gateway-sender --id=parallelTransactions --remote-distributed-system-id=1 --parallel=true --enable-persistence=true --disk-store-name=gateway_store");
    gfsh.execute(
        "create gateway-sender --id=parallelAccountBalances --remote-distributed-system-id=1 --parallel=true --enable-persistence=true --disk-store-name=gateway_store");
    gfsh.execute(
        "create gateway-sender --id=parallelRealTimePositions --remote-distributed-system-id=1 --parallel=true --enable-persistence=true --disk-store-name=gateway_store");
    gfsh.execute(
        "create gateway-sender --id=parallelRealTimeTransactions --remote-distributed-system-id=1 --parallel=true --enable-persistence=true --disk-store-name=gateway_store");
    gfsh.execute(
        "create gateway-sender --id=parallelAccounts --remote-distributed-system-id=1 --parallel=true --enable-persistence=true --disk-store-name=gateway_store");
    GeodeAwaitility.await().atMost(2, TimeUnit.MINUTES).until(() -> {
      gfsh.execute("alter region --name=Positions --gateway-sender-id=parallelPositions");
      gfsh.execute("alter region --name=Positions --gateway-sender-id=parallelPositions");
      gfsh.execute(
          "alter region --name=RealTimePositions --gateway-sender-id=parallelRealTimePositions");
      gfsh.execute("alter region --name=Transactions --gateway-sender-id=parallelTransactions");
      gfsh.execute(
          "alter region --name=RealTimeTransactions --gateway-sender-id=parallelRealTimeTransactions");
      gfsh.execute(
          "alter region --name=AccountBalances --gateway-sender-id=parallelAccountBalances");
      gfsh.execute("alter region --name=Accounts --gateway-sender-id=parallelAccounts");
      gfsh.execute("alter region --name=FxRates --gateway-sender-id=serialSender1,serialSender2");
      gfsh.execute(
          "alter region --name=AssetClasses --gateway-sender-id=serialSender1,serialSender2");
      gfsh.execute(
          "alter region --name=MarketPrices --gateway-sender-id=serialSender1,serialSender2");
      gfsh.execute("alter region --name=Currency --gateway-sender-id=serialSender1,serialSender2");
      gfsh.execute(
          "alter region --name=Securities --gateway-sender-id=serialSender1,serialSender2");
      gfsh.execute(
          "alter region --name=SecurityCrossReferences --gateway-sender-id=serialSender1,serialSender2");
      return true;
    });


  }

}
