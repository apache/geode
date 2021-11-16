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
package org.apache.geode.management.internal.configuration;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
@SuppressWarnings("serial")
public class WANClusterConfigurationDUnitTest {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static final String REPLICATE_REGION = "ReplicateRegion1";

  private MemberVM locator;
  private MemberVM dataMember;

  @Before
  public void before() throws Exception {
    locator = clusterStartupRule.startLocatorVM(20);
  }

  @Test
  public void whenAlteringNoncolocatedRegionsWithTheSameParallelSenderIdThenFailure()
      throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("cannot have the same parallel gateway sender id");

    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    MemberVM server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ny");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test2");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test2");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny");
    gfsh.executeAndAssertThat(csb.toString()).statusIsError();
  }

  @Test
  public void whenAlteringOneRegionsWithDifferentParallelSenderIdThenSuccess() throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("cannot have the same parallel gateway sender id");

    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    MemberVM server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ny");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny,ln");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  @Test
  public void whenAlteringOneRegionsWithDifferentParallelSenderIdsAfterItWasAlreadySetWithOneOfTheNewSenderThenSuccess()
      throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("cannot have the same parallel gateway sender id");

    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    MemberVM server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ny");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny,ln");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }


  @Test
  public void whenAlteringOneRegionsWithDifferentParallelSenderIdAfterItWasAlreadyCreatedWithDifferentSenderThenSuccess()
      throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("cannot have the same parallel gateway sender id");
    addIgnoredException("Could not execute \"list gateways\"");
    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    MemberVM server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ny");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    waitTillAllGatewaySendersAreReady();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, "ny");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ln");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void waitTillAllGatewaySendersAreReady() {
    await().untilAsserted(() -> {
      gfsh.executeAndAssertThat(CliStrings.LIST_GATEWAY).statusIsSuccess()
          .hasTableSection("gatewaySenders").hasRowSize(4);
    });
  }


  @Test
  public void whenAlteringOneRegionsWithDifferentParallelSenderIdAfterItWasSetWithOneSenderThenSuccess()
      throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("cannot have the same parallel gateway sender id");

    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    MemberVM server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ny");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ln");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  @Test
  public void whenAlteringNoncolocatedRegionsWithTheSameSerialSenderIdThenSuccess()
      throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("cannot have the same parallel gateway sender id");

    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    MemberVM server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ny");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test2");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test2");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  @Test
  public void whenAlteringNoncolocatedRegionsWithDifferentSerialGatewayIDThenSuccess()
      throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("cannot have the same parallel gateway sender id");

    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    MemberVM server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ny");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test2");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test2");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ln");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }


  @Test
  public void whenAlteringNoncolocatedRegionsWithDifferentParallelGatewayIDThenSuccess()
      throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("cannot have the same parallel gateway sender id");

    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    MemberVM server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ny");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test2");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test2");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ln");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  @Test
  public void whenAlteringColocatedRegionsWithSameParallelGatewayIDThenSuccess() throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("cannot have the same parallel gateway sender id");

    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    MemberVM server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ny");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test2");
    csb.addOption(CliStrings.CREATE_REGION__COLOCATEDWITH, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
    csb.addOption(CliStrings.ALTER_REGION__REGION, "test2");
    csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }


  @Test
  public void testCreateGatewaySenderReceiver() throws Exception {
    addIgnoredException("could not get remote locator");

    final String gsId = "GatewaySender1";
    final String batchSize = "1000";
    final String dispatcherThreads = "5";
    final String enableConflation = "false";
    final String manualStart = "false";
    final String alertThreshold = "1000";
    final String batchTimeInterval = "20";
    final String maxQueueMemory = "100";
    final String orderPolicy = GatewaySender.OrderPolicy.KEY.toString();
    final String parallel = "true";
    final String rmDsId = "250";
    final String socketBufferSize =
        String.valueOf(GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000);
    final String socketReadTimeout =
        String.valueOf(GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 200);

    dataMember = clusterStartupRule.startServerVM(1, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    // create GatewayReceiver
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYRECEIVER);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART, "true");
    csb.addOption(CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT, "10000");
    csb.addOption(CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT, "20000");
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS, "20");
    csb.addOption(CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS, "myLocalHost");
    gfsh.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    // create GatewaySender
    csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ID, gsId);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE, batchSize);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD, alertThreshold);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL,
        batchTimeInterval);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS,
        dispatcherThreads);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION,
        enableConflation);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, manualStart);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY, maxQueueMemory);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY, orderPolicy);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, parallel);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, rmDsId);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE,
        socketBufferSize);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT,
        socketReadTimeout);

    gfsh.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    // Start a new member which receives the shared configuration
    // Verify the config creation on this member

    MemberVM newMember = clusterStartupRule.startServerVM(2, locator.getPort());

    // verify GatewayReceiver attributes saved in cluster config
    newMember.invoke(() -> {
      Set<GatewayReceiver> gatewayReceivers = ClusterStartupRule.getCache().getGatewayReceivers();
      assertNotNull(gatewayReceivers);
      assertFalse(gatewayReceivers.isEmpty());
      assertTrue(gatewayReceivers.size() == 1);
      for (GatewayReceiver gr : gatewayReceivers) {
        assertThat(gr.isManualStart()).isTrue();
        assertThat(gr.getStartPort()).isEqualTo(10000);
        assertThat(gr.getEndPort()).isEqualTo(20000);
        assertThat(gr.getMaximumTimeBetweenPings()).isEqualTo(20);
        assertThat(gr.getHostnameForSenders()).isEqualTo("myLocalHost");
      }
    });

    // verify GatewaySender attributes saved in cluster config
    newMember.invoke(() -> {
      GatewaySender gs = ClusterStartupRule.getCache().getGatewaySender(gsId);
      assertNotNull(gs);
      assertTrue(alertThreshold.equals(Integer.toString(gs.getAlertThreshold())));
      assertTrue(batchSize.equals(Integer.toString(gs.getBatchSize())));
      assertTrue(dispatcherThreads.equals(Integer.toString(gs.getDispatcherThreads())));
      assertTrue(enableConflation.equals(Boolean.toString(gs.isBatchConflationEnabled())));
      assertTrue(manualStart.equals(Boolean.toString(gs.isManualStart())));
      assertTrue(alertThreshold.equals(Integer.toString(gs.getAlertThreshold())));
      assertTrue(batchTimeInterval.equals(Integer.toString(gs.getBatchTimeInterval())));
      assertTrue(maxQueueMemory.equals(Integer.toString(gs.getMaximumQueueMemory())));
      assertTrue(orderPolicy.equals(gs.getOrderPolicy().toString()));
      assertTrue(parallel.equals(Boolean.toString(gs.isParallel())));
      assertTrue(rmDsId.equals(Integer.toString(gs.getRemoteDSId())));
      assertTrue(socketBufferSize.equals(Integer.toString(gs.getSocketBufferSize())));
      assertTrue(socketReadTimeout.equals(Integer.toString(gs.getSocketReadTimeout())));
    });
  }

  // toberal
  // @Test
  // public void
  // whenAlteringOnePartitionedPersistentReplicatedRegionWithParallelSenderWithAHighNumberOfServersCommandDoesNotHang()
  // throws Exception {
  // // toberal.
  // // With 24 servers I get errors due to lack of resources (lost heartbeats). Not able to run
  // // With 16 servers. It hangs (eventually finishes)
  // // with 3 sleeps of 10 it finished after 6 minutes and a half showing the "having elapsed"
  // // log.
  // // with 3 sleeps of 30 it finished after 15 minutes and a half, 9 and a half second time,
  // // showing the "having elapsed" log. Time taken by the alter command: 272 seconds
  // // Without sleeps it finished in 6 minutes. Max stuck of 153 seconds. Time taken for the
  // // command: 153 seconds
  // // With 20 servers. It hangs (eventually finishes).
  // // stuck for 660 seconds
  // // time taken by command: 694 seconds
  // // With 8 servers it does not hang. (I do not see 15 secs have elapsed waiting for a primary
  // for
  // // bucket messages) or thread stuck messages
  // // If I do not create first the gateway sender and what I do is:
  // // - create region
  // // - allocate buckets
  // // - create gateway sender
  // // - alter region with gateway sender
  // // it does not hang.
  // // With 1013 bucketes the problem does not appear. Probably because recover buckets launches
  // // more threads and allows more time to iscolocated to be true
  // addIgnoredException("could not get remote locator");
  // addIgnoredException("cannot have the same parallel gateway sender id");
  //
  // MemberVM locator = clusterStartupRule.startLocatorVM(0);
  //
  // int serversNo = 16;
  // MemberVM serversArray[] = new MemberVM[serversNo];
  //
  // // setup servers in Site #1
  // int initialIndex = 1;
  // Properties properties = new Properties();
  // for (int index = 0; index < serversArray.length; index++) {
  // serversArray[index] =
  // clusterStartupRule.startServerVM(initialIndex + index, properties, locator.getPort());
  // }
  //
  // // Connect Gfsh to locator.
  // gfsh.connectAndVerify(locator);
  //
  // // Create disk-store
  // CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
  // csb.addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore");
  // csb.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir");
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  // // Create disk-store1
  // // csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
  // // csb.addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore1");
  // // csb.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir");
  // // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  // // Create region
  // csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
  // csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
  // csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_PERSISTENT");
  // csb.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, "1");
  // csb.addOption(CliStrings.CREATE_REGION__TOTALNUMBUCKETS, "" + 1031);
  // // With this delay, test cases pass even tiwh 16 and 20 servers
  // // csb.addOption(CliStrings.CREATE_REGION__STARTUPRECOVERYDDELAY, "1000");
  // // The region must have a disk store (the same or another) to see the hang. If it is different,
  // // the hanging lasts longer
  // csb.addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore");
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  // // Allocate buckets
  // String queryString = "\"select * from /test1\"";
  // csb = new CommandStringBuilder(CliStrings.QUERY);
  // csb.addOption(QUERY, queryString).getCommandString();
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  // System.out.println("toberal before create gateway sender again");
  //
  // // Create gateway sender
  // csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
  // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ny");
  // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
  // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
  // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__DISKSTORENAME, "diskStore");
  // // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__DISKSTORENAME, "diskStore1");
  // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE, "true");
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  // // Thread.sleep(30000);
  // System.out.println("toberal before altering region to add gateway sender");
  //
  // long currentMillis = System.currentTimeMillis();
  // // Add gateway sender to region
  // csb = new CommandStringBuilder(CliStrings.ALTER_REGION);
  // csb.addOption(CliStrings.ALTER_REGION__REGION, "test1");
  // csb.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "ny");
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  // long timeTaken = (System.currentTimeMillis() - currentMillis) / 1000;
  //
  // int entries = 10;
  // for (int i = 0; i < entries; i++) {
  // csb = new CommandStringBuilder(CliStrings.PUT);
  // csb.addOption(CliStrings.PUT__REGIONNAME, "test1");
  // csb.addOption(CliStrings.PUT__KEY, "" + i);
  // csb.addOption(CliStrings.PUT__VALUE, "" + i);
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  // }
  //
  // int queuedEvents = 0;
  // for (MemberVM server : serversArray) {
  // queuedEvents += server.getVM().invoke(() -> getSenderStats("ny", -1)).get(2);
  // }
  //
  // System.out.println("toberal queuedEvents: " + queuedEvents);
  //
  // System.out.println("toberal time taken by alter region: "
  // + (System.currentTimeMillis() - currentMillis) / 1000 + " seconds");
  // assertThat(timeTaken).isLessThan(15);
  // }
  //
  // @Test
  // public void whenStartGatewaySenderWithCleanQueuesAndAHighNumberOfServersCommandDoesNotHang()
  //
  // throws Exception {
  // addIgnoredException("could not get remote locator");
  // addIgnoredException("cannot have the same parallel gateway sender id");
  //
  // MemberVM locator = clusterStartupRule.startLocatorVM(0);
  //
  // int serversNo = 16;
  // MemberVM serversArray[] = new MemberVM[serversNo];
  //
  // // setup servers in Site #1
  // int initialIndex = 1;
  // Properties properties = new Properties();
  // for (int index = 0; index < serversArray.length; index++) {
  // serversArray[index] =
  // clusterStartupRule.startServerVM(initialIndex + index, properties, locator.getPort());
  // }
  //
  // // Connect Gfsh to locator.
  // gfsh.connectAndVerify(locator);
  //
  // // Create disk-store
  // CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
  // csb.addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore");
  // csb.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir");
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  // // Create disk-store1
  // // csb = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
  // // csb.addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore1");
  // // csb.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir");
  // // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  // System.out.println("toberal before create gateway sender");
  //
  // // Create gateway sender
  // csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER);
  // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ny");
  // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true");
  // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2");
  // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__DISKSTORENAME, "diskStore");
  // // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__DISKSTORENAME, "diskStore1");
  // csb.addOption(CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE, "true");
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  //
  // // Create region
  // csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
  // csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
  // csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_PERSISTENT");
  // csb.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, "1");
  // csb.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, "ny");
  // csb.addOption(CliStrings.CREATE_REGION__TOTALNUMBUCKETS, "" + serversNo * 8);
  // // With this delay, test cases pass even tiwh 16 and 20 servers
  // // csb.addOption(CliStrings.CREATE_REGION__STARTUPRECOVERYDDELAY, "1000");
  // // The region must have a disk store (the same or another) to see the hang. If it is different,
  // // the hanging lasts longer
  // csb.addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore");
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  // // Allocate buckets
  // String queryString = "\"select * from /test1\"";
  // csb = new CommandStringBuilder(CliStrings.QUERY);
  // csb.addOption(QUERY, queryString).getCommandString();
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  //
  // System.out.println("toberal before stopping gateway sender");
  //
  // csb = new CommandStringBuilder(CliStrings.STOP_GATEWAYSENDER);
  // csb.addOption(CliStrings.STOP_GATEWAYSENDER__ID, "ny");
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  // // Wait for 2 seconds because beans are updated every 2 secs and it might
  // // be that some server thinks that the sender is still running and
  // // not start it and then we could have a hang.
  // // toberal: Remove temporarily to see it fail
  // // Thread.sleep(2000);
  //
  // System.out.println("toberal before starting gateway sender with clean queues");
  //
  // long currentMillis = System.currentTimeMillis();
  // // Add gateway sender to region
  // csb = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER);
  // csb.addOption(CliStrings.START_GATEWAYSENDER__ID, "ny");
  // csb.addOption(CliStrings.START_GATEWAYSENDER__CLEAN_QUEUE);
  // gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  //
  // long timeTaken = (System.currentTimeMillis() - currentMillis) / 1000;
  //
  // System.out.println(
  // "toberal time taken by alter region assuming that cleanQueues is waiting for buckets to be
  // created: "
  // + (System.currentTimeMillis() - currentMillis) / 1000 + " seconds");
  // assertThat(timeTaken).isLessThan(15);
  //
  // // Thread.sleep(300000);
  // //
  // // System.out.println("toberal after sleep: "
  // // + (System.currentTimeMillis() - currentMillis) / 1000 + " seconds");
  //
  // }
  //
  //
  // public static List<Integer> getSenderStats(String senderId, int expectedQueueSize) {
  // Cache cache = InternalDistributedSystem.getAnyInstance().getCache();
  // AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
  // GatewaySenderStats statistics = sender.getStatistics();
  // if (expectedQueueSize != -1) {
  // final RegionQueue regionQueue;
  // regionQueue = sender.getQueues().toArray(new RegionQueue[1])[0];
  // if (sender.isParallel()) {
  // ConcurrentParallelGatewaySenderQueue parallelGatewaySenderQueue =
  // (ConcurrentParallelGatewaySenderQueue) regionQueue;
  // PartitionedRegion pr =
  // parallelGatewaySenderQueue.getRegions().toArray(new PartitionedRegion[1])[0];
  // }
  // await()
  // .untilAsserted(() -> assertThat(regionQueue.size()).isEqualTo(expectedQueueSize));
  // }
  // ArrayList<Integer> stats = new ArrayList<Integer>();
  // stats.add(statistics.getEventQueueSize());
  // stats.add(statistics.getEventsReceived());
  // stats.add(statistics.getEventsQueued());
  // stats.add(statistics.getEventsDistributed());
  // stats.add(statistics.getBatchesDistributed());
  // stats.add(statistics.getBatchesRedistributed());
  // stats.add(statistics.getEventsFiltered());
  // stats.add(statistics.getEventsNotQueuedConflated());
  // stats.add(statistics.getEventsConflatedFromBatches());
  // stats.add(statistics.getConflationIndexesMapSize());
  // stats.add(statistics.getSecondaryEventQueueSize());
  // stats.add(statistics.getEventsProcessedByPQRM());
  // stats.add(statistics.getEventsExceedingAlertThreshold());
  // stats.add((int) statistics.getBatchesWithIncompleteTransactions());
  // return stats;
  // }
}
