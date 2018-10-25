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

import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
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
    locator = clusterStartupRule.startLocatorVM(3);
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
      CommandStringBuilder csb2 = new CommandStringBuilder(CliStrings.LIST_GATEWAY);
      CommandResult cmdResult = gfsh.executeCommand(csb2.toString());
      assertThat(cmdResult).isNotNull();
      assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);
      TabularResultData tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders =
          tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertThat(senders).hasSize(4);
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
}
