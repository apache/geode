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

  private MemberVM locator;

  @Before
  public void before() {
    locator = clusterStartupRule.startLocatorVM(3);
  }

  @Test
  public void whenAlteringNoncolocatedRegionsWithTheSameParallelSenderIdThenFailure()
      throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("cannot have the same parallel gateway sender id");

    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    clusterStartupRule.startServerVM(1, locator.getPort());
    clusterStartupRule.startServerVM(2, locator.getPort());

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
    clusterStartupRule.startServerVM(1, locator.getPort());
    clusterStartupRule.startServerVM(2, locator.getPort());

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
    clusterStartupRule.startServerVM(1, locator.getPort());
    clusterStartupRule.startServerVM(2, locator.getPort());

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
    clusterStartupRule.startServerVM(1, locator.getPort());
    clusterStartupRule.startServerVM(2, locator.getPort());

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
    await().untilAsserted(() -> gfsh.executeAndAssertThat(CliStrings.LIST_GATEWAY).statusIsSuccess()
        .hasTableSection("gatewaySenders").hasRowSize(4));
  }


  @Test
  public void whenAlteringOneRegionsWithDifferentParallelSenderIdAfterItWasSetWithOneSenderThenSuccess()
      throws Exception {
    addIgnoredException("could not get remote locator");
    addIgnoredException("cannot have the same parallel gateway sender id");

    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    clusterStartupRule.startServerVM(1, locator.getPort());
    clusterStartupRule.startServerVM(2, locator.getPort());

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
    clusterStartupRule.startServerVM(1, locator.getPort());
    clusterStartupRule.startServerVM(2, locator.getPort());

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
    clusterStartupRule.startServerVM(1, locator.getPort());
    clusterStartupRule.startServerVM(2, locator.getPort());

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
    clusterStartupRule.startServerVM(1, locator.getPort());
    clusterStartupRule.startServerVM(2, locator.getPort());

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
    clusterStartupRule.startServerVM(1, locator.getPort());
    clusterStartupRule.startServerVM(2, locator.getPort());

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
    final int batchSize = 1000;
    final int dispatcherThreads = 5;
    final boolean enableConflation = false;
    final boolean manualStart = false;
    final int alertThreshold = 1000;
    final int batchTimeInterval = 20;
    final int maxQueueMemory = 100;
    final GatewaySender.OrderPolicy orderPolicy = GatewaySender.OrderPolicy.KEY;
    final boolean parallel = true;
    final int rmDsId = 250;
    final int socketBufferSize = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    final int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 200;

    clusterStartupRule.startServerVM(1, locator.getPort());

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
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE,
        String.valueOf(batchSize));
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD,
        String.valueOf(alertThreshold));
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL,
        String.valueOf(batchTimeInterval));
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS,
        String.valueOf(dispatcherThreads));
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION,
        String.valueOf(enableConflation));
    setDeprecatedManualStart(csb, String.valueOf(manualStart));
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY,
        String.valueOf(maxQueueMemory));
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY,
        String.valueOf(orderPolicy));
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__PARALLEL,
        String.valueOf(parallel));
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID,
        String.valueOf(rmDsId));
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE,
        String.valueOf(socketBufferSize));
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT,
        String.valueOf(socketReadTimeout));

    gfsh.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    // Start a new member which receives the shared configuration
    // Verify the config creation on this member

    MemberVM newMember = clusterStartupRule.startServerVM(2, locator.getPort());

    // verify GatewayReceiver attributes saved in cluster config
    newMember.invoke(() -> {
      Set<GatewayReceiver> gatewayReceivers = ClusterStartupRule.getCache().getGatewayReceivers();
      assertThat(gatewayReceivers).isNotNull().isNotEmpty().hasSize(1);
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
      assertThat(gs).isNotNull();
      assertThat(gs.getAlertThreshold()).isEqualTo(alertThreshold);
      assertThat(gs.getBatchSize()).isEqualTo(batchSize);
      assertThat(gs.getDispatcherThreads()).isEqualTo(dispatcherThreads);
      assertThat(gs.isBatchConflationEnabled()).isEqualTo(enableConflation);
      assertDeprecatedManualStart(manualStart, gs);
      assertThat(gs.getBatchTimeInterval()).isEqualTo(batchTimeInterval);
      assertThat(gs.getMaximumQueueMemory()).isEqualTo(maxQueueMemory);
      assertThat(gs.getOrderPolicy()).isSameAs(orderPolicy);
      assertThat(gs.isParallel()).isEqualTo(parallel);
      assertThat(gs.getRemoteDSId()).isEqualTo(rmDsId);
      assertThat(gs.getSocketBufferSize()).isEqualTo(socketBufferSize);
      assertThat(gs.getSocketReadTimeout()).isEqualTo(socketReadTimeout);
    });
  }

  @SuppressWarnings("deprecation")
  private static void assertDeprecatedManualStart(boolean manualStart, GatewaySender gs) {
    assertThat(gs.isManualStart()).isEqualTo(manualStart);
  }

  @SuppressWarnings("deprecation")
  private static void setDeprecatedManualStart(CommandStringBuilder csb, String manualStart) {
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, manualStart);
  }
}
