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
package org.apache.geode.cache.wan.internal.cli.commands;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__BATCHSIZE;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__CANCEL;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MAXRATE;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__ALREADY__RUNNING__COMMAND;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__CANCELED__BEFORE__HAVING__COPIED;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__COPIED__ENTRIES;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__EXECUTION__CANCELED;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__NO__RUNNING__COMMAND;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__REGION__NOT__FOUND;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__SENDER__NOT__FOUND;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__MSG__SENDER__SERIAL__AND__NOT__PRIMARY;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__REGION;
import static org.apache.geode.cache.wan.internal.cli.commands.WanCopyRegionCommand.WAN_COPY_REGION__SENDERID;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.ImmutableList;
import junitparams.Parameters;
import org.assertj.core.api.Condition;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.internal.WanCopyRegionFunctionService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedErrorCollector;
import org.apache.geode.test.dunit.rules.DistributedExecutorServiceRule;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorLauncherStartupRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
@Category({WanTest.class})
public class WanCopyRegionCommandDUnitTest extends WANTestBase {

  protected static VM vm8;

  private static final long serialVersionUID = 1L;

  private enum Gateway {
    SENDER, RECEIVER
  }

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

  @Rule
  public DistributedExecutorServiceRule executorServiceRule = new DistributedExecutorServiceRule();

  @BeforeClass
  public static void beforeClassWanCopyRegionCommandDUnitTest() {
    vm8 = VM.getVM(8);
  }

  @Test
  public void testUnsuccessfulExecution_RegionNotFound() throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    int senderLocatorPort = create3WanSitesAndClient(true, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB, true);

    int wanCopyRegionBatchSize = 20;
    String regionName = "foo";

    // Execute wan-copy region command
    gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
    String commandString = new CommandStringBuilder(WAN_COPY_REGION)
        .addOption(WAN_COPY_REGION__REGION, regionName)
        .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
        .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
        .getCommandString();

    // Check command status and output
    CommandResultAssert command =
        verifyStatusIsError(gfsh.executeAndAssertThat(commandString));
    String message =
        CliStrings.format(WAN_COPY_REGION__MSG__REGION__NOT__FOUND,
            regionName);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .containsExactly(message, message, message);
  }

  @Test
  public void testUnsuccessfulExecution_SenderNotFound() throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    int senderLocatorPort = create3WanSitesAndClient(true, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB, true);

    int wanCopyRegionBatchSize = 20;
    String regionName = getRegionName(true);

    // Execute wan-copy region command
    gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
    String commandString = new CommandStringBuilder(WAN_COPY_REGION)
        .addOption(WAN_COPY_REGION__REGION, regionName)
        .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
        .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
        .getCommandString();

    // Check command status and output
    CommandResultAssert command =
        verifyStatusIsError(gfsh.executeAndAssertThat(commandString));
    String message =
        CliStrings.format(WAN_COPY_REGION__MSG__SENDER__NOT__FOUND, senderIdInA);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .containsExactly(message, message, message);
  }

  @Test
  @Parameters({"true, true", "true, false", "false, false"})
  public void testUnsuccessfulExecution_ExceptionAtReceiver(
      boolean isPartitionedRegion, boolean isParallelGatewaySender) throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    int senderLocatorPort = create3WanSitesAndClient(isPartitionedRegion, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB, true);

    String regionName = getRegionName(isPartitionedRegion);
    int wanCopyRegionBatchSize = 20;

    int entries = 20;
    // Put entries
    client.invoke(() -> doPutsFrom(regionName, 0, entries));

    // Check that entries are put in the region
    for (VM member : serversInA) {
      member.invoke(() -> validateRegionSize(regionName, entries));
    }

    // destroy region to provoke the exception
    serverInB.invoke(() -> destroyRegion(regionName));

    // Create senders and receivers with replication as follows: "A" -> "B" -> "C"
    createSenders(isParallelGatewaySender, serversInA, serverInB,
        senderIdInA, senderIdInB);
    createReceivers(serverInB, serverInC);

    // Execute wan-copy region command
    gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
    String commandString = new CommandStringBuilder(WAN_COPY_REGION)
        .addOption(WAN_COPY_REGION__REGION, regionName)
        .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
        .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
        .getCommandString();

    // Check command status and output
    if (isParallelGatewaySender) {
      CommandResultAssert command =
          verifyStatusIsError(gfsh.executeAndAssertThat(commandString));
      Condition<String> exceptionError =
          new Condition<>(s -> s.startsWith("Error ("), "Error");
      command
          .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
          .hasColumn("Message")
          .asList()
          .haveExactly(3, exceptionError);
    } else {
      CommandResultAssert command =
          verifyStatusIsErrorInOneServer(gfsh.executeAndAssertThat(commandString));
      Condition<String> exceptionError =
          new Condition<>(s -> s.startsWith("Error ("), "Error");
      Condition<String> senderNotPrimary = new Condition<>(
          s -> s.equals(CliStrings
              .format(WAN_COPY_REGION__MSG__SENDER__SERIAL__AND__NOT__PRIMARY,
                  senderIdInA)),
          "sender not primary");
      command
          .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
          .hasColumn("Message")
          .asList()
          .haveExactly(1, exceptionError)
          .haveExactly(2, senderNotPrimary);
    }
  }

  private Object[] parametersToTestSenderOrReceiverGoesDownDuringExecution() {
    return new Object[] {
        new Object[] {true, true, Gateway.SENDER, false},
        new Object[] {false, true, Gateway.SENDER, true},
        new Object[] {false, true, Gateway.SENDER, false},
        new Object[] {false, false, Gateway.SENDER, true},
        new Object[] {false, false, Gateway.SENDER, false},
        new Object[] {true, true, Gateway.RECEIVER, false},
        new Object[] {false, true, Gateway.RECEIVER, true},
        new Object[] {false, true, Gateway.RECEIVER, false},
        new Object[] {false, false, Gateway.RECEIVER, true},
        new Object[] {false, false, Gateway.RECEIVER, false}
    };
  }

  /**
   * This test creates two sites A & B, each one containing 3 servers.
   * A region is created in both sites, and populated in site A.
   * After that replication is configured from site A to site B.
   * WanCopyRegionFunction is called, and while it is running, a sender in site A
   * or a receiver in site B are killed.
   */
  @Test
  @Parameters(method = "parametersToTestSenderOrReceiverGoesDownDuringExecution")
  public void testSenderOrReceiverGoesDownDuringExecution(boolean useParallel,
      boolean usePartitionedRegion, Gateway gwToBeStopped, boolean stopPrimarySender)
      throws Exception {

    if (gwToBeStopped == Gateway.SENDER && (useParallel || stopPrimarySender)) {
      addIgnoredExceptionsForSenderInUseWentDown();
    }
    if (gwToBeStopped == Gateway.RECEIVER &&
        (usePartitionedRegion || !useParallel)) {
      addIgnoredExceptionsForReceiverConnectedToSenderInUseWentDown();
    }
    final int wanCopyRegionBatchSize = 10;
    final int entries;
    if (!useParallel && !usePartitionedRegion && stopPrimarySender) {
      entries = 2500;
    } else {
      entries = 1000;
    }

    final String regionName = getRegionName(usePartitionedRegion);

    // Site A
    VM locatorInA = vm0;
    VM server1InA = vm1;
    VM server2InA = vm2;
    VM server3InA = vm3;
    List<VM> serversInA = Arrays.asList(server1InA, server2InA, server3InA);
    final String senderIdInA = "B";

    // Site B
    VM locatorInB = vm4;
    VM server1InB = vm5;
    VM server2InB = vm6;
    VM server3InB = vm7;
    List<VM> serversInB = Arrays.asList(server1InB, server2InB, server3InB);
    VM client = vm8;

    int locatorAPort = create2WanSitesAndClient(locatorInA, serversInA, senderIdInA, locatorInB,
        serversInB, client, usePartitionedRegion, regionName);

    // Put entries
    client.invoke(() -> doPutsFrom(regionName, 0, entries));
    for (VM member : serversInA) {
      member.invoke(() -> validateRegionSize(regionName, entries));
    }

    // Create senders and receivers with replication as follows: "A" -> "B"
    if (useParallel) {
      createReceiverInVMs(server1InB, server2InB, server3InB);
      createSenders(useParallel, serversInA, null, senderIdInA, null);
    } else {
      // Senders will connect to receiver in server1InB
      server1InB.invoke(WANTestBase::createReceiver);
      createSenders(useParallel, serversInA, null, senderIdInA, null);
      createReceiverInVMs(server2InB, server3InB);
    }

    Callable<CommandResultAssert> wanCopyCommandCallable = () -> {
      String command = new CommandStringBuilder(WAN_COPY_REGION)
          .addOption(WAN_COPY_REGION__REGION, regionName)
          .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
          .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
          .addOption(WAN_COPY_REGION__MAXRATE, "50")
          .getCommandString();
      try {
        gfsh.connectAndVerify(locatorAPort, GfshCommandRule.PortType.locator);
      } catch (Exception e) {
        errorCollector.addError(e);
      }
      return gfsh.executeAndAssertThat(command);
    };

    Future<CommandResultAssert> wanCopyCommandFuture =
        executorServiceRule.submit(wanCopyCommandCallable);

    // Wait for the wan-copy command to start
    waitForWanCopyRegionCommandToStart(useParallel, usePartitionedRegion, serversInA);

    // Stop sender or receiver and verify result
    if (gwToBeStopped == Gateway.SENDER) {
      stopSenderAndVerifyResult(useParallel, stopPrimarySender, server2InA, serversInA, senderIdInA,
          wanCopyCommandFuture);
    } else if (gwToBeStopped == Gateway.RECEIVER) {
      stopReceiverAndVerifyResult(useParallel, stopPrimarySender, entries, regionName, server1InB,
          server2InB, server3InB, wanCopyCommandFuture);
    }
  }

  @Test
  @Parameters({"false, false", "false, true", "true, true"})
  public void testRegionDestroyedDuringExecution(boolean isParallelGatewaySender,
      boolean isPartitionedRegion)
      throws Exception {
    addIgnoredException("org.apache.geode.cache.RegionDestroyedException");
    final int wanCopyRegionBatchSize = 10;
    final int entries = 1000;

    final String regionName = getRegionName(isPartitionedRegion);

    // Site A
    VM locatorInA = vm0;
    VM server1InA = vm1;
    List<VM> serversInA = ImmutableList.of(server1InA);
    final String senderIdInA = "B";

    // Site B
    VM locatorInB = vm4;
    VM server1InB = vm5;
    List<VM> serversInB = ImmutableList.of(server1InB);
    VM client = vm8;

    int locatorAPort = create2WanSitesAndClient(locatorInA, serversInA, senderIdInA, locatorInB,
        serversInB, client, isPartitionedRegion, regionName);

    // Put entries
    client.invoke(() -> doPutsFrom(regionName, 0, entries));
    for (VM member : serversInA) {
      member.invoke(() -> validateRegionSize(regionName, entries));
    }

    // Create senders and receivers with replication as follows: "A" -> "B"
    createReceiverInVMs(server1InB);
    createSenders(isParallelGatewaySender, serversInA, null, senderIdInA, null);

    Callable<CommandResultAssert> wanCopyCommandCallable = () -> {
      String command = new CommandStringBuilder(WAN_COPY_REGION)
          .addOption(WAN_COPY_REGION__REGION, regionName)
          .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
          .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
          .addOption(WAN_COPY_REGION__MAXRATE, "5")
          .getCommandString();
      try {
        gfsh.connectAndVerify(locatorAPort, GfshCommandRule.PortType.locator);
      } catch (Exception e) {
        errorCollector.addError(e);
      }
      return gfsh.executeAndAssertThat(command);
    };

    Future<CommandResultAssert> wanCopyCommandFuture =
        executorServiceRule.submit(wanCopyCommandCallable);

    // Wait for the wan-copy command to start
    waitForWanCopyRegionCommandToStart(isParallelGatewaySender, isPartitionedRegion, serversInA);

    // Destroy region in a server in A
    server1InA.invoke(() -> cache.getRegion(regionName).destroyRegion());

    CommandResultAssert result = wanCopyCommandFuture.get();
    result.statusIsError();
    result
        .hasTableSection()
        .hasColumns()
        .hasSize(3);
    result
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Member")
        .hasSize(1);
    result
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .hasSize(1);
    result
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Status")
        .containsExactly("ERROR");

    Condition<String> regionDestroyedError = new Condition<>(
        s -> (s.startsWith(
            "Execution failed. Error: org.apache.geode.cache.RegionDestroyedException: ")
            || s.startsWith("Error (Region destroyed) in operation after having copied")),
        "execution error");
    result
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .asList()
        .haveExactly(1, regionDestroyedError);
  }

  @Test
  @Parameters({"false, false", "false, true", "true, true"})
  public void testDetectOngoingExecution(boolean useParallel,
      boolean usePartitionedRegion)
      throws Exception {
    final int wanCopyRegionBatchSize = 10;
    final int entries = 1000;

    final String regionName = getRegionName(usePartitionedRegion);

    // Site A
    VM locatorInA = vm0;
    VM server1InA = vm1;
    VM server2InA = vm2;
    VM server3InA = vm3;
    List<VM> serversInA = Arrays.asList(server1InA, server2InA, server3InA);
    final String senderIdInA = "B";

    // Site B
    VM locatorInB = vm4;
    VM server1InB = vm5;
    VM server2InB = vm6;
    VM server3InB = vm7;
    List<VM> serversInB = Arrays.asList(server1InB, server2InB, server3InB);
    VM client = vm8;

    int locatorAPort = create2WanSitesAndClient(locatorInA, serversInA, senderIdInA, locatorInB,
        serversInB, client, usePartitionedRegion, regionName);

    // Put entries
    client.invoke(() -> doPutsFrom(regionName, 0, entries));
    for (VM member : serversInA) {
      member.invoke(() -> validateRegionSize(regionName, entries));
    }

    // Create senders and receivers with replication as follows: "A" -> "B"
    if (useParallel) {
      createReceiverInVMs(server1InB, server2InB, server3InB);
      createSenders(useParallel, serversInA, null, senderIdInA, null);
    } else {
      // Senders will connect to receiver in server1InB
      server1InB.invoke(WANTestBase::createReceiver);
      createSenders(useParallel, serversInA, null, senderIdInA, null);
      createReceiverInVMs(server2InB, server3InB);
    }

    Callable<CommandResultAssert> wanCopyCommandCallable = () -> {
      String command = new CommandStringBuilder(WAN_COPY_REGION)
          .addOption(WAN_COPY_REGION__REGION, regionName)
          .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
          .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
          .addOption(WAN_COPY_REGION__MAXRATE, "5")
          .getCommandString();
      try {
        gfsh.connectAndVerify(locatorAPort, GfshCommandRule.PortType.locator);
      } catch (Exception e) {
        errorCollector.addError(e);
      }
      return gfsh.executeAndAssertThat(command);
    };

    Future<CommandResultAssert> wanCopyCommandFuture =
        executorServiceRule.submit(wanCopyCommandCallable);

    // Wait for the wan-copy command to start
    waitForWanCopyRegionCommandToStart(useParallel, usePartitionedRegion, serversInA);

    // Execute again the same wan-copy region command
    String commandString = new CommandStringBuilder(WAN_COPY_REGION)
        .addOption(WAN_COPY_REGION__REGION, regionName)
        .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
        .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
        .getCommandString();

    // Check command status and output
    Condition<String> exceptionError = new Condition<>(
        s -> s.equals(CliStrings.format(WAN_COPY_REGION__MSG__ALREADY__RUNNING__COMMAND,
            regionName, senderIdInA)),
        "already running");
    if (useParallel) {
      CommandResultAssert command =
          verifyStatusIsError(gfsh.executeAndAssertThat(commandString));
      command
          .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
          .hasColumn("Message")
          .asList()
          .haveExactly(3, exceptionError);
    } else {
      CommandResultAssert command =
          verifyStatusIsErrorInOneServer(gfsh.executeAndAssertThat(commandString));
      Condition<String> senderNotPrimary = new Condition<>(
          s -> s.equals(CliStrings
              .format(WAN_COPY_REGION__MSG__SENDER__SERIAL__AND__NOT__PRIMARY,
                  senderIdInA)),
          "sender not primary");
      command
          .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
          .hasColumn("Message")
          .asList()
          .haveExactly(1, exceptionError)
          .haveExactly(2, senderNotPrimary);
    }

    // cancel command
    String commandString1 = new CommandStringBuilder(WAN_COPY_REGION)
        .addOption(WAN_COPY_REGION__REGION, regionName)
        .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
        .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
        .addOption(WAN_COPY_REGION__CANCEL)
        .getCommandString();
    gfsh.executeAndAssertThat(commandString1);
    wanCopyCommandFuture.get();
    addIgnoredExceptionsForClosingAfterCancelCommand();
  }

  /**
   * Scenario with 3 WAN sites: "A", "B" and "C".
   * Initially, no replication is configured between sites.
   * Several entries are put in WAN site "A".
   *
   * The following gateway senders are created and started:
   * - In "A" site: to replicate region entries to "B" site. Sender called "B".
   * - In "B" site: to replicate region entries to "C" site. Sender called "C".
   * (Replication is as follows: A -> B -> C)
   *
   * The "wan-copy region" command is run from "A" site passing sender "B".
   *
   * It must be verified that the entries are copied to site "B".
   * It must also be verified that the entries are not transitively
   * copied to "C" even though replication is configured from "B" to "C"
   * because with this command generateCallbacks is set to false in the
   * events generated.
   *
   */
  @Test
  @Parameters({"true, true, true", "true, false, true", "false, false, true", "true, true, false",
      "true, false, false", "false, false, false"})
  public void testSuccessfulExecution(boolean isPartitionedRegion,
      boolean isParallelGatewaySender, boolean concurrencyChecksEnabled) throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    int senderLocatorPort = create3WanSitesAndClient(isPartitionedRegion, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB, concurrencyChecksEnabled);

    int wanCopyRegionBatchSize = 20;
    int entries = 100;
    String regionName = getRegionName(isPartitionedRegion);
    // Put entries
    client.invoke(() -> doPutsFrom(regionName, 0, entries + 1));
    // remove an entry to make sure that replication works well even when removes.
    client.invoke(() -> removeEntry(regionName, entries));

    // Check that entries are put in the region
    for (VM member : serversInA) {
      member.invoke(() -> validateRegionSize(regionName, entries));
    }

    // Check that entries are not copied to "B" nor "C"
    serverInB.invoke(() -> validateRegionSize(regionName, 0));
    serverInC.invoke(() -> validateRegionSize(regionName, 0));

    // Create senders and receivers with replication as follows: "A" -> "B" -> "C"
    createSenders(isParallelGatewaySender, serversInA, serverInB,
        senderIdInA, senderIdInB);
    createReceivers(serverInB, serverInC);

    // Check that entries are not copied to "B" nor "C"
    serverInB.invoke(() -> validateRegionSize(regionName, 0));
    serverInC.invoke(() -> validateRegionSize(regionName, 0));

    // Execute wan-copy region command
    gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
    String commandString = new CommandStringBuilder(WAN_COPY_REGION)
        .addOption(WAN_COPY_REGION__REGION, regionName)
        .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
        .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
        .getCommandString();

    // Check command status and output
    CommandResultAssert command =
        verifyStatusIsOk(gfsh.executeAndAssertThat(commandString));
    if (isPartitionedRegion && isParallelGatewaySender) {
      String msg1 = CliStrings.format(WAN_COPY_REGION__MSG__COPIED__ENTRIES, 33);
      String msg2 = CliStrings.format(WAN_COPY_REGION__MSG__COPIED__ENTRIES, 34);
      command
          .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
          .hasColumn("Message")
          .containsExactlyInAnyOrder(msg1, msg1, msg2);
    } else {
      String msg1 = CliStrings.format(WAN_COPY_REGION__MSG__COPIED__ENTRIES, 100);
      String msg2 = CliStrings
          .format(WAN_COPY_REGION__MSG__SENDER__SERIAL__AND__NOT__PRIMARY, senderIdInA);
      command
          .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
          .hasColumn("Message")
          .containsExactlyInAnyOrder(msg1, msg2, msg2);
    }

    // Check that entries are copied in "B"
    serverInB.invoke(() -> validateRegionSize(regionName, entries));

    // Check that the region's data is the same in sites "A" and "B"
    checkEqualRegionData(regionName, serversInA.get(0), serverInB, concurrencyChecksEnabled);

    // Check that wanCopyRegionBatchSize is correctly used by the command
    long receivedBatches = serverInB.invoke(() -> getReceiverStats().get(2));
    if (isPartitionedRegion && isParallelGatewaySender) {
      assertThat(receivedBatches).isEqualTo(6);
    } else {
      assertThat(receivedBatches).isEqualTo(5);
    }

    // Check that entries are not copied in "C" (generateCallbacks is false)
    serverInC.invoke(() -> validateRegionSize(regionName, 0));
  }

  /**
   * Scenario with 2 WAN sites: "A" and "B".
   * Initially, no replication is configured between sites.
   * Several entries are put in WAN site "A".
   *
   * The following gateway senders are created and started:
   * - In "A" site: to replicate region entries to "B" site. Sender called "B".
   * (Replication is as follows: A -> B)
   *
   * The "wan-copy region" command is run from "A" site passing sender "B".
   * Simultaneously, random operations for entries with the same
   * keys as the one previously put are run.
   *
   * When the command finishes and the puts finish it
   * must be verified that the entries in the region in site "A"
   * are the same as the ones in region in site "B".
   */
  @Test
  @Parameters({"true, true", "true, false", "false, false"})
  public void testSuccessfulExecutionWhileRunningOpsOnRegion(
      boolean isPartitionedRegion,
      boolean isParallelGatewaySender) throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    int senderLocatorPort = create3WanSitesAndClient(isPartitionedRegion, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB, true);

    int wanCopyRegionBatchSize = 20;
    int entries = 10000;
    Set<Long> keySet = LongStream.range(0L, entries).boxed().collect(Collectors.toSet());
    String regionName = getRegionName(isPartitionedRegion);
    // Put entries
    client.invoke(() -> doPutsFrom(regionName, 0, entries));

    // Check that entries are put in the region
    for (VM member : serversInA) {
      member.invoke(() -> validateRegionSize(regionName, entries));
    }

    // Create senders and receivers with replication as follows: "A" -> "B" -> "C"
    createSenders(isParallelGatewaySender, serversInA, serverInB,
        senderIdInA, senderIdInB);
    createReceivers(serverInB, serverInC);

    // Let maxRate be a 5th of the number of entries in order to have the
    // command running for about 5 seconds so that there is time
    // for the random operations to be done at the same time
    // as the command is running.
    // The rate is divided by the number of servers in case the sender is parallel
    int maxRate = (entries / 5) / (isParallelGatewaySender ? serversInA.size() : 1);

    // Execute wan-copy region command
    Callable<CommandResultAssert> wanCopyCommandCallable = () -> {
      String command = new CommandStringBuilder(WAN_COPY_REGION)
          .addOption(WAN_COPY_REGION__REGION, regionName)
          .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
          .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
          .addOption(WAN_COPY_REGION__MAXRATE, String.valueOf(maxRate))
          .getCommandString();
      try {
        gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
      } catch (Exception e) {
        errorCollector.addError(e);
      }
      return gfsh.executeAndAssertThat(command);
    };

    Future<CommandResultAssert> wanCopyCommandFuture =
        executorServiceRule.submit(wanCopyCommandCallable);

    // Wait for the wan-copy command to start
    waitForWanCopyRegionCommandToStart(isParallelGatewaySender, isPartitionedRegion, serversInA);

    // While the command is running, send some random operations over the same keys
    AsyncInvocation<Void> asyncOps1 =
        client.invokeAsync(() -> sendRandomOpsFromClient(regionName, keySet, 3));
    AsyncInvocation<Void> asyncOps2 =
        client.invokeAsync(() -> sendRandomOpsFromClient(regionName, keySet, 3));
    AsyncInvocation<Void> asyncOps3 =
        client.invokeAsync(() -> sendRandomOpsFromClient(regionName, keySet, 3));

    // Check command status and output
    CommandResultAssert command = wanCopyCommandFuture.get();
    verifyStatusIsOk(command);

    // Wait for random operations to finish
    asyncOps1.await();
    asyncOps2.await();
    asyncOps3.await();

    // Wait for entries to be replicated (replication queues empty)
    for (VM server : serversInA) {
      server.invoke(() -> getSenderStats(senderIdInA, 0));
    }

    // Check that the region's data is the same in sites "A" and "B"
    checkEqualRegionData(regionName, serversInA.get(0), serverInB, true);
  }

  /**
   * Scenario with 2 WAN sites: "A" and "B".
   * Initially, no replication is configured between sites.
   * Several entries are put in WAN site "A".
   *
   * The following gateway senders are created and started:
   * - In "A" site: to replicate region entries to "B" site. Sender called "B".
   * (Replication is as follows: A -> B)
   *
   * The "wan-copy region" command is run from "A" site passing sender "B".
   * Simultaneously, some put operations for entries with keys different
   * to the ones previously put are run.
   *
   * When the command finishes and the puts finish it
   * must be verified that the number of entries copied by it
   * is equal to the number of entries put before the command
   * was run.
   * Also it must be verified that the
   * entries in the region in site "A"
   * are the same as the ones in region in site "B".
   */
  @Test
  @Parameters({"true, true", "true, false", "false, false"})
  public void testCommandDoesNotCopyEntriesUpdatedAfterCommandStarted(
      boolean isPartitionedRegion,
      boolean isParallelGatewaySender) throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    int senderLocatorPort = create3WanSitesAndClient(isPartitionedRegion, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB, true);

    int entries = 50000;
    String regionName = getRegionName(isPartitionedRegion);
    // Put entries
    client.invoke(() -> doPutsFrom(regionName, 0, entries));

    // Check that entries are put in the region
    for (VM member : serversInA) {
      member.invoke(() -> validateRegionSize(regionName, entries));
    }

    // Create senders and receivers with replication as follows: "A" -> "B" -> "C"
    createSenders(isParallelGatewaySender, serversInA, serverInB,
        senderIdInA, senderIdInB);
    createReceivers(serverInB, serverInC);

    int wanCopyRegionBatchSize = 20;
    // Let maxRate be a 5th of the number of entries in order to have the
    // command running for about 5 seconds so that there is time
    // for the random operations to be done at the same time
    // as the command is running.
    // The rate is divided by the number of servers in case the sender is parallel
    int maxRate = (entries / 5) / (isParallelGatewaySender ? serversInA.size() : 1);

    // Execute wan-copy region command
    Callable<CommandResultAssert> wanCopyCommandCallable = () -> {
      String command = new CommandStringBuilder(WAN_COPY_REGION)
          .addOption(WAN_COPY_REGION__REGION, regionName)
          .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
          .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
          .addOption(WAN_COPY_REGION__MAXRATE, String.valueOf(maxRate))
          .getCommandString();
      try {
        gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
      } catch (Exception e) {
        errorCollector.addError(e);
      }
      return gfsh.executeAndAssertThat(command);
    };

    Future<CommandResultAssert> wanCopyCommandFuture =
        executorServiceRule.submit(wanCopyCommandCallable);

    // Wait for the command to start
    waitForWanCopyRegionCommandToStart(isParallelGatewaySender, isPartitionedRegion, serversInA);

    // While the command is running, send some random operations over a different set of keys
    client.invoke(() -> doPutsFrom(regionName, entries, entries + 2000));

    // Check command status and output
    CommandResultAssert command = wanCopyCommandFuture.get();
    verifyStatusIsOk(command);

    // Wait for entries to be replicated (replication queues empty)
    for (VM server : serversInA) {
      server.invoke(() -> getSenderStats(senderIdInA, 0));
    }

    // Check that the region's data is the same in sites "A" and "B"
    checkEqualRegionData(regionName, serversInA.get(0), serverInB, true);

    // Check that the number of entries copied is equal to the number of
    // entries put before the command was executed.
    assertThat(getCopiedEntries(command)).isEqualTo(entries);
  }

  /**
   * Cancel is executed when no wan-copy region command is running.
   */
  @Test
  @Parameters({"true, true", "true, false", "false, false"})
  public void testUnsuccessfulCancelExecution(boolean isPartitionedRegion,
      boolean isParallelGatewaySender) throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    int senderLocatorPort = create3WanSitesAndClient(isPartitionedRegion, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB, true);

    String regionName = getRegionName(isPartitionedRegion);

    // Create senders and receivers with replication as follows: "A" -> "B" -> "C"
    createSenders(isParallelGatewaySender, serversInA, serverInB,
        senderIdInA, senderIdInB);
    createReceivers(serverInB, serverInC);

    // Execute cancel wan-copy region command
    gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
    String cancelCommand = new CommandStringBuilder(WAN_COPY_REGION)
        .addOption(WAN_COPY_REGION__REGION, regionName)
        .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
        .addOption(WAN_COPY_REGION__CANCEL)
        .getCommandString();

    CommandResultAssert cancelCommandResult =
        verifyStatusIsError(gfsh.executeAndAssertThat(cancelCommand));
    String msg1 = CliStrings.format(WAN_COPY_REGION__MSG__NO__RUNNING__COMMAND,
        regionName, senderIdInA);
    cancelCommandResult
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .containsExactlyInAnyOrder(msg1, msg1, msg1);
  }

  /**
   * Scenario with 3 WAN sites: "A", "B" and "C".
   * Initially, no replication is configured between sites.
   * Several entries are put in WAN site "A".
   *
   * The following gateway senders are created and started:
   * - In "A" site: to replicate region entries to "B" site. Sender called "B".
   * - In "B" site: to replicate region entries to "C" site. Sender called "C".
   * (Replication is as follows: A -> B -> C)
   *
   * The "wan-copy region" command is run from "A" site passing sender "B"
   * in a different thread. The maxRate is set to a very low value so that there is
   * time to cancel it before it finishes.
   *
   * The "wan-copy region" command with the cancel option
   * is run from "A" site passing sender "B".
   *
   * It must be verified that the command is canceled.
   * Also, the output of the command must show
   * the number of entries copied before the command was canceled.
   */
  @Test
  @Parameters({"true, true", "true, false", "false, false"})
  public void testSuccessfulCancelExecution(boolean isPartitionedRegion,
      boolean isParallelGatewaySender) throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    int senderLocatorPort = create3WanSitesAndClient(isPartitionedRegion, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB, true);

    int wanCopyRegionBatchSize = 20;
    int entries = 100;
    String regionName = getRegionName(isPartitionedRegion);
    // Put entries
    client.invoke(() -> doPutsFrom(regionName, 0, entries));

    // Create senders and receivers with replication as follows: "A" -> "B" -> "C"
    createSenders(isParallelGatewaySender, serversInA, serverInB,
        senderIdInA, senderIdInB);
    createReceivers(serverInB, serverInC);

    Callable<CommandResultAssert> wanCopyCommandCallable = () -> {
      String command = new CommandStringBuilder(WAN_COPY_REGION)
          .addOption(WAN_COPY_REGION__REGION, regionName)
          .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
          .addOption(WAN_COPY_REGION__BATCHSIZE, String.valueOf(wanCopyRegionBatchSize))
          .addOption(WAN_COPY_REGION__MAXRATE, String.valueOf(1))
          .getCommandString();
      try {
        gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
      } catch (Exception e) {
        errorCollector.addError(e);
      }
      return gfsh.executeAndAssertThat(command).statusIsError();
    };

    Future<CommandResultAssert> wanCopyCommandFuture =
        executorServiceRule.submit(wanCopyCommandCallable);

    // Wait for the wan-copy command to start
    waitForWanCopyRegionCommandToStart(isParallelGatewaySender, isPartitionedRegion, serversInA);

    // Cancel wan-copy region command
    GfshCommandRule gfshCancelCommand = new GfshCommandRule();
    gfshCancelCommand.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
    String cancelCommand = new CommandStringBuilder(WAN_COPY_REGION)
        .addOption(WAN_COPY_REGION__REGION, regionName)
        .addOption(WAN_COPY_REGION__SENDERID, senderIdInA)
        .addOption(WAN_COPY_REGION__CANCEL)
        .getCommandString();
    CommandResultAssert cancelCommandResult =
        gfshCancelCommand.executeAndAssertThat(cancelCommand);

    if (isPartitionedRegion && isParallelGatewaySender) {
      verifyStatusIsOk(cancelCommandResult);
      cancelCommandResult
          .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
          .hasColumn("Message")
          .containsExactlyInAnyOrder(WAN_COPY_REGION__MSG__EXECUTION__CANCELED,
              WAN_COPY_REGION__MSG__EXECUTION__CANCELED,
              WAN_COPY_REGION__MSG__EXECUTION__CANCELED);
    } else {
      verifyStatusIsOkInOneServer(cancelCommandResult);
      String msg1 = CliStrings.format(WAN_COPY_REGION__MSG__NO__RUNNING__COMMAND,
          regionName, senderIdInA);
      cancelCommandResult
          .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
          .hasColumn("Message")
          .containsExactlyInAnyOrder(msg1, msg1, WAN_COPY_REGION__MSG__EXECUTION__CANCELED);
    }

    // Check wan-copy region command output
    CommandResultAssert wanCopyCommandResult = wanCopyCommandFuture.get();
    if (isPartitionedRegion && isParallelGatewaySender) {
      verifyStatusIsError(wanCopyCommandResult);
      String msg = WAN_COPY_REGION__MSG__CANCELED__BEFORE__HAVING__COPIED;
      wanCopyCommandResult
          .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
          .hasColumn("Message")
          .containsExactly(msg, msg, msg);
    } else {
      verifyStatusIsErrorInOneServer(wanCopyCommandResult);
      String msg1 = CliStrings
          .format(WAN_COPY_REGION__MSG__SENDER__SERIAL__AND__NOT__PRIMARY, senderIdInA);
      wanCopyCommandResult
          .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
          .hasColumn("Message")
          .containsExactlyInAnyOrder(WAN_COPY_REGION__MSG__CANCELED__BEFORE__HAVING__COPIED, msg1,
              msg1);
    }
    addIgnoredExceptionsForClosingAfterCancelCommand();
  }

  private int create3WanSitesAndClient(boolean isPartitionedRegion, VM locatorSender,
      VM locatorSenderReceiver, VM locatorReceiver, List<VM> serversInA, VM serverInB,
      VM serverInC, VM client, String senderIdInA, String senderIdInB,
      boolean concurrencyChecksEnabled) {
    // Create locators
    int receiverLocatorPort =
        locatorReceiver.invoke(() -> createFirstLocatorWithDSId(3));
    int senderReceiverLocatorPort = locatorSenderReceiver
        .invoke(() -> createFirstRemoteLocator(2, receiverLocatorPort));
    int senderLocatorPort = locatorSender.invoke(() -> {
      Properties props = getDistributedSystemProperties();
      props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
      props.setProperty(REMOTE_LOCATORS, "localhost[" + senderReceiverLocatorPort + "]");
      LocatorLauncherStartupRule launcherStartupRule =
          new LocatorLauncherStartupRule().withProperties(props);
      launcherStartupRule.start();
      return launcherStartupRule.getLauncher().getPort();
    });

    // Create servers
    Properties properties = new Properties();
    serverInB.invoke(() -> createServer(senderReceiverLocatorPort, -1, properties));
    serverInC.invoke(() -> createServer(receiverLocatorPort, -1, properties));
    for (VM server : serversInA) {
      server.invoke(() -> createServer(senderLocatorPort, -1, properties));
    }

    // Create region in servers
    final String regionName = getRegionName(isPartitionedRegion);
    if (isPartitionedRegion) {
      for (VM server : serversInA) {
        server
            .invoke(() -> createPartitionedRegion(regionName, senderIdInA, 1, 100,
                isOffHeap(), RegionShortcut.PARTITION, true, concurrencyChecksEnabled));
      }
      serverInB.invoke(
          () -> createPartitionedRegion(regionName, senderIdInB, 0, 100,
              isOffHeap(), RegionShortcut.PARTITION, true, concurrencyChecksEnabled));
      serverInC.invoke(() -> createPartitionedRegion(regionName, null, 0, 100,
          isOffHeap(), RegionShortcut.PARTITION, true, concurrencyChecksEnabled));
    } else {
      for (VM server : serversInA) {
        server.invoke(() -> createReplicatedRegion(regionName, senderIdInA,
            Scope.GLOBAL, DataPolicy.REPLICATE,
            isOffHeap(), true, concurrencyChecksEnabled));
      }
      serverInB
          .invoke(() -> createReplicatedRegion(regionName, senderIdInB,
              Scope.GLOBAL, DataPolicy.REPLICATE,
              isOffHeap(), true, concurrencyChecksEnabled));
      serverInC.invoke(() -> createReplicatedRegion(regionName, null,
          Scope.GLOBAL, DataPolicy.REPLICATE, isOffHeap(), true, concurrencyChecksEnabled));
    }

    // Create client
    client.invoke(() -> createClientWithLocatorAndRegion(senderLocatorPort, "localhost",
        regionName, ClientRegionShortcut.PROXY));

    return senderLocatorPort;
  }

  private int create2WanSitesAndClient(VM locatorInA, List<VM> serversInA, String senderIdInA,
      VM locatorInB, List<VM> serversInB, VM client, boolean usePartitionedRegion,
      String regionName) {
    // Create locators
    int locatorBPort = locatorInB.invoke(() -> createFirstLocatorWithDSId(2));
    int locatorAPort = locatorInA.invoke(() -> {
      Properties props = getDistributedSystemProperties();
      props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
      props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorBPort + "]");
      LocatorLauncherStartupRule launcherStartupRule =
          new LocatorLauncherStartupRule().withProperties(props);
      launcherStartupRule.start();
      return launcherStartupRule.getLauncher().getPort();
    });

    // Create servers and regions
    createServersAndRegions(locatorBPort, serversInB, usePartitionedRegion, regionName, null);
    createServersAndRegions(locatorAPort, serversInA, usePartitionedRegion, regionName,
        senderIdInA);

    // Create client
    client.invoke(() -> createClientWithLocatorAndRegion(locatorAPort, "localhost",
        regionName, ClientRegionShortcut.PROXY));

    return locatorAPort;
  }

  private void createSenders(boolean isParallelGatewaySender, List<VM> serversInA,
      VM serverInB, String senderIdInA, String senderIdInB) {
    if (serverInB != null && senderIdInB != null) {
      serverInB.invoke(() -> createSender(senderIdInB, 3,
          isParallelGatewaySender, 100, 10, false,
          false, null, false));
    }
    for (VM server : serversInA) {
      server.invoke(() -> createSender(senderIdInA, 2, isParallelGatewaySender,
          100, 10, false,
          false, null, true));
    }
    startSenderInVMsAsync(senderIdInA, serversInA.toArray(new VM[0]));
  }

  private void createReceivers(VM serverInB, VM serverInC) {
    createReceiverInVMs(serverInB);
    createReceiverInVMs(serverInC);
  }

  private void stopReceiverAndVerifyResult(boolean useParallel, boolean stopPrimarySender,
      int entries, String regionName, VM server1InB, VM server2InB, VM server3InB,
      Future<CommandResultAssert> commandFuture)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    // if parallel sender: stop any receiver
    // if serial sender: stop receiver connected to primary or secondary
    if (useParallel) {
      server2InB.invoke(() -> cache.close());
    } else {
      // Region type has no influence on which server should be stopped
      if (stopPrimarySender) {
        // Stop the first server which had an available receiver
        server1InB.invoke(() -> cache.close());
      } else {
        server3InB.invoke(() -> cache.close());
      }
    }

    CommandResultAssert result = commandFuture.get();
    // Verify result
    if (useParallel) {
      verifyResultOfStoppingReceiverWhenUsingParallelSender(result);
    } else {
      verifyResultOfStoppingReceiverWhenUsingSerialSender(result);
      server2InB.invoke(() -> validateRegionSize(regionName, entries));
    }
  }

  private void stopSenderAndVerifyResult(boolean useParallel, boolean stopPrimarySender,
      VM server2InA, List<VM> serversInA, String senderIdInA,
      Future<CommandResultAssert> wanCopyCommandFuture)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    // If parallel: stop any server
    // If serial: stop primary or secondary
    if (useParallel) {
      server2InA.invoke(() -> killSender(senderIdInA));
    } else {
      for (VM server : serversInA) {
        boolean senderWasStopped = server.invoke(() -> {
          GatewaySender sender = cache.getGatewaySender(senderIdInA);
          if (((InternalGatewaySender) sender).isPrimary() == stopPrimarySender) {
            killSender();
            return true;
          }
          return false;
        });
        if (senderWasStopped) {
          break;
        }
      }
    }

    CommandResultAssert result = wanCopyCommandFuture.get();
    // Verify result
    if (useParallel) {
      verifyResultOfStoppingParallelSender(result);
    } else {
      if (stopPrimarySender) {
        verifyResultOfStoppingPrimarySerialSender(result);
      } else {
        verifyResultStoppingSecondarySerialSender(result);
      }
    }
  }

  public void verifyResultOfStoppingReceiverWhenUsingSerialSender(
      CommandResultAssert command) {
    verifyStatusIsOk(command);
    Condition<String> haveEntriesCopied =
        new Condition<>(s -> s.startsWith("Entries copied: "), "Entries copied");
    Condition<String> senderNotPrimary = new Condition<>(
        s -> s.equals("Sender B is serial and not primary. 0 entries copied."),
        "sender not primary");

    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .asList()
        .haveExactly(1, haveEntriesCopied)
        .haveExactly(2, senderNotPrimary);
  }

  public void verifyResultOfStoppingReceiverWhenUsingParallelSender(
      CommandResultAssert command) {
    verifyStatusIsOk(command);
    Condition<String> haveEntriesCopied =
        new Condition<>(s -> s.startsWith("Entries copied: "), "Entries copied");

    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .asList()
        .haveExactly(3, haveEntriesCopied);
  }

  public void verifyResultOfStoppingParallelSender(CommandResultAssert command) {
    verifyStatusIsErrorInOneServer(command);
    Condition<String> startsWithError = new Condition<>(
        s -> (s.startsWith("Execution failed. Error:")
            || s.startsWith("Error (Unknown error sending batch)")
            || s.startsWith("Error (Region destroyed)")
            || s.startsWith("MemberResponse got memberDeparted event for")
            || s.equals(WAN_COPY_REGION__MSG__CANCELED__BEFORE__HAVING__COPIED)),
        "execution error");
    Condition<String> haveEntriesCopied =
        new Condition<>(s -> s.startsWith("Entries copied:"), "Entries copied");

    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .asList()
        .haveExactly(1, startsWithError)
        .haveExactly(2, haveEntriesCopied);
  }

  public void verifyResultOfStoppingPrimarySerialSender(
      CommandResultAssert command) {
    verifyStatusIsErrorInOneServer(command);

    Condition<String> startsWithError = new Condition<>(
        s -> (s.startsWith("Execution failed. Error:")
            || s.startsWith("Error (Unknown error sending batch)")
            || s.startsWith("No connection available towards receiver after having copied")
            || s.startsWith("Error (Region destroyed)")
            || s.startsWith("MemberResponse got memberDeparted event for")
            || s.equals(WAN_COPY_REGION__MSG__CANCELED__BEFORE__HAVING__COPIED)),
        "execution error");

    Condition<String> senderNotPrimary = new Condition<>(
        s -> s.equals("Sender B is serial and not primary. 0 entries copied."),
        "sender not primary");

    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .asList()
        .haveExactly(1, startsWithError)
        .haveExactly(2, senderNotPrimary);
  }

  public void verifyResultStoppingSecondarySerialSender(
      CommandResultAssert command) {
    command.statusIsSuccess();
    command
        .hasTableSection()
        .hasColumns()
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Member")
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Status")
        .containsExactlyInAnyOrder("OK", "OK", "OK");

    Condition<String> haveEntriesCopied =
        new Condition<>(s -> s.startsWith("Entries copied:"), "Entries copied");
    Condition<String> senderNotPrimary = new Condition<>(
        s -> s.equals("Sender B is serial and not primary. 0 entries copied."),
        "sender not primary");

    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .asList()
        .haveExactly(1, haveEntriesCopied)
        .haveExactly(2, senderNotPrimary);
  }

  private String getRegionName(boolean isPartitionedRegion) {
    return getTestMethodName() + (isPartitionedRegion ? "_PR" : "RR");
  }

  public static void removeEntry(String regionName, long key) {
    Region<?, ?> region = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(region);
    region.remove(key);
  }

  public void sendRandomOpsFromClient(String regionName, Set<Long> keySet, int iterations) {
    Region<Long, Integer> region = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(region);
    int min = 0;
    int max = 1000;
    for (int i = 0; i < iterations; i++) {
      for (Long key : keySet) {
        long longKey = key;
        int value = (int) (Math.random() * (max - min + 1) + min);
        if (value < 50) {
          region.remove(longKey);
        } else {
          region.put(longKey, value);
        }
      }
    }
  }

  public CommandResultAssert verifyStatusIsOk(CommandResultAssert command) {
    command.statusIsSuccess();
    command
        .hasTableSection()
        .hasColumns()
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Member")
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Status")
        .containsExactly("OK", "OK", "OK");
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .hasSize(3);
    return command;
  }

  public CommandResultAssert verifyStatusIsError(CommandResultAssert command) {
    command.statusIsError();
    command
        .hasTableSection()
        .hasColumns()
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Member")
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Status")
        .containsExactly("ERROR", "ERROR", "ERROR");
    return command;
  }

  public CommandResultAssert verifyStatusIsErrorInOneServer(
      CommandResultAssert command) {
    command.statusIsError();
    command
        .hasTableSection()
        .hasColumns()
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Member")
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Status")
        .containsExactlyInAnyOrder("OK", "OK", "ERROR");
    return command;
  }

  public void verifyStatusIsOkInOneServer(
      CommandResultAssert command) {
    command.statusIsError();
    command
        .hasTableSection()
        .hasColumns()
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Member")
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Message")
        .hasSize(3);
    command
        .hasTableSection(ResultModel.MEMBER_STATUS_SECTION)
        .hasColumn("Status")
        .containsExactlyInAnyOrder("OK", "ERROR", "ERROR");
  }

  public void createServersAndRegions(int locatorPort, List<VM> servers,
      boolean usePartitionedRegion, String regionName, String senderId) {

    Properties properties = new Properties();
    for (VM server : servers) {
      server.invoke(() -> createServer(locatorPort, -1, properties));
      if (usePartitionedRegion) {
        server
            .invoke(() -> createPartitionedRegion(regionName, senderId, 1, 100,
                isOffHeap(), RegionShortcut.PARTITION, true, true));
      } else {
        server.invoke(() -> createReplicatedRegion(regionName, senderId,
            Scope.GLOBAL, DataPolicy.REPLICATE,
            isOffHeap(), true, true));
      }
    }
  }

  private void waitForWanCopyRegionCommandToStart(boolean useParallel, boolean usePartitionedRegion,
      List<VM> servers) {
    final int executionsExpected = useParallel && usePartitionedRegion ? servers.size() : 1;
    await().untilAsserted(
        () -> assertThat(getNumberOfCurrentExecutionsInServers(servers))
            .isEqualTo(executionsExpected));
  }

  private void addIgnoredExceptionsForClosingAfterCancelCommand() {
    addIgnoredException(
        "Error closing the connection used to wan-copy region entries");
    addIgnoredException(
        "Exception org.apache.geode.cache.client.internal.pooling.ConnectionDestroyedException in sendBatch. Retrying");
    addIgnoredException(
        "Exception org.apache.geode.cache.client.ServerConnectivityException in sendBatch. Retrying");
  }

  private void addIgnoredExceptionsForSenderInUseWentDown() {
    addIgnoredException(
        "Exception org.apache.geode.cache.client.internal.pooling.ConnectionDestroyedException in sendBatch. Retrying");
    addIgnoredException(
        "Exception org.apache.geode.cache.client.ServerConnectivityException in sendBatch. Retrying");
    addIgnoredException("DistributedSystemDisconnectedException");
    addIgnoredException("org.apache.geode.distributed.PoolCancelledException");
    addIgnoredException(
        "Exception when running wan-copy region command: ");
    addIgnoredException(
        "Exception when running wan-copy region command: java.util.concurrent.ExecutionException: org.apache.geode.cache.EntryDestroyedException");
    addIgnoredException(
        "Error closing the connection used to wan-copy region entries");
  }

  private void addIgnoredExceptionsForReceiverConnectedToSenderInUseWentDown() {
    addIgnoredException(
        "Exception org.apache.geode.cache.client.internal.pooling.ConnectionDestroyedException in sendBatch. Retrying");
    addIgnoredException(
        "Exception org.apache.geode.cache.client.ServerConnectivityException in sendBatch. Retrying");
    addIgnoredException("DistributedSystemDisconnectedException");
  }

  private int getNumberOfCurrentExecutionsInServers(List<VM> vmList) {
    return vmList.stream()
        .map((vm) -> vm.invoke(() -> ((InternalCache) cache)
            .getService(WanCopyRegionFunctionService.class).getNumberOfCurrentExecutions()))
        .reduce(0, Integer::sum);
  }

  private int getCopiedEntries(CommandResultAssert command) throws ParseException {
    List<String> messages = command.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).getActual()
        .getContent().get("Message");
    Pattern numberPattern = Pattern.compile("(.*)(copied: )(.*)");
    int copiedEntries = 0;
    for (String message : messages) {
      Matcher m = numberPattern.matcher(message);
      if (m.find()) {
        copiedEntries +=
            NumberFormat.getIntegerInstance(Locale.getDefault()).parse(m.group(3)).intValue();
      }
    }
    return copiedEntries;
  }
}
