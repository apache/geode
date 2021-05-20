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

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.management.internal.i18n.CliStrings.REPLICATE_REGION;
import static org.apache.geode.management.internal.i18n.CliStrings.REPLICATE_REGION__BATCHSIZE;
import static org.apache.geode.management.internal.i18n.CliStrings.REPLICATE_REGION__CANCEL;
import static org.apache.geode.management.internal.i18n.CliStrings.REPLICATE_REGION__MAXRATE;
import static org.apache.geode.management.internal.i18n.CliStrings.REPLICATE_REGION__MSG__CANCELED__AFTER__HAVING__REPLICATED;
import static org.apache.geode.management.internal.i18n.CliStrings.REPLICATE_REGION__MSG__EXECUTION__CANCELED;
import static org.apache.geode.management.internal.i18n.CliStrings.REPLICATE_REGION__MSG__NO__RUNNING__COMMAND;
import static org.apache.geode.management.internal.i18n.CliStrings.REPLICATE_REGION__REGION;
import static org.apache.geode.management.internal.i18n.CliStrings.REPLICATE_REGION__SENDERID;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.FutureTask;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorLauncherStartupRule;

@Category({WanTest.class})
public class ReplicateRegionCommandDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public ReplicateRegionCommandDUnitTest() {
    super();
  }

  @Test
  public void testUnsuccessfulReplicateRegionCommandInvocation_RegionNotFound() throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    Integer senderLocatorPort = create3WanSitesAndClient(true, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB);

    int replicateRegionBatchSize = 20;
    String regionName = "foo";

    // Execute replicate region command
    GfshCommandRule gfsh = new GfshCommandRule();
    gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
    String command = new CommandStringBuilder(REPLICATE_REGION)
        .addOption(REPLICATE_REGION__REGION, regionName)
        .addOption(REPLICATE_REGION__SENDERID, senderIdInA)
        .addOption(REPLICATE_REGION__BATCHSIZE, String.valueOf(replicateRegionBatchSize))
        .getCommandString();

    // Check command status and output
    CommandResultAssert replicateRegionCommand = gfsh.executeAndAssertThat(command).statusIsError();
    replicateRegionCommand.hasTableSection().hasColumns().hasSize(3);
    replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Member")
        .hasSize(3);
    replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Status")
        .containsExactly("ERROR", "ERROR", "ERROR");
    replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
        .hasSize(3);
    String message =
        CliStrings.format(CliStrings.REPLICATE_REGION__MSG__REGION__NOT__FOUND,
            Region.SEPARATOR + regionName);
    replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
        .containsExactly(message, message, message);
  }

  @Test
  public void testUnsuccessfulReplicateRegionCommandInvocation_SenderNotFound() throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    Integer senderLocatorPort = create3WanSitesAndClient(true, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB);

    int replicateRegionBatchSize = 20;
    String regionName = getRegionName(true);

    // Execute replicate region command
    GfshCommandRule gfsh = new GfshCommandRule();
    gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
    String command = new CommandStringBuilder(REPLICATE_REGION)
        .addOption(REPLICATE_REGION__REGION, regionName)
        .addOption(REPLICATE_REGION__SENDERID, senderIdInA)
        .addOption(REPLICATE_REGION__BATCHSIZE, String.valueOf(replicateRegionBatchSize))
        .getCommandString();

    // Check command status and output
    CommandResultAssert replicateRegionCommand = gfsh.executeAndAssertThat(command).statusIsError();
    replicateRegionCommand.hasTableSection().hasColumns().hasSize(3);
    replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Member")
        .hasSize(3);
    replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Status")
        .containsExactly("ERROR", "ERROR", "ERROR");
    replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
        .hasSize(3);
    String message =
        CliStrings.format(CliStrings.REPLICATE_REGION__MSG__SENDER__NOT__FOUND, senderIdInA);
    replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
        .containsExactly(message, message, message);
  }

  @Test
  public void testSuccessfulReplicateRegionCommandInvocation_WithReplicatedRegionAndSerialGatewaySender()
      throws Exception {
    testSuccessfulReplicateRegionCommandInvocation(false, false);
  }

  @Test
  public void testSuccessfulReplicateRegionCommandInvocation_WithPartitionedRegionAndSerialGatewaySender()
      throws Exception {
    testSuccessfulReplicateRegionCommandInvocation(true, false);
  }

  @Test
  public void testSuccessfulReplicateRegionCommandInvocation_WithPartitionedRegionAndParallelGatewaySender()
      throws Exception {
    testSuccessfulReplicateRegionCommandInvocation(true, true);
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
   * The "replicate region" command is run from "A" site passing sender "B".
   *
   * It must be verified that the entries are replicated to site "B".
   * It must also be verified that the entries are not transitively
   * replicated to "C" even though replication is configured from "B" to "C"
   * because with this command generateCallbacks is set to false in the
   * events generated.
   *
   */
  public void testSuccessfulReplicateRegionCommandInvocation(boolean isPartitionedRegion,
      boolean isParallelGatewaySender) throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    Integer senderLocatorPort = create3WanSitesAndClient(isPartitionedRegion, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB);

    int replicateRegionBatchSize = 20;
    int entries = 100;
    String regionName = getRegionName(isPartitionedRegion);
    // Put entries
    client.invoke(() -> WANTestBase.doClientPutsFrom(regionName, 0, entries));

    // Check that entries are put in the region
    for (VM member : serversInA) {
      member.invoke(() -> WANTestBase.validateRegionSize(regionName, entries));
    }

    // Check that entries are not replicated to "B" nor "C"
    serverInB.invoke(() -> WANTestBase.validateRegionSize(regionName, 0));
    serverInC.invoke(() -> WANTestBase.validateRegionSize(regionName, 0));

    // Create senders and receivers with replication as follows: "A" -> "B" -> "C"
    createSenders(isParallelGatewaySender, serversInA, serverInB,
        senderIdInA, senderIdInB);
    createReceivers(serverInB, serverInC);

    // Check that entries are not replicated to "B" nor "C"
    serverInB.invoke(() -> WANTestBase.validateRegionSize(regionName, 0));
    serverInC.invoke(() -> WANTestBase.validateRegionSize(regionName, 0));

    // Execute replicate region command
    GfshCommandRule gfsh = new GfshCommandRule();
    gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
    String command = new CommandStringBuilder(REPLICATE_REGION)
        .addOption(REPLICATE_REGION__REGION, regionName)
        .addOption(REPLICATE_REGION__SENDERID, senderIdInA)
        .addOption(REPLICATE_REGION__BATCHSIZE, String.valueOf(replicateRegionBatchSize))
        .getCommandString();

    // Check command status and output
    CommandResultAssert replicateRegionCommand =
        gfsh.executeAndAssertThat(command).statusIsSuccess();
    replicateRegionCommand.hasTableSection().hasColumns().hasSize(3);
    replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Member")
        .hasSize(3);
    replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Status")
        .containsExactly("OK", "OK", "OK");
    replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
        .hasSize(3);
    if (isPartitionedRegion && isParallelGatewaySender) {
      String msg1 = CliStrings.format(CliStrings.REPLICATE_REGION__MSG__REPLICATED__ENTRIES, 33);
      String msg2 = CliStrings.format(CliStrings.REPLICATE_REGION__MSG__REPLICATED__ENTRIES, 34);
      replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
          .containsExactlyInAnyOrder(msg1, msg1, msg2);
    } else {
      String msg1 = CliStrings.format(CliStrings.REPLICATE_REGION__MSG__REPLICATED__ENTRIES, 100);
      String msg2 = CliStrings.REPLICATE_REGION__MSG__SENDER__SERIAL__AND__NOT__PRIMARY;
      replicateRegionCommand.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
          .containsExactlyInAnyOrder(msg1, msg2, msg2);
    }

    // Check that entries are replicated in "B"
    serverInB.invoke(() -> WANTestBase.validateRegionSize(regionName, entries));

    // Check that replicateRegionBatchSize is correctly used by the command
    int receivedBatches = serverInB.invoke(() -> WANTestBase.getReceiverStats().get(2));
    if (isPartitionedRegion && isParallelGatewaySender) {
      assertThat(receivedBatches).isEqualTo(6);
    } else {
      assertThat(receivedBatches).isEqualTo(5);
    }

    // Check that entries are not replicated in "C" (generateCallbacks is false)
    serverInC.invoke(() -> WANTestBase.validateRegionSize(regionName, 0));
  }

  @Test
  public void testSuccessfulCancelReplicateRegionCommandInvocation_WithReplicatedRegionAndSerialGatewaySender()
      throws Exception {
    testSuccessfulCancelReplicateRegionCommandInvocation(false, false);
  }

  @Test
  public void testSuccessfulCancelReplicateRegionCommandInvocation_WithPartitionedRegionAndSerialGatewaySender()
      throws Exception {
    testSuccessfulCancelReplicateRegionCommandInvocation(true, false);
  }

  @Test
  public void testSuccessfulCancelReplicateRegionCommandInvocation_WithPartitionedRegionAndParallelGatewaySender()
      throws Exception {
    testSuccessfulCancelReplicateRegionCommandInvocation(true, true);
  }

  @Test
  public void testUnsuccessfulCancelReplicateRegionCommandInvocation_WithReplicatedRegionAndSerialGatewaySender()
      throws Exception {
    testUnsuccessfulCancelReplicateRegionCommandInvocation(false, false);
  }

  @Test
  public void testUnsuccessfulCancelReplicateRegionCommandInvocation_WithPartitionedRegionAndSerialGatewaySender()
      throws Exception {
    testUnsuccessfulCancelReplicateRegionCommandInvocation(true, false);
  }

  @Test
  public void testUnsuccessfulCancelReplicateRegionCommandInvocation_WithPartitionedRegionAndParallelGatewaySender()
      throws Exception {
    testUnsuccessfulCancelReplicateRegionCommandInvocation(true, true);
  }

  public void testUnsuccessfulCancelReplicateRegionCommandInvocation(boolean isPartitionedRegion,
      boolean isParallelGatewaySender) throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    Integer senderLocatorPort = create3WanSitesAndClient(isPartitionedRegion, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB);

    String regionName = getRegionName(isPartitionedRegion);

    // Create senders and receivers with replication as follows: "A" -> "B" -> "C"
    createSenders(isParallelGatewaySender, serversInA, serverInB,
        senderIdInA, senderIdInB);
    createReceivers(serverInB, serverInC);

    // Execute cancel replicate region command
    GfshCommandRule gfsh = new GfshCommandRule();
    gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
    String cancelCommand = new CommandStringBuilder(REPLICATE_REGION)
        .addOption(REPLICATE_REGION__REGION, regionName)
        .addOption(REPLICATE_REGION__SENDERID, senderIdInA)
        .addOption(REPLICATE_REGION__CANCEL)
        .getCommandString();
    CommandResultAssert cancelCommandResult =
        gfsh.executeAndAssertThat(cancelCommand).statusIsError();

    // Check cancel command output
    cancelCommandResult.hasTableSection().hasColumns().hasSize(3);
    cancelCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Member")
        .hasSize(3);
    cancelCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
        .hasSize(3);
    cancelCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Status")
        .containsExactly("ERROR", "ERROR", "ERROR");
    String msg1 = CliStrings.format(REPLICATE_REGION__MSG__NO__RUNNING__COMMAND,
        Region.SEPARATOR + regionName, senderIdInA);
    cancelCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
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
   * The "replicate region" command is run from "A" site passing sender "B"
   * in a different thread. The maxRate is set to a very low value so that there is
   * time to cancel it before it finishes.
   *
   * The "replicate region" command with the cancel option
   * is run from "A" site passing sender "B".
   *
   * It must be verified that the command is canceled.
   * Also, the output of the replicate region command must show
   * the number of entries replicated before the command was canceled.
   */
  public void testSuccessfulCancelReplicateRegionCommandInvocation(boolean isPartitionedRegion,
      boolean isParallelGatewaySender) throws Exception {
    List<VM> serversInA = Arrays.asList(vm5, vm6, vm7);
    VM serverInB = vm3;
    VM serverInC = vm4;
    VM client = vm8;
    String senderIdInA = "B";
    String senderIdInB = "C";

    Integer senderLocatorPort = create3WanSitesAndClient(isPartitionedRegion, vm0,
        vm1, vm2, serversInA, serverInB, serverInC, client,
        senderIdInA, senderIdInB);

    int replicateRegionBatchSize = 20;
    int entries = 100;
    String regionName = getRegionName(isPartitionedRegion);
    // Put entries
    client.invoke(() -> WANTestBase.doClientPutsFrom(regionName, 0, entries));

    // Create senders and receivers with replication as follows: "A" -> "B" -> "C"
    createSenders(isParallelGatewaySender, serversInA, serverInB,
        senderIdInA, senderIdInB);
    createReceivers(serverInB, serverInC);

    // Execute replicate region command to be canceled in an independent thread
    FutureTask<CommandResultAssert> replicateCommandFuture =
        new FutureTask<>(() -> {
          String command = new CommandStringBuilder(REPLICATE_REGION)
              .addOption(REPLICATE_REGION__REGION, regionName)
              .addOption(REPLICATE_REGION__SENDERID, senderIdInA)
              .addOption(REPLICATE_REGION__BATCHSIZE, String.valueOf(replicateRegionBatchSize))
              .addOption(REPLICATE_REGION__MAXRATE, String.valueOf(1))
              .getCommandString();
          GfshCommandRule gfsh = new GfshCommandRule();
          try {
            gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
          } catch (Exception e) {
            e.printStackTrace();
          }
          return gfsh.executeAndAssertThat(command).statusIsSuccess();
        });
    LoggingExecutors.newSingleThreadExecutor(getTestMethodName(), true)
        .submit(replicateCommandFuture);

    // Sleep a bit to make sure the replication has started.
    Thread.sleep(5000);

    // Cancel replicate region command
    GfshCommandRule gfsh = new GfshCommandRule();
    gfsh.connectAndVerify(senderLocatorPort, GfshCommandRule.PortType.locator);
    String cancelCommand = new CommandStringBuilder(REPLICATE_REGION)
        .addOption(REPLICATE_REGION__REGION, regionName)
        .addOption(REPLICATE_REGION__SENDERID, senderIdInA)
        .addOption(REPLICATE_REGION__CANCEL)
        .getCommandString();
    CommandResultAssert cancelCommandResult =
        gfsh.executeAndAssertThat(cancelCommand).statusIsSuccess();

    // Check cancel command output
    cancelCommandResult.hasTableSection().hasColumns().hasSize(3);
    cancelCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Member")
        .hasSize(3);
    cancelCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
        .hasSize(3);
    if (isPartitionedRegion && isParallelGatewaySender) {
      cancelCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Status")
          .containsExactly("OK", "OK", "OK");
      cancelCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
          .containsExactlyInAnyOrder(REPLICATE_REGION__MSG__EXECUTION__CANCELED,
              REPLICATE_REGION__MSG__EXECUTION__CANCELED,
              REPLICATE_REGION__MSG__EXECUTION__CANCELED);
    } else {
      cancelCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Status")
          .containsExactlyInAnyOrder("OK", "ERROR", "ERROR");
      String msg1 = CliStrings.format(REPLICATE_REGION__MSG__NO__RUNNING__COMMAND,
          Region.SEPARATOR + regionName, senderIdInA);
      cancelCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
          .containsExactlyInAnyOrder(msg1, msg1, REPLICATE_REGION__MSG__EXECUTION__CANCELED);
    }

    // Check replicate region command output
    CommandResultAssert replicateCommandResult = replicateCommandFuture.get();
    replicateCommandResult.hasTableSection().hasColumns().hasSize(3);
    replicateCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Member")
        .hasSize(3);
    replicateCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Status")
        .containsExactly("OK", "OK", "OK");
    replicateCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
        .hasSize(3);
    if (isPartitionedRegion && isParallelGatewaySender) {
      String msg = CliStrings.format(REPLICATE_REGION__MSG__CANCELED__AFTER__HAVING__REPLICATED,
          replicateRegionBatchSize);
      replicateCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
          .containsExactly(msg, msg, msg);
    } else {
      String msg1 =
          CliStrings.format(CliStrings.REPLICATE_REGION__MSG__CANCELED__AFTER__HAVING__REPLICATED,
              replicateRegionBatchSize);
      String msg2 = CliStrings.REPLICATE_REGION__MSG__SENDER__SERIAL__AND__NOT__PRIMARY;
      replicateCommandResult.hasTableSection(ResultModel.MEMBER_STATUS_SECTION).hasColumn("Message")
          .containsExactlyInAnyOrder(msg1, msg2, msg2);
    }
  }

  private void createSenders(boolean isParallelGatewaySender, List<VM> serversInA,
      VM serverInB, String senderIdInA, String senderIdInB) {
    serverInB.invoke(() -> WANTestBase.createSender(senderIdInB, 3,
        isParallelGatewaySender, 100, 10, false,
        false, null, false));
    for (VM server : serversInA) {
      server.invoke(() -> WANTestBase.createSender(senderIdInA, 2, isParallelGatewaySender,
          100, 10, false,
          false, null, true));
    }
    startSenderInVMsAsync(senderIdInA, (VM[]) serversInA.toArray());
  }

  private void createReceivers(VM serverInB, VM serverInC) {
    createReceiverInVMs(serverInB);
    createReceiverInVMs(serverInC);
  }

  private Integer create3WanSitesAndClient(boolean isPartitionedRegion, VM locatorSender,
      VM locatorSenderReceiver, VM locatorReceiver, List<VM> serversInA, VM serverInB,
      VM serverInC, VM client, String senderIdInA, String senderIdInB) {
    // Create locators
    Integer receiverLocatorPort =
        locatorReceiver.invoke(() -> WANTestBase.createFirstLocatorWithDSId(3));
    Integer senderReceiverLocatorPort = locatorSenderReceiver
        .invoke(() -> WANTestBase.createFirstRemoteLocator(2, receiverLocatorPort));
    Integer senderLocatorPort = locatorSender.invoke(() -> {
      Properties props = getDistributedSystemProperties();
      props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
      props.setProperty(REMOTE_LOCATORS, "localhost[" + senderReceiverLocatorPort + "]");
      LocatorLauncherStartupRule launcherStartupRule =
          new LocatorLauncherStartupRule().withProperties(props);
      launcherStartupRule.start();
      return launcherStartupRule.getLauncher().getPort();
    });

    // Create servers
    serverInB.invoke(() -> WANTestBase.createServer(senderReceiverLocatorPort));
    serverInC.invoke(() -> WANTestBase.createServer(receiverLocatorPort));
    for (VM server : serversInA) {
      server.invoke(() -> WANTestBase.createServer(senderLocatorPort));
    }

    // Create region in servers
    final String regionName = getRegionName(isPartitionedRegion);
    if (isPartitionedRegion) {
      for (VM server : serversInA) {
        server
            .invoke(() -> WANTestBase.createPartitionedRegion(regionName, senderIdInA, 1, 100,
                isOffHeap()));
      }
      serverInB.invoke(
          () -> WANTestBase.createPartitionedRegion(regionName, senderIdInB, 0, 100,
              isOffHeap()));
      serverInC.invoke(() -> WANTestBase.createPartitionedRegion(regionName, null, 0, 100,
          isOffHeap()));
    } else {
      for (VM server : serversInA) {
        server.invoke(() -> WANTestBase.createReplicatedRegion(regionName, senderIdInA,
            Scope.GLOBAL, DataPolicy.REPLICATE,
            isOffHeap()));
      }
      serverInB
          .invoke(() -> WANTestBase.createReplicatedRegion(regionName, senderIdInB,
              Scope.GLOBAL, DataPolicy.REPLICATE,
              isOffHeap()));
      serverInC.invoke(() -> WANTestBase.createReplicatedRegion(regionName, null,
          Scope.GLOBAL, DataPolicy.REPLICATE,
          isOffHeap()));
    }

    // Create client
    client.invoke(() -> WANTestBase.createClientWithLocator(senderLocatorPort, "localhost",
        regionName, ClientRegionShortcut.PROXY));

    return senderLocatorPort;
  }

  private String getRegionName(boolean isPartitionedRegion) {
    return getTestMethodName() + (isPartitionedRegion ? "_PR" : "RR");
  }
}
