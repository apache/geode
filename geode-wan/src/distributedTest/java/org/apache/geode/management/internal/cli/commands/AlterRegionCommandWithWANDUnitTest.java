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
import static org.apache.geode.lang.Identifiable.find;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.wan.AsyncEventQueueConfigurationException;
import org.apache.geode.internal.cache.wan.GatewaySenderConfigurationException;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(WanTest.class)
public class AlterRegionCommandWithWANDUnitTest {
  private static MemberVM locator;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
    lsRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        "create disk-store --name=diskStore --dir=" + temporaryFolder.getRoot())
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");
  }

  @Test
  public void alterPartitionRegionWithParallelAsynchronousEventQueueShouldPersistTheChangesIntoTheClusterConfigurationService() {
    String regionName = testName.getMethodName();
    String asyncEventQueueName = "asyncEventQueue1";

    gfsh.executeAndAssertThat(
        "create async-event-queue --parallel=true --persistent=false --listener=org.apache.geode.internal.cache.wan.MyAsyncEventListener --id="
            + asyncEventQueueName)
        .statusIsSuccess();
    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers(asyncEventQueueName, 1);

    gfsh.executeAndAssertThat("create region --type=PARTITION --name=" + regionName)
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 1);

    // Associate the async-event-queue
    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --async-event-queue-id=" + asyncEventQueueName)
        .statusIsSuccess().containsOutput("server-1", "OK", "Region " + regionName + " altered");

    // Check the cluster configuration service.
    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      assertThat(internalLocator).isNotNull();
      CacheConfig config =
          internalLocator.getConfigurationPersistenceService().getCacheConfig("cluster");

      RegionConfig regionConfig = find(config.getRegions(), regionName);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getRegionAttributes()).isNotNull();
      assertThat(regionConfig.getRegionAttributes().getAsyncEventQueueIds()).isNotEmpty()
          .isEqualTo(asyncEventQueueName);
    });
  }

  @Test
  public void alterNonColocatedPartitionRegionWithTheSameParallelAsynchronousEventQueueShouldThrowExceptionAndPreventTheClusterConfigurationServiceFromBeingUpdated() {
    addIgnoredException(
        "Non colocated regions (.*) cannot have the same parallel async event queue id (.*) configured.");

    String asyncEventQueue = "asyncEventQueue";
    String region1Name = testName.getMethodName() + "1";
    String region2Name = testName.getMethodName() + "2";

    gfsh.executeAndAssertThat(
        "create async-event-queue --parallel=true --persistent=false --listener=org.apache.geode.internal.cache.wan.MyAsyncEventListener --id="
            + asyncEventQueue)
        .statusIsSuccess();
    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers(asyncEventQueue, 1);

    gfsh.executeAndAssertThat("create region --type=PARTITION --name=" + region1Name)
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region1Name, 1);

    gfsh.executeAndAssertThat("create region --type=PARTITION --name=" + region2Name)
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region2Name, 1);

    // Associate the async-event-queue to both regions (second one should fail because they are not
    // co-located)
    gfsh.executeAndAssertThat(
        "alter region --name=" + region1Name + " --async-event-queue-id=" + asyncEventQueue)
        .statusIsSuccess().containsOutput("server-1", "OK", "Region " + region1Name + " altered");
    gfsh.executeAndAssertThat(
        "alter region --name=" + region2Name + " --async-event-queue-id=" + asyncEventQueue)
        .statusIsError().containsOutput("server-1", "ERROR",
            "Non colocated regions " + SEPARATOR + region2Name + ", " + SEPARATOR + region1Name
                + " cannot have the same parallel async event queue id " + asyncEventQueue
                + " configured.");

    // The exception must be thrown early in the initialization, so the change shouldn't be
    // persisted to the cluster configuration service for the second region.
    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      assertThat(internalLocator).isNotNull();
      CacheConfig config =
          internalLocator.getConfigurationPersistenceService().getCacheConfig("cluster");

      RegionConfig regionConfig = find(config.getRegions(), region1Name);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getRegionAttributes()).isNotNull();
      assertThat(regionConfig.getRegionAttributes().getAsyncEventQueueIds())
          .isEqualTo(asyncEventQueue);

      RegionConfig region2Config = find(config.getRegions(), region2Name);
      assertThat(region2Config).isNotNull();
      assertThat(region2Config.getRegionAttributes()).isNotNull();
      assertThat(region2Config.getRegionAttributes().getAsyncEventQueueIds()).isBlank();
    });
  }

  @Test
  public void alterPartitionPersistentRegionWithParallelNonPersistentAsynchronousEventQueueShouldThrowExceptionAndPreventTheClusterConfigurationServiceFromBeingUpdated() {
    addIgnoredException(
        "Non persistent asynchronous event queue (.*) can not be attached to persistent region (.*)");
    String regionName = testName.getMethodName();
    String asyncEventQueueName = "asyncEventQueue";

    gfsh.executeAndAssertThat(
        "create async-event-queue --parallel=true --persistent=false --listener=org.apache.geode.internal.cache.wan.MyAsyncEventListener --id="
            + asyncEventQueueName)
        .statusIsSuccess();
    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers(asyncEventQueueName, 1);

    gfsh.executeAndAssertThat("create region --type=PARTITION_PERSISTENT --name=" + regionName
        + " --disk-store=diskStore").statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 1);

    // Make sure that the next invocations also fail and that the changes are not persisted to the
    // cluster configuration service. See GEODE-6551.
    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --async-event-queue-id=" + asyncEventQueueName)
        .statusIsError()
        .containsOutput("server-1", "ERROR", "Non persistent asynchronous event queue "
            + asyncEventQueueName + " can not be attached to persistent region " + SEPARATOR
            + regionName);
    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --async-event-queue-id=" + asyncEventQueueName)
        .statusIsError()
        .containsOutput("server-1", "ERROR", "Non persistent asynchronous event queue "
            + asyncEventQueueName + " can not be attached to persistent region " + SEPARATOR
            + regionName);
    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --async-event-queue-id=" + asyncEventQueueName)
        .statusIsError()
        .containsOutput("server-1", "ERROR", "Non persistent asynchronous event queue "
            + asyncEventQueueName + " can not be attached to persistent region " + SEPARATOR
            + regionName);

    // The exception must be thrown early in the initialization, so the change shouldn't be
    // persisted to the cluster configuration service.
    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      assertThat(internalLocator).isNotNull();
      CacheConfig config =
          internalLocator.getConfigurationPersistenceService().getCacheConfig("cluster");

      RegionConfig regionConfig = find(config.getRegions(), regionName);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getRegionAttributes()).isNotNull();
      assertThat(regionConfig.getRegionAttributes().getAsyncEventQueueIds()).isBlank();
    });
  }

  @Test
  public void alterPartitionRegionWithParallelGatewaySenderShouldPersistTheChangesIntoTheClusterConfigurationService() {
    addIgnoredException("could not get remote locator information");
    String regionName = testName.getMethodName();
    String gatewaySenderName = "gatewaySender";

    gfsh.executeAndAssertThat(
        "create gateway-sender --parallel=true --enable-persistence=false --remote-distributed-system-id=2 --id="
            + gatewaySenderName)
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    gfsh.executeAndAssertThat("create region --type=PARTITION --name=" + regionName)
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 1);

    // Associate the gateway-sender
    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --gateway-sender-id=" + gatewaySenderName)
        .statusIsSuccess().containsOutput("server-1", "OK", "Region " + regionName + " altered");

    // Check the cluster configuration service.
    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      assertThat(internalLocator).isNotNull();
      CacheConfig config =
          internalLocator.getConfigurationPersistenceService().getCacheConfig("cluster");

      RegionConfig regionConfig = find(config.getRegions(), regionName);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getRegionAttributes()).isNotNull();
      assertThat(regionConfig.getRegionAttributes().getGatewaySenderIds()).isNotEmpty()
          .isEqualTo(gatewaySenderName);
    });
  }

  @Test
  public void alterNonColocatedPartitionRegionWithTheSameParallelGatewaySenderShouldThrowExceptionAndPreventTheClusterConfigurationServiceFromBeingUpdated() {
    addIgnoredException("could not get remote locator information");
    addIgnoredException(
        "Non colocated regions (.*) cannot have the same parallel gateway sender id (.*) configured.");

    String gatewaySenderName = "gatewaySender";
    String region1Name = testName.getMethodName() + "1";
    String region2Name = testName.getMethodName() + "2";

    gfsh.executeAndAssertThat(
        "create gateway-sender --parallel=true --enable-persistence=false --remote-distributed-system-id=2 --id="
            + gatewaySenderName)
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    gfsh.executeAndAssertThat("create region --type=PARTITION --name=" + region1Name)
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region1Name, 1);

    gfsh.executeAndAssertThat("create region --type=PARTITION --name=" + region2Name)
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region2Name, 1);

    // Associate the gateway-sender to both regions (second one should fail because they are not
    // co-located)
    gfsh.executeAndAssertThat(
        "alter region --name=" + region1Name + " --gateway-sender-id=" + gatewaySenderName)
        .statusIsSuccess().containsOutput("server-1", "OK", "Region " + region1Name + " altered");
    gfsh.executeAndAssertThat(
        "alter region --name=" + region2Name + " --gateway-sender-id=" + gatewaySenderName)
        .statusIsError().containsOutput("server-1", "ERROR",
            "Non colocated regions " + SEPARATOR + region2Name + ", " + SEPARATOR + region1Name
                + " cannot have the same parallel gateway sender id " + gatewaySenderName
                + " configured.");

    // The exception must be thrown early in the initialization, so the change shouldn't be
    // persisted to the cluster configuration service for the second region.
    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      assertThat(internalLocator).isNotNull();
      CacheConfig config =
          internalLocator.getConfigurationPersistenceService().getCacheConfig("cluster");

      RegionConfig regionConfig = find(config.getRegions(), region1Name);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getRegionAttributes()).isNotNull();
      assertThat(regionConfig.getRegionAttributes().getGatewaySenderIds())
          .isEqualTo(gatewaySenderName);

      RegionConfig region2Config = find(config.getRegions(), region2Name);
      assertThat(region2Config).isNotNull();
      assertThat(region2Config.getRegionAttributes()).isNotNull();
      assertThat(region2Config.getRegionAttributes().getGatewaySenderIds()).isBlank();
    });
  }

  @Test
  public void alterPartitionPersistentRegionWithParallelNonPersistentGatewaySenderShouldThrowExceptionAndPreventTheClusterConfigurationServiceFromBeingUpdated() {
    addIgnoredException("could not get remote locator information");
    addIgnoredException(
        "Non persistent gateway sender (.*) can not be attached to persistent region (.*)");
    String regionName = testName.getMethodName();
    String gatewaySenderName = "gatewaySender";

    gfsh.executeAndAssertThat(
        "create gateway-sender --parallel=true --enable-persistence=false --remote-distributed-system-id=2 --id="
            + gatewaySenderName)
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    gfsh.executeAndAssertThat("create region --type=PARTITION_PERSISTENT --name=" + regionName
        + " --disk-store=diskStore").statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 1);

    // Make sure that the next invocations also fail and that the changes are not persisted to the
    // cluster configuration service. See GEODE-6551.
    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --gateway-sender-id=" + gatewaySenderName)
        .statusIsError().containsOutput("server-1", "ERROR", "Non persistent gateway sender "
            + gatewaySenderName + " can not be attached to persistent region " + SEPARATOR
            + regionName);
    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --gateway-sender-id=" + gatewaySenderName)
        .statusIsError().containsOutput("server-1", "ERROR", "Non persistent gateway sender "
            + gatewaySenderName + " can not be attached to persistent region " + SEPARATOR
            + regionName);
    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --gateway-sender-id=" + gatewaySenderName)
        .statusIsError().containsOutput("server-1", "ERROR", "Non persistent gateway sender "
            + gatewaySenderName + " can not be attached to persistent region " + SEPARATOR
            + regionName);

    // The exception must be thrown early in the initialization, so the change shouldn't be
    // persisted to the cluster configuration service.
    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      assertThat(internalLocator).isNotNull();
      CacheConfig config =
          internalLocator.getConfigurationPersistenceService().getCacheConfig("cluster");

      RegionConfig regionConfig = find(config.getRegions(), regionName);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getRegionAttributes()).isNotNull();
      assertThat(regionConfig.getRegionAttributes().getGatewaySenderIds()).isBlank();
    });
  }

  @Test
  public void alterReplicateRegionWithParallelGatewaySenderShouldFailAndDoNotPersistChangesIntoTheClusterConfigurationService() {
    addIgnoredException(GatewaySenderConfigurationException.class);
    addIgnoredException("could not get remote locator information");
    String regionName = testName.getMethodName();
    String gatewaySenderId = testName.getMethodName() + "_parallelGatewaySender";

    CommandStringBuilder createSenderBuilder = new CommandStringBuilder("create gateway-sender")
        .addOption("id", gatewaySenderId)
        .addOption("parallel", "true")
        .addOption("enable-persistence", "false")
        .addOption("remote-distributed-system-id", "2");
    gfsh.executeAndAssertThat(createSenderBuilder.getCommandString())
        .statusIsSuccess().doesNotContainOutput("Did not complete waiting");

    CommandStringBuilder createRegionBuilder = new CommandStringBuilder("create region")
        .addOption("name", regionName)
        .addOption("type", RegionShortcut.REPLICATE.toString());
    gfsh.executeAndAssertThat(createRegionBuilder.getCommandString()).statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 1);

    // Associate the gateway-sender
    CommandStringBuilder alterRegionBuilder = new CommandStringBuilder("alter region")
        .addOption("name", regionName)
        .addOption("gateway-sender-id", gatewaySenderId);
    gfsh.executeAndAssertThat(alterRegionBuilder.getCommandString())
        .statusIsError()
        .containsOutput("server-1", "Parallel Gateway Sender " + gatewaySenderId
            + " can not be used with replicated region " + SEPARATOR + regionName);

    // Check the cluster configuration service.
    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      assertThat(internalLocator).isNotNull();
      CacheConfig config =
          internalLocator.getConfigurationPersistenceService().getCacheConfig("cluster");

      RegionConfig regionConfig = find(config.getRegions(), regionName);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getRegionAttributes()).isNotNull();
      assertThat(regionConfig.getRegionAttributes().getGatewaySenderIds()).isNull();
    });
  }

  @Test
  public void alterReplicateRegionWithParallelAsynchronousEventQueueShouldFailAndDoNotPersistChangesIntoTheClusterConfigurationService() {
    addIgnoredException(AsyncEventQueueConfigurationException.class);
    String regionName = testName.getMethodName();
    String asyncEventQueueName = testName.getMethodName() + "_asyncEventQueue";

    CommandStringBuilder createAsyncQueueBuilder =
        new CommandStringBuilder("create async-event-queue")
            .addOption("id", asyncEventQueueName)
            .addOption("parallel", "true")
            .addOption("persistent", "false")
            .addOption("listener", MyAsyncEventListener.class.getCanonicalName());
    gfsh.executeAndAssertThat(createAsyncQueueBuilder.getCommandString()).statusIsSuccess();
    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers(asyncEventQueueName, 1);

    CommandStringBuilder createRegionBuilder = new CommandStringBuilder("create region")
        .addOption("name", regionName)
        .addOption("type", RegionShortcut.REPLICATE.toString());
    gfsh.executeAndAssertThat(createRegionBuilder.getCommandString()).statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 1);

    // Associate the async-event-queue
    CommandStringBuilder alterRegionBuilder = new CommandStringBuilder("alter region")
        .addOption("name", regionName)
        .addOption("async-event-queue-id", asyncEventQueueName);
    gfsh.executeAndAssertThat(alterRegionBuilder.getCommandString())
        .statusIsError()
        .containsOutput("server-1", "Parallel Async Event Queue " + asyncEventQueueName
            + " can not be used with replicated region " + SEPARATOR + regionName);

    // Check the cluster configuration service.
    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      assertThat(internalLocator).isNotNull();
      CacheConfig config =
          internalLocator.getConfigurationPersistenceService().getCacheConfig("cluster");

      RegionConfig regionConfig = find(config.getRegions(), regionName);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getRegionAttributes()).isNotNull();
      assertThat(regionConfig.getRegionAttributes().getAsyncEventQueueIds()).isNull();
    });
  }
}
