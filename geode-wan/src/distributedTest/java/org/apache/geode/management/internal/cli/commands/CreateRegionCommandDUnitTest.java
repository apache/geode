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
import static org.assertj.core.api.Assertions.assertThat;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({WanTest.class})
@RunWith(GeodeParamsRunner.class)
public class CreateRegionCommandDUnitTest {
  private static MemberVM locator, server1, server2;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, locator.getPort());
    server2 = lsRule.startServerVM(2, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  public Object[] replicatedRegionAndScopePairs() {
    String[] replicatedRegions = {"REPLICATE", "REPLICATE_PERSISTENT", "REPLICATE_PROXY",
        "REPLICATE_OVERFLOW", "REPLICATE_PERSISTENT_OVERFLOW", "REPLICATE_HEAP_LRU"};
    String[] scopes = {"DISTRIBUTED_NO_ACK", "DISTRIBUTED_ACK", "LOCAL", "GLOBAL"};
    Object[] pairs = new Object[replicatedRegions.length * scopes.length];
    int count = 0;
    for (String replicatedRegion : replicatedRegions) {
      for (String scope : scopes) {
        pairs[count] = new Object[] {replicatedRegion, scope};
        count++;
      }
    }
    return pairs;
  }

  public Object[] nonReplicatedRegionAndScopePairs() {
    String[] scopes = {"DISTRIBUTED_NO_ACK", "DISTRIBUTED_ACK", "LOCAL", "GLOBAL"};
    String[] nonReplicatedRegions =
        {"PARTITION", "PARTITION_PERSISTENT", "PARTITION_PROXY", "PARTITION_REDUNDANT",
            "PARTITION_REDUNDANT_PERSISTENT", "PARTITION_OVERFLOW", "PARTITION_REDUNDANT_OVERFLOW",
            "PARTITION_PERSISTENT_OVERFLOW", "PARTITION_REDUNDANT_PERSISTENT_OVERFLOW",
            "PARTITION_HEAP_LRU", "PARTITION_REDUNDANT_HEAP_LRU", "LOCAL", "LOCAL_PERSISTENT",
            "LOCAL_HEAP_LRU", "LOCAL_OVERFLOW", "LOCAL_PERSISTENT_OVERFLOW"};
    int count = 0;
    Object[] pairs = new Object[scopes.length * nonReplicatedRegions.length];
    for (String nonReplicatedRegion : nonReplicatedRegions) {
      for (String scope : scopes) {
        pairs[count] = new Object[] {nonReplicatedRegion, scope};
        count++;
      }
    }
    return pairs;
  }

  @Test
  @TestCaseName("[{index}] {method}(RegionType:{0},Scope:{1})")
  @Parameters(method = "replicatedRegionAndScopePairs")
  public void createReplicateRegionCommandWithScopeSetsTheScope(String regionType, String scope) {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --type=" + regionType + " --scope=" + scope + " --name=" + regionName)
        .statusIsSuccess();
    String describeOutputScope = scope.replace("_", "-").toLowerCase();
    gfsh.executeAndAssertThat("describe region --name=" + regionName).statusIsSuccess()
        .containsOutput(describeOutputScope);
  }

  @Test
  @TestCaseName("[{index}] {method}(RegionType:{0},Scope:{1})")
  @Parameters(method = "nonReplicatedRegionAndScopePairs")
  public void createNonReplicatedRegionCommandWithScopeShouldFail(String regionType, String scope) {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --type=" + regionType + " --scope=" + scope + " --name=" + regionName)
        .statusIsError();
  }

  @Test
  public void createReplicatedRegionWithParallelAsynchronousEventQueueShouldThrowExceptionAndPreventTheRegionFromBeingCreated() {
    String regionName = testName.getMethodName();
    String asyncQueueName = "asyncEventQueue";

    gfsh.executeAndAssertThat(
        "create async-event-queue --parallel=true --listener=org.apache.geode.internal.cache.wan.MyAsyncEventListener --id="
            + asyncQueueName)
        .statusIsSuccess();
    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers(asyncQueueName, 2);

    gfsh.executeAndAssertThat("create region --type=REPLICATE  --name=" + regionName
        + " --async-event-queue-id=" + asyncQueueName)
        .statusIsError()
        .containsOutput("server-1",
            "Parallel Async Event Queue " + asyncQueueName
                + " can not be used with replicated region " + SEPARATOR + regionName)
        .containsOutput("server-2", "Parallel Async Event Queue " + asyncQueueName
            + " can not be used with replicated region " + SEPARATOR + regionName);

    // The exception must be thrown early in the initialization, so the region itself shouldn't be
    // added to the root regions.
    gfsh.executeAndAssertThat("list regions").statusIsSuccess().doesNotContainOutput(regionName);
  }

  @Test
  public void createReplicatedRegionWithParallelGatewaySenderShouldThrowExceptionAndPreventTheRegionFromBeingCreated() {
    String regionName = testName.getMethodName();
    String gatewaySenderName = "gatewaySender";
    IgnoredException.addIgnoredException("could not get remote locator information");

    gfsh.executeAndAssertThat(
        "create gateway-sender --parallel=true --remote-distributed-system-id=2 --id="
            + gatewaySenderName)
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    gfsh.executeAndAssertThat("create region --type=REPLICATE  --name=" + regionName
        + " --gateway-sender-id=" + gatewaySenderName)
        .statusIsError()
        .containsOutput("server-1",
            "Parallel Gateway Sender " + gatewaySenderName
                + " can not be used with replicated region " + SEPARATOR + regionName)
        .containsOutput("server-2", "Parallel Gateway Sender " + gatewaySenderName
            + " can not be used with replicated region " + SEPARATOR + regionName);

    // The exception must be thrown early in the initialization, so the region itself shouldn't be
    // added to the root regions.
    gfsh.executeAndAssertThat("list regions").statusIsSuccess().doesNotContainOutput(regionName);
  }

  @Test
  public void cannotCreateRegionIfGatewaySenderDoesNotExist() {
    String regionName = testName.getMethodName();
    String gatewaySenderName = "gatewaySender";
    IgnoredException.addIgnoredException("could not get remote locator information");

    gfsh.executeAndAssertThat(
        "create gateway-sender --remote-distributed-system-id=2 --id="
            + gatewaySenderName)
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    gfsh.executeAndAssertThat("create region --type=REPLICATE  --name=" + regionName
        + " --gateway-sender-id=" + gatewaySenderName + "-2")
        .statusIsError()
        .containsOutput("Specify valid gateway-sender-id");

    // The exception must be thrown early in the initialization, so the region itself shouldn't be
    // added to the root regions.
    gfsh.executeAndAssertThat("list regions").statusIsSuccess().doesNotContainOutput(regionName);
  }

  /**
   * Ignored this test until we refactor the FetchRegionAttributesFunction to not use
   * AttributesFactory, and instead use RegionConfig, which we will do as part of implementing
   * GEODE-6104
   */
  @Ignore
  @Test
  public void createRegionFromTemplateWithGatewaySender() throws Exception {
    String regionName = testName.getMethodName();
    String sender = "sender1";
    String remoteDS = "2";
    IgnoredException.addIgnoredException("could not get remote locator information");

    gfsh.executeAndAssertThat("create gateway-sender"
        + " --id=" + sender
        + " --remote-distributed-system-id=" + remoteDS)
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");

    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=REPLICATE_PERSISTENT"
        + " --gateway-sender-id=" + sender).statusIsSuccess();

    String regionNameFromTemplate = regionName + "-from-template";
    gfsh.executeAndAssertThat("create region --name=" + regionNameFromTemplate
        + " --template-region=" + regionName)
        .statusIsSuccess();

    server1.invoke(() -> {
      Region region1 = ClusterStartupRule.getCache().getRegion(regionNameFromTemplate);
      assertThat(region1.getAttributes().getGatewaySenderIds())
          .describedAs("region1 contains gateway sender")
          .contains(sender);

      Region region2 = ClusterStartupRule.getCache().getRegion(regionNameFromTemplate);
      assertThat(region2.getAttributes().getGatewaySenderIds())
          .describedAs("region2 contains gateway sender")
          .contains(sender);
    });
  }
}
