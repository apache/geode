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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({DistributedTest.class, WanTest.class})
public class CreateRegionCommandDUnitTest {
  private static MemberVM locator, server1, server2;

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @BeforeClass
  public static void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, locator.getPort());
    server2 = lsRule.startServerVM(2, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void createReplicatedRegionWithParallelAsynchronousEventQueueShouldThrowExceptionAndPreventTheRegionFromBeingCreated() {
    String regionName = testName.getMethodName();
    String asyncQueueName = "asyncEventQueue";

    gfsh.executeAndAssertThat(
        "create async-event-queue --parallel=true --listener=org.apache.geode.internal.cache.wan.MyAsyncEventListener --id="
            + asyncQueueName)
        .statusIsSuccess();
    locator.waitTillAsyncEventQueuesAreReadyOnServers(asyncQueueName, 2);

    gfsh.executeAndAssertThat("create region --type=REPLICATE  --name=" + regionName
        + " --async-event-queue-id=" + asyncQueueName)
        .statusIsError()
        .containsOutput("server-1",
            "ERROR: Parallel Async Event Queue " + asyncQueueName
                + " can not be used with replicated region /" + regionName)
        .containsOutput("server-2", "ERROR: Parallel Async Event Queue " + asyncQueueName
            + " can not be used with replicated region /" + regionName);

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
        .statusIsSuccess();
    locator.waitTilGatewaySendersAreReady(2);

    gfsh.executeAndAssertThat("create region --type=REPLICATE  --name=" + regionName
        + " --gateway-sender-id=" + gatewaySenderName)
        .statusIsError()
        .containsOutput("server-1",
            "ERROR: Parallel gateway sender " + gatewaySenderName
                + " can not be used with replicated region /" + regionName)
        .containsOutput("server-2", "ERROR: Parallel gateway sender " + gatewaySenderName
            + " can not be used with replicated region /" + regionName);

    // The exception must be thrown early in the initialization, so the region itself shouldn't be
    // added to the root regions.
    gfsh.executeAndAssertThat("list regions").statusIsSuccess().doesNotContainOutput(regionName);
  }
}
