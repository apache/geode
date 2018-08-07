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

package org.apache.geode.test.dunit.rules.tests;

import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collection;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.MemberStarterRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class MemberStarterRuleAwaitDUnitTest {

  @ClassRule
  public static ClusterStartupRule csRule = new ClusterStartupRule(2);

  // Name snooped in server VM below. At time of writing, should be "DEFAULT"
  private static String existingDiskStoreName;
  private static String existingRegionName = "existingRegion";

  private static MemberVM locator, server;
  private MemberVM memberToTest;

  @Before
  public void before() {
    memberToTest = memberTypeToTest.equals("locator") ? locator : server;
  }

  @Parameter
  public String memberTypeToTest;

  @Parameters(name = "{index}: Using {0} VM")
  public static Collection<String> useBothRules() {
    return Arrays.asList("locator", "server");
  }

  @BeforeClass
  public static void beforeClass() {
    locator = csRule.startLocatorVM(0);
    int locatorPort = locator.getPort();
    server = csRule.startServerVM(1, member -> member.withJMXManager()
        .withConnectionToLocator(locatorPort)
        .withRegion(RegionShortcut.PARTITION_PERSISTENT, existingRegionName));

    existingDiskStoreName = server.invoke(() -> {
      DiskStore anExistingDiskStore =
          (DiskStore) ClusterStartupRule.getCache().listDiskStores().toArray()[0];
      return anExistingDiskStore.getName();
    });

    // Override the default 30 second timeout to something lower for the scope of this test
    locator.invoke(() -> MemberStarterRule.setWaitUntilTimeout(3));
    server.invoke(() -> MemberStarterRule.setWaitUntilTimeout(3));
  }

  @Test
  public void nonexistingRegionTimeout() {
    assertThatThrownBy(
        () -> memberToTest.waitUntilRegionIsReadyOnExactlyThisManyServers("nonexistentRegion", 3))
            .hasCauseInstanceOf(ConditionTimeoutException.class)
            .hasStackTraceContaining("Expecting to find an mbean for region 'nonexistentRegion'");
  }

  @Test
  public void existingRegionTimeout() {
    assertThatThrownBy(
        () -> memberToTest.waitUntilRegionIsReadyOnExactlyThisManyServers(existingRegionName, 3))
            .hasCauseInstanceOf(ConditionTimeoutException.class)
            .hasStackTraceContaining(
                "Expecting to find an mbean for region '" + existingRegionName);
  }

  @Test
  public void nonexistingQueueTimeout() {
    assertThatThrownBy(() -> memberToTest
        .waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("badQueueId", 3))
            .hasCauseInstanceOf(ConditionTimeoutException.class)
            .hasStackTraceContaining(
                "Expecting exactly 3 servers to have an AEQ with id 'badQueueId'.");
  }

  @Test
  public void nonexistingDiskStoreTimeout() {
    assertThatThrownBy(
        () -> memberToTest.waitUntilDiskStoreIsReadyOnExactlyThisManyServers("badDiskName", 3))
            .hasCauseInstanceOf(ConditionTimeoutException.class)
            .hasStackTraceContaining(
                "Expecting exactly 3 servers to present mbeans for a disk store with name badDiskName");
  }

  @Test
  public void existingDiskStoreTimeout() {
    assertThatThrownBy(() -> memberToTest
        .waitUntilDiskStoreIsReadyOnExactlyThisManyServers(existingDiskStoreName, 3))
            .hasCauseInstanceOf(ConditionTimeoutException.class)
            .hasStackTraceContaining(
                "Expecting exactly 3 servers to present mbeans for a disk store with name "
                    + existingDiskStoreName);
  }

  @Test
  public void nonexistingGatewayTimeout() {
    assertThatThrownBy(
        () -> memberToTest.waitUntilGatewaySendersAreReadyOnExactlyThisManyServers(3))
            .hasCauseInstanceOf(ConditionTimeoutException.class)
            .hasStackTraceContaining("Expecting to find exactly 3 gateway sender beans");
  }
}
