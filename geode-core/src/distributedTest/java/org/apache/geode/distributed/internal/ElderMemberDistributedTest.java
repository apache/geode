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
package org.apache.geode.distributed.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ConcurrencyRule;

public class ElderMemberDistributedTest {
  public List<MemberVM> locators = new ArrayList<>();

  @Rule
  public ConcurrencyRule concurrencyRule = new ConcurrencyRule();

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Before
  public void before() {
    Properties properties = new Properties();
    properties.setProperty(ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION, "false");
    properties.setProperty(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");
    locators.add(clusterStartupRule.startLocatorVM(0, properties));
    locators.add(clusterStartupRule.startLocatorVM(1, properties, locators.get(0).getPort()));
    locators.add(clusterStartupRule.startLocatorVM(2, properties, locators.get(0).getPort()));
  }

  @After
  public void after() {
    locators.clear();
  }

  @Test
  public void oldestMemberIsElder() {
    final InternalDistributedMember elderId = locators.get(0).invoke(
        ElderMemberDistributedTest::assertIsElderAndGetId);

    locators.get(1).invoke(() -> elderConsistencyCheck(elderId));

    locators.get(2).invoke(() -> elderConsistencyCheck(elderId));

    clusterStartupRule.crashVM(0);

    final InternalDistributedMember newElderId = locators.get(1).invoke(
        ElderMemberDistributedTest::assertIsElderAndGetId);
    locators.get(2).invoke(() -> elderConsistencyCheck(newElderId));
  }

  @Test
  public void elderIsCorrectAfterKillingMultipleLocators() throws InterruptedException {
    locators.add(clusterStartupRule.startLocatorVM(3, locators.get(0).getPort()));

    // check that all members agree on elder
    final InternalDistributedMember elderId =
        locators.get(0).invoke(ElderMemberDistributedTest::assertIsElderAndGetId);
    for (int i = 1; i < locators.size(); i++) {
      locators.get(i).invoke(() -> elderConsistencyCheck(elderId));
    }

    // kill several members at the same time
    concurrencyRule.add(() -> {
      clusterStartupRule.stop(0);

      return null;
    });

    concurrencyRule.add(() -> {
      clusterStartupRule.stop(1);
      return null;
    });

    concurrencyRule.executeInParallel();

    InternalDistributedMember newElderId =
        locators.get(2).invoke(ElderMemberDistributedTest::assertIsElderAndGetId);
    assertThat(newElderId).isNotEqualByComparingTo(elderId);
    locators.get(3).invoke(() -> elderConsistencyCheck(newElderId));
  }

  private static InternalDistributedMember assertIsElderAndGetId() {
    DistributionManager distributionManager =
        ClusterStartupRule.getCache().getInternalDistributedSystem().getDistributionManager();
    await()
        .untilAsserted(() -> assertThat(distributionManager.isElder()).isTrue());
    return distributionManager.getElderId();
  }

  private static void elderConsistencyCheck(InternalDistributedMember elderId) {
    ClusterDistributionManager distributionManager =
        (ClusterDistributionManager) ClusterStartupRule.getCache().getInternalDistributedSystem()
            .getDistributionManager();
    assertThat(distributionManager.isElder()).isFalse();
    await("Choosing elder stopped for too long")
        .until(() -> {
          distributionManager.waitForElder(elderId);
          return true;
        });
    assertThat(distributionManager.getElderId()).isEqualTo(elderId);
  }
}
