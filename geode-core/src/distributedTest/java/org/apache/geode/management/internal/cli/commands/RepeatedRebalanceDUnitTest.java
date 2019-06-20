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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class RepeatedRebalanceDUnitTest {
  private static final String PARENT_REGION = "RepeatedRebalanceTestRegion";
  private static final String COLOCATED_REGION_ONE = "RepeatedRebalanceColocatedRegionOne";
  private static final String COLOCATED_REGION_TWO = "RepeatedRebalanceColocatedRegionTwo";
  private static final String PARTITION_RESOLVER =
      "org.apache.geode.management.internal.cli.commands.RepeatedRebalancePartitionResolver";

  private static final int TOTAL_NUM_BUCKETS = 48;
  private static final int NUM_REDUNDANT_COPIES = 2;
  private static final int INITIAL_SERVERS = 4;
  private static final int NUMBER_OF_ENTRIES = 30000;

  private List<MemberVM> memberList = new ArrayList();
  private MemberVM locator1;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();


  @Before
  public void before() throws Exception {

    locator1 = cluster.startLocatorVM(0);

    for (int i = 0; i < INITIAL_SERVERS; ++i) {
      memberList.add(cluster.startServerVM(i + 1, locator1.getPort()));
    }

    gfsh.connectAndVerify(locator1);

    gfsh.executeAndAssertThat("create region --name=" + PARENT_REGION
        + " --type=PARTITION --redundant-copies=" + NUM_REDUNDANT_COPIES
        + " --enable-statistics=true "
        + "--recovery-delay=-1 --startup-recovery-delay=-1 --total-num-buckets=" + TOTAL_NUM_BUCKETS
        + " --partition-resolver=" + PARTITION_RESOLVER)
        .statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=" + COLOCATED_REGION_ONE
        + " --type=PARTITION --redundant-copies=" + NUM_REDUNDANT_COPIES
        + " --enable-statistics=true "
        + "--recovery-delay=-1 --startup-recovery-delay=-1 --total-num-buckets=" + TOTAL_NUM_BUCKETS
        + " --partition-resolver=" + PARTITION_RESOLVER
        + " --colocated-with=" + PARENT_REGION)
        .statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=" + COLOCATED_REGION_TWO
        + " --type=PARTITION --redundant-copies=" + NUM_REDUNDANT_COPIES
        + " --enable-statistics=true "
        + "--recovery-delay=-1 --startup-recovery-delay=-1 --total-num-buckets=" + TOTAL_NUM_BUCKETS
        + " --partition-resolver=" + PARTITION_RESOLVER
        + " --colocated-with=" + COLOCATED_REGION_ONE)
        .statusIsSuccess();
  }

  @After
  public void after() {
    for (int i = 0; i < memberList.size(); i++) {
      memberList.get(i).stop();
    }
  }

  @Test
  public void testSecondRebalanceIsNotNecessaryWithAddedMembers() {

    addDataToRegion(NUMBER_OF_ENTRIES);

    addOrRestartServers(2, 0);

    // Because we have 2 redundant copies and begin with 4 servers, redundancy is already satisfied
    // before this rebalance. As such
    // we expect to see no redundant copies created.
    gfsh.executeAndAssertThat("rebalance").containsOutput(
        "Total bytes in all redundant bucket copies created during this rebalance                        | 0")
        .containsOutput(
            "Total time (in milliseconds) spent creating redundant bucket copies during this rebalance       | 0")
        .containsOutput(
            "Total number of redundant copies created during this rebalance                                  | 0")
        .doesNotContainOutput(
            "Total bytes in buckets moved during this rebalance                                              | 0")
        .doesNotContainOutput(
            "Total time (in milliseconds) spent moving buckets during this rebalance                         | 0")
        .doesNotContainOutput(
            "Total number of buckets moved during this rebalance                                             | 0")
        .doesNotContainOutput(
            "Total time (in milliseconds) spent switching the primary state of buckets during this rebalance | 0")
        .doesNotContainOutput(
            "Total primaries transferred during this rebalance                                               | 0");
    System.out.println("DEBR Finished rebalance one");

    gfsh.executeAndAssertThat("rebalance").containsOutput(
        "Total bytes in all redundant bucket copies created during this rebalance                        | 0")
        .containsOutput(
            "Total time (in milliseconds) spent creating redundant bucket copies during this rebalance       | 0")
        .containsOutput(
            "Total number of redundant copies created during this rebalance                                  | 0")
        .containsOutput(
            "Total bytes in buckets moved during this rebalance                                              | 0")
        .containsOutput(
            "Total time (in milliseconds) spent moving buckets during this rebalance                         | 0")
        .containsOutput(
            "Total number of buckets moved during this rebalance                                             | 0")
        .containsOutput(
            "Total time (in milliseconds) spent switching the primary state of buckets during this rebalance | 0")
        .containsOutput(
            "Total primaries transferred during this rebalance                                               | 0");

    System.out.println("DEBR Finished rebalance two");
  }

  @Test
  public void testSecondRebalanceIsNotNecessaryWithAddedAndRestartedMembers() {

    addDataToRegion(NUMBER_OF_ENTRIES);

    addOrRestartServers(2, 1);

    gfsh.executeAndAssertThat("rebalance").doesNotContainOutput(
        "Total bytes in all redundant bucket copies created during this rebalance                        | 0")
        .doesNotContainOutput(
            "Total time (in milliseconds) spent creating redundant bucket copies during this rebalance       | 0")
        .doesNotContainOutput(
            "Total number of redundant copies created during this rebalance                                  | 0")
        .doesNotContainOutput(
            "Total bytes in buckets moved during this rebalance                                              | 0")
        .doesNotContainOutput(
            "Total time (in milliseconds) spent moving buckets during this rebalance                         | 0")
        .doesNotContainOutput(
            "Total number of buckets moved during this rebalance                                             | 0")
        .doesNotContainOutput(
            "Total time (in milliseconds) spent switching the primary state of buckets during this rebalance | 0")
        .doesNotContainOutput(
            "Total primaries transferred during this rebalance                                               | 0");

    gfsh.executeAndAssertThat("rebalance").containsOutput(
        "Total bytes in all redundant bucket copies created during this rebalance                        | 0")
        .containsOutput(
            "Total time (in milliseconds) spent creating redundant bucket copies during this rebalance       | 0")
        .containsOutput(
            "Total number of redundant copies created during this rebalance                                  | 0")
        .containsOutput(
            "Total bytes in buckets moved during this rebalance                                              | 0")
        .containsOutput(
            "Total time (in milliseconds) spent moving buckets during this rebalance                         | 0")
        .containsOutput(
            "Total number of buckets moved during this rebalance                                             | 0")
        .containsOutput(
            "Total time (in milliseconds) spent switching the primary state of buckets during this rebalance | 0")
        .containsOutput(
            "Total primaries transferred during this rebalance                                               | 0");
  }

  public void addDataToRegion(int entriesToAdd) {
    memberList.get(0).invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      Region<String, String> region = cache.getRegion(PARENT_REGION);
      Region<String, String> colocatedRegionOne = cache.getRegion(COLOCATED_REGION_ONE);
      Region<String, String> colocatedRegionTwo = cache.getRegion(COLOCATED_REGION_TWO);

      for (int i = 0; i < entriesToAdd; i++) {
        region.put("key" + i, "value" + i);
        colocatedRegionOne.put("key" + i, "value" + i);
        colocatedRegionTwo.put("key" + i, "value" + i);
      }
    });
  }

  public void addOrRestartServers(int serversToAdd, int serversToRestart) {
    for (int i = 0; i < serversToAdd; ++i) {
      memberList.add(cluster.startServerVM(INITIAL_SERVERS + i, locator1.getPort()));
    }

    for (int i = 0; i < serversToRestart; ++i) {
      if (i < INITIAL_SERVERS && i < memberList.size()) {
        memberList.get(i).stop(false);
        memberList.remove(i);
        memberList.add(i, cluster.startServerVM(i + 1, locator1.getPort()));
      }
    }
  }
}
