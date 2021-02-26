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

package org.apache.geode.management.internal.cli;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;


public class CliUtilsDUnitTest {

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule(5);

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator;
  private static Set<DistributedMember> members;

  @BeforeClass
  public static void setUpCluster() throws Exception {
    locator = lsRule.startLocatorVM(0);
    gfsh.connectAndVerify(locator);

    Properties properties = new Properties();
    properties.setProperty(ConfigurationProperties.NAME, "member1");
    properties.setProperty(ConfigurationProperties.GROUPS, "group1");
    lsRule.startServerVM(1, properties, locator.getPort());

    properties.setProperty(ConfigurationProperties.NAME, "member2");
    properties.setProperty(ConfigurationProperties.GROUPS, "group1");
    lsRule.startServerVM(2, properties, locator.getPort());

    properties.setProperty(ConfigurationProperties.NAME, "member3");
    properties.setProperty(ConfigurationProperties.GROUPS, "group2");
    lsRule.startServerVM(3, properties, locator.getPort());

    properties.setProperty(ConfigurationProperties.NAME, "member4");
    properties.setProperty(ConfigurationProperties.GROUPS, "group2");
    lsRule.startServerVM(4, properties, locator.getPort());

    // create regions
    gfsh.executeAndAssertThat("create region --name=commonRegion --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=group1Region --group=group1 --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=group2Region --group=group2 --type=REPLICATE")
        .statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + "commonRegion", 4);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + "group1Region", 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + "group2Region", 2);
  }

  @Test
  public void getMembersWithQueueId() throws Exception {
    gfsh.executeAndAssertThat("create async-event-queue --id=queue1 --group=group1 --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();
    gfsh.executeAndAssertThat("create async-event-queue --id=queue2 --group=group2 --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create async-event-queue --id=queue --listener=" + MyAsyncEventListener.class.getName())
        .statusIsSuccess();

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 2);
    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue2", 2);
    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue", 4);

    locator.invoke(() -> {
      members = CliUtils.getMembersWithAsyncEventQueue(ClusterStartupRule.getCache(), "queue1");
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2");

      members = CliUtils.getMembersWithAsyncEventQueue(ClusterStartupRule.getCache(), "queue2");
      assertThat(getNames(members)).containsExactlyInAnyOrder("member3", "member4");

      members = CliUtils.getMembersWithAsyncEventQueue(ClusterStartupRule.getCache(), "queue");
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2", "member3",
          "member4");
    });
  }

  private static Set<String> getNames(Set<DistributedMember> members) {
    return members.stream().map(DistributedMember::getName).collect(Collectors.toSet());
  }
}
