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

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.management.internal.cli.exceptions.UserErrorException;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class CliUtilDUnitTest {

  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

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

    locator.waitTillRegionsAreReadyOnServers("/commonRegion", 4);
    locator.waitTillRegionsAreReadyOnServers("/group1Region", 2);
    locator.waitTillRegionsAreReadyOnServers("/group2Region", 2);
  }

  @Test
  public void findMembers() throws Exception {
    locator.invoke(() -> {
      // can't pass in both group and names
      assertThatThrownBy(() -> CliUtil.findMembers("group1".split(","), "member1".split(",")))
          .isInstanceOf(UserErrorException.class);

      // finds all servers
      members = CliUtil.findMembers(null, null);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2", "member3",
          "member4");

      // find only one member
      members = CliUtil.findMembers(null, "member1".split(","));
      assertThat(getNames(members)).containsExactly("member1");

      // find multiple members
      members = CliUtil.findMembers(null, "member1,member3".split(","));
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member3");

      // find multiple members
      members = CliUtil.findMembers(null, "MembER1,member3".split(","));
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member3");

      // find only one group
      members = CliUtil.findMembers("group1".split(","), null);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2");

      // find multiple groups
      members = CliUtil.findMembers("group1,group2".split(","), null);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2", "member3",
          "member4");
    });
  }

  public void getMember() throws Exception {
    locator.invoke(() -> {
      assertThat(CliUtil.getDistributedMemberByNameOrId("notValidName")).isNull();
      assertThat(CliUtil.getDistributedMemberByNameOrId("member1").getName()).isEqualTo("member1");
      assertThat(CliUtil.getDistributedMemberByNameOrId("MembER1").getName()).isEqualTo("member1");
    });
  }

  @Test
  public void getAllMembers() throws Exception {
    locator.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.getCache();
      members = CliUtil.getAllMembers(cache);
      assertThat(getNames(members)).containsExactlyInAnyOrder("locator-0", "member1", "member2",
          "member3", "member4");

      members = CliUtil.getAllNormalMembers(cache);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2", "member3",
          "member4");
    });
  }

  @Test
  public void getRegionAssociatedMembers() throws Exception {
    locator.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.getCache();
      members = CliUtil.getRegionAssociatedMembers("commonRegion", cache, true);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2", "member3",
          "member4");

      members = CliUtil.getRegionAssociatedMembers("group1Region", cache, true);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2");
    });
  }

  @Test
  public void getRegionsAssociatedMembers() throws Exception {
    locator.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.getCache();
      // this finds the members that host both these two regions
      Set<String> regions =
          Arrays.stream("commonRegion,group1Region".split(",")).collect(Collectors.toSet());
      members = CliUtil.getQueryRegionsAssociatedMembers(regions, cache, true);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2");

      regions = Arrays.stream("group1Region,group2Region".split(",")).collect(Collectors.toSet());
      members = CliUtil.getQueryRegionsAssociatedMembers(regions, cache, true);
      assertThat(getNames(members)).isEmpty();
    });
  }

  @Test
  public void getMemberByNameOrId() throws Exception {
    locator.invoke(() -> {
      DistributedMember member = CliUtil.getDistributedMemberByNameOrId("member1");
      assertThat(member.getName()).isEqualTo("member1");
      assertThat(member.getId()).contains("member1:");
      assertThat(member.getGroups()).containsExactly("group1");

      member = CliUtil.getDistributedMemberByNameOrId("member100");
      assertThat(member).isNull();
    });
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

    locator.waitTillAsyncEventQueuesAreReadyOnServers("queue1", 2);
    locator.waitTillAsyncEventQueuesAreReadyOnServers("queue2", 2);
    locator.waitTillAsyncEventQueuesAreReadyOnServers("queue", 4);

    locator.invoke(() -> {
      members =
          CliUtil.getMembersWithAsyncEventQueue(LocatorServerStartupRule.getCache(), "queue1");
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2");

      members =
          CliUtil.getMembersWithAsyncEventQueue(LocatorServerStartupRule.getCache(), "queue2");
      assertThat(getNames(members)).containsExactlyInAnyOrder("member3", "member4");

      members = CliUtil.getMembersWithAsyncEventQueue(LocatorServerStartupRule.getCache(), "queue");
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2", "member3",
          "member4");
    });
  }

  private static Set<String> getNames(Set<DistributedMember> members) {
    return members.stream().map(DistributedMember::getName).collect(Collectors.toSet());
  }
}
