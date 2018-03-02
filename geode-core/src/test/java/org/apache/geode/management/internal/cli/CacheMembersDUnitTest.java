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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class CacheMembersDUnitTest {

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

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
      InternalCache cache = ClusterStartupRule.getCache();
      // can't pass in both group and names
      CacheMembers cacheMembers = ClusterStartupRule::getCache;
      assertThatThrownBy(
          () -> cacheMembers.findMembers("group1".split(","), "member1".split(","), cache))
              .isInstanceOf(UserErrorException.class);

      // finds all servers
      members = cacheMembers.findMembers(null, null, cache);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2", "member3",
          "member4");

      // find only one member
      members = cacheMembers.findMembers(null, "member1".split(","), cache);
      assertThat(getNames(members)).containsExactly("member1");

      // find multiple members
      members = cacheMembers.findMembers(null, "member1,member3".split(","), cache);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member3");

      // find multiple members
      members = cacheMembers.findMembers(null, "MembER1,member3".split(","), cache);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member3");

      // find only one group
      members = cacheMembers.findMembers("group1".split(","), null, cache);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2");

      // find multiple groups
      members = cacheMembers.findMembers("group1,group2".split(","), null, cache);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2", "member3",
          "member4");
    });
  }

  public void getMember() {
    locator.invoke(() -> {
      CacheMembers cacheMembers = ClusterStartupRule::getCache;
      assertThat(cacheMembers.findMember("notValidName", ClusterStartupRule.getCache())).isNull();
      assertThat(cacheMembers.findMember("member1", ClusterStartupRule.getCache()).getName())
          .isEqualTo("member1");
      assertThat(cacheMembers.findMember("MembER1", ClusterStartupRule.getCache()).getName())
          .isEqualTo("member1");
    });
  }

  @Test
  public void getAllMembers() throws Exception {
    locator.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      CacheMembers cacheMembers = ClusterStartupRule::getCache;
      members = cacheMembers.getAllMembers(cache);
      assertThat(getNames(members)).containsExactlyInAnyOrder("locator-0", "member1", "member2",
          "member3", "member4");

      members = cacheMembers.getAllNormalMembers(cache);
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2", "member3",
          "member4");
    });
  }

  @Test
  public void getRegionAssociatedMembers() throws Exception {
    locator.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
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
      InternalCache cache = ClusterStartupRule.getCache();
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
      CacheMembers cacheMembers = ClusterStartupRule::getCache;
      DistributedMember member = cacheMembers.findMember("member1", ClusterStartupRule.getCache());
      assertThat(member.getName()).isEqualTo("member1");
      assertThat(member.getId()).contains("member1:");
      assertThat(member.getGroups()).containsExactly("group1");

      member = cacheMembers.findMember("member100", ClusterStartupRule.getCache());
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
      InternalCache cache2 = ClusterStartupRule.getCache();
      CacheMembers cacheMembers = ClusterStartupRule::getCache;
      Set<DistributedMember> members3 = cacheMembers.findMembers(null, null, cache2);
      members = members3.stream()
          .filter(m2 -> CliUtil.getAsyncEventQueueIds(cache2, m2).contains("queue1"))
          .collect(Collectors.toSet());
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2");

      InternalCache cache1 = ClusterStartupRule.getCache();
      Set<DistributedMember> members2 = cacheMembers.findMembers(null, null, cache1);
      members = members2.stream()
          .filter(m1 -> CliUtil.getAsyncEventQueueIds(cache1, m1).contains("queue2"))
          .collect(Collectors.toSet());
      assertThat(getNames(members)).containsExactlyInAnyOrder("member3", "member4");

      InternalCache cache = ClusterStartupRule.getCache();
      Set<DistributedMember> members1 = cacheMembers.findMembers(null, null, cache);
      members =
          members1.stream().filter(m -> CliUtil.getAsyncEventQueueIds(cache, m).contains("queue"))
              .collect(Collectors.toSet());
      assertThat(getNames(members)).containsExactlyInAnyOrder("member1", "member2", "member3",
          "member4");
    });
  }

  private static Set<String> getNames(Set<DistributedMember> members) {
    return members.stream().map(DistributedMember::getName).collect(Collectors.toSet());
  }
}
