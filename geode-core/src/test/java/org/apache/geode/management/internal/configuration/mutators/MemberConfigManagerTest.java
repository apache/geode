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
package org.apache.geode.management.internal.configuration.mutators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.internal.cli.domain.CacheServerInfo;
import org.apache.geode.management.internal.cli.domain.MemberInformation;

public class MemberConfigManagerTest {
  private MemberConfigManager memberConfigManager;
  private DistributionManager distributionManager;
  private MembershipManager membershipManager;
  private DistributedMember coordinator;
  private CacheServerInfo cacheServerInfo;
  private MemberConfig filter;
  private InternalDistributedMember internalDistributedMemberMatch;
  private InternalDistributedMember internalDistributedMember;

  @Before
  public void setUp() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    memberConfigManager = new MemberConfigManager(cache);
    distributionManager = mock(DistributionManager.class);
    membershipManager = mock(MembershipManager.class);
    coordinator = mock(DistributedMember.class);
    cacheServerInfo = mock(CacheServerInfo.class);
    filter = new MemberConfig();
    internalDistributedMember = mock(InternalDistributedMember.class);
    internalDistributedMemberMatch = mock(InternalDistributedMember.class);

    when(internalDistributedMember.getName()).thenReturn("no-match");
    when(cache.getDistributionManager()).thenReturn(distributionManager);
  }

  @Test
  public void getDistributedMembersNoFilter() {
    Set<InternalDistributedMember> internalDistributedMembers = new HashSet<>();
    internalDistributedMembers.add(internalDistributedMember);
    internalDistributedMembers.add(internalDistributedMember);
    when(distributionManager.getDistributionManagerIds()).thenReturn(internalDistributedMembers);

    assertThat(memberConfigManager.getDistributedMembers(filter).size())
        .isEqualTo(internalDistributedMembers.size());
  }

  @Test
  public void getDistributedMembersReturnsNothing() {
    filter.setId("some-id-no-one-else-has");
    Set<InternalDistributedMember> internalDistributedMembers = new HashSet<>();

    internalDistributedMembers.add(internalDistributedMember);
    internalDistributedMembers.add(internalDistributedMember);
    when(distributionManager.getDistributionManagerIds()).thenReturn(internalDistributedMembers);

    assertThat(memberConfigManager.getDistributedMembers(filter).size())
        .isEqualTo(0);
  }

  @Test
  public void getDistributedMembersReturnsSomeMatches() {
    String sharedId = "shared-id";
    filter.setId(sharedId);
    Set<InternalDistributedMember> internalDistributedMembers = new HashSet<>();
    when(internalDistributedMemberMatch.getName()).thenReturn(sharedId);
    internalDistributedMembers.add(internalDistributedMemberMatch);
    internalDistributedMembers.add(internalDistributedMember);
    when(distributionManager.getDistributionManagerIds()).thenReturn(internalDistributedMembers);

    assertThat(memberConfigManager.getDistributedMembers(filter).size())
        .isEqualTo(1);
  }

  @Test
  public void generatesMemberConfigs() {
    ArrayList<MemberInformation> emptyArrayList = new ArrayList<>();
    assertThat(memberConfigManager.generateMemberConfigs(emptyArrayList)).isNotNull();

    ArrayList<MemberInformation> smallArrayList = new ArrayList<>();
    MemberInformation someMemberInfo = new MemberInformation();
    someMemberInfo.setGroups("hello");
    someMemberInfo.setId("world");
    smallArrayList.add(someMemberInfo);
    MemberInformation someOtherMemberInfo = new MemberInformation();
    someOtherMemberInfo.setId("hello");
    someOtherMemberInfo.setGroups("world");
    smallArrayList.add(someOtherMemberInfo);
    assertThat(memberConfigManager.generateMemberConfigs(smallArrayList).size())
        .isEqualTo(smallArrayList.size());
  }

  @Test
  public void generateMemberConfig() {
    MemberInformation memberInformation = new MemberInformation();
    memberInformation.setId("some-id");
    String someName = "some-name";
    memberInformation.setName(someName);
    String someHost = "some-host";
    memberInformation.setHost(someHost);
    int somePid = 7;
    memberInformation.setProcessId(somePid);
    String someStatus = "some-status";
    memberInformation.setStatus(someStatus);
    long someInitHeapSize = 234L;
    memberInformation.setInitHeapSize(someInitHeapSize);
    long someMaxHeapSize = 523L;
    memberInformation.setMaxHeapSize(someMaxHeapSize);
    long someHeapUsage = 123L;
    memberInformation.setHeapUsage(someHeapUsage);
    String somePath = "somePath";
    memberInformation.setLogFilePath(somePath);
    memberInformation.setWorkingDirPath(somePath);

    MemberConfig memberConfig =
        memberConfigManager.generateMemberConfig("coordinatorId", memberInformation);
    assertThat(memberConfig.getId()).isEqualTo(someName);
    assertThat(memberConfig.getHost()).isEqualTo(someHost);
    assertThat(memberConfig.getPid()).isEqualTo(somePid);
    assertThat(memberConfig.getStatus()).isEqualTo(someStatus);
    assertThat(memberConfig.getInitialHeap()).isEqualTo(someInitHeapSize);
    assertThat(memberConfig.getMaxHeap()).isEqualTo(someMaxHeapSize);
    assertThat(memberConfig.getUsedHeap()).isEqualTo(someHeapUsage);
    assertThat(memberConfig.getLogFile()).isEqualTo(somePath);
    assertThat(memberConfig.getWorkingDirectory()).isEqualTo(somePath);
    assertThat(memberConfig.isCoordinator()).isFalse();
  }

  @Test
  public void generateServerMemberConfig() {
    int somePort = 5000;
    when(cacheServerInfo.getPort()).thenReturn(somePort);
    int someConnectionNumber = 10;
    when(cacheServerInfo.getMaxConnections()).thenReturn(someConnectionNumber);
    int someThreadNumber = 5;
    when(cacheServerInfo.getMaxThreads()).thenReturn(someThreadNumber);

    MemberInformation memberInformation = new MemberInformation();
    memberInformation.addCacheServerInfo(cacheServerInfo);
    memberInformation.setServer(true);
    String someGroup1 = "something1";
    String someGroup2 = "something2";
    memberInformation.setGroups(someGroup1 + "," + someGroup2);
    String memberId = "memberId";
    memberInformation.setId(memberId);

    String coordinatorId = "coordinatorId";
    MemberConfig memberConfig =
        memberConfigManager.generateMemberConfig(coordinatorId, memberInformation);

    assertThat(memberConfig.isLocator()).isFalse();
    assertThat(memberConfig.isCoordinator()).isFalse();
    MemberConfig.CacheServerConfig cacheServerConfig = memberConfig.getCacheServers().get(0);
    assertThat(cacheServerConfig).isNotNull();
    assertThat(cacheServerConfig.getPort()).isEqualTo(somePort);
    assertThat(cacheServerConfig.getMaxConnections()).isEqualTo(someConnectionNumber);
    assertThat(cacheServerConfig.getMaxThreads()).isEqualTo(someThreadNumber);
    assertThat(memberConfig.getGroups()).contains(someGroup1, someGroup2);
  }

  @Test
  public void generateLocatorMemberConfig() {
    MemberInformation memberInformation = new MemberInformation();
    int someLocatorPort = 180;
    memberInformation.setLocatorPort(someLocatorPort);
    String memberId = "memberId";
    memberInformation.setId(memberId);

    String coordinatorId = "coordinatorId";
    MemberConfig memberConfig =
        memberConfigManager.generateMemberConfig(coordinatorId, memberInformation);
    assertThat(memberConfig.getPort()).isEqualTo(someLocatorPort);
    assertThat(memberConfig.isLocator()).isTrue();
    assertThat(memberConfig.isCoordinator()).isFalse();
  }


  @Test
  public void generateCoordinatorMemberConfig() {
    MemberInformation memberInformation = new MemberInformation();
    String coordinatorId = "coordinatorId";
    memberInformation.setId(coordinatorId);

    MemberConfig memberConfig =
        memberConfigManager.generateMemberConfig(coordinatorId, memberInformation);
    assertThat(memberConfig.isCoordinator()).isTrue();
  }

  @Test
  public void getsCoordinatorId() {
    when(distributionManager.getMembershipManager()).thenReturn(membershipManager);
    when(membershipManager.getCoordinator()).thenReturn(coordinator);
    String coordinatorId = "some-id";
    when(coordinator.getId()).thenReturn(coordinatorId);

    assertThat(memberConfigManager.getCoordinatorId()).isEqualTo(coordinatorId);
  }

  @Test
  public void getCoordinatorIdReturnsNullWhenMembershipManagerIsNull() {
    when(distributionManager.getMembershipManager()).thenReturn(null);

    assertThat(memberConfigManager.getCoordinatorId()).isNull();
  }

  @Test
  public void getCoordinatorIdReturnsNullWhenCoordinatorIsNull() {
    when(distributionManager.getMembershipManager()).thenReturn(membershipManager);
    when(membershipManager.getCoordinator()).thenReturn(null);

    assertThat(memberConfigManager.getCoordinatorId()).isNull();
  }
}
