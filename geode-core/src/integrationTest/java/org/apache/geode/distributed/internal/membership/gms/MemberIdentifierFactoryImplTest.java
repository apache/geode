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

package org.apache.geode.distributed.internal.membership.gms;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.Version;

public class MemberIdentifierFactoryImplTest {

  private MemberIdentifierFactoryImpl factory;

  @Before
  public void setup() {
    factory = new MemberIdentifierFactoryImpl();
  }

  @Test
  public void testRemoteHost() throws UnknownHostException {
    InetAddress localhost = LocalHostUtil.getLocalHost();
    MemberData memberData =
        MemberDataBuilder.newBuilder(localhost, localhost.getHostName()).build();
    MemberIdentifier data = factory.create(memberData);
    assertThat(data.getInetAddress()).isEqualTo(localhost);
    assertThat(data.getHostName()).isEqualTo(localhost.getHostName());
  }

  @Test
  public void testNewBuilderForLocalHost() throws UnknownHostException {
    InetAddress localhost = LocalHostUtil.getLocalHost();
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("hostname").build();
    MemberIdentifier data = factory.create(memberData);
    assertThat(data.getInetAddress()).isEqualTo(localhost);
    assertThat(data.getHostName()).isEqualTo("hostname");
  }

  @Test
  public void testSetMembershipPort() {
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setMembershipPort(1234).build();
    MemberIdentifier data = factory.create(memberData);
    assertThat(data.getMembershipPort()).isEqualTo(1234);
  }

  @Test
  public void testSetVmKind() {
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setVmKind(12).build();
    MemberIdentifier data = factory.create(memberData);
    assertThat(data.getVmKind()).isEqualTo((byte) 12);
  }

  @Test
  public void testSetVmViewId() {
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setVmViewId(1234).build();
    MemberIdentifier data = factory.create(memberData);
    assertThat(data.getVmViewId()).isEqualTo(1234);
  }

  @Test
  public void testSetGroups() {
    String[] groups = new String[] {"group1", "group2"};
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setGroups(groups).build();
    MemberIdentifier data = factory.create(memberData);
    assertThat(data.getGroups()).isEqualTo(Arrays.asList(groups));
  }

  @Test
  public void testSetDurableId() {
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setDurableId("myId").build();
    MemberIdentifier data = factory.create(memberData);
    assertThat(data.getMemberData().getDurableId()).isEqualTo("myId");
  }

  @Test
  public void testSetVersionOrdinal() {
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setVersionOrdinal(Version.CURRENT_ORDINAL).build();
    MemberIdentifier data = factory.create(memberData);
    assertThat(data.getVersionOrdinal()).isEqualTo(Version.CURRENT_ORDINAL);
  }

  @Test
  public void membersAreEqual() {
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setMembershipPort(1).build();
    InternalDistributedMember member1 = factory.create(memberData);
    memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setMembershipPort(1).build();
    InternalDistributedMember member2 = factory.create(memberData);
    assertThat(factory.getComparator().compare(member1, member2)).isZero();
  }

  @Test
  public void membersAreNotEqual() {
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setMembershipPort(1).build();
    InternalDistributedMember member1 = factory.create(memberData);
    memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setMembershipPort(2).build();
    InternalDistributedMember member2 = factory.create(memberData);
    assertThat(factory.getComparator().compare(member1, member2)).isLessThan(0);
    assertThat(factory.getComparator().compare(member2, member1)).isGreaterThan(0);
  }
}
