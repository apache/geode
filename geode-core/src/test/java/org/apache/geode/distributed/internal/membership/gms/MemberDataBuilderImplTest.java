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

import org.junit.Test;

import org.apache.geode.distributed.internal.membership.gms.api.MemberData;
import org.apache.geode.distributed.internal.membership.gms.api.MemberDataBuilder;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.Version;

public class MemberDataBuilderImplTest {

  @Test
  public void testNewBuilder() throws UnknownHostException {
    InetAddress localhost = SocketCreator.getLocalHost();
    MemberData data = MemberDataBuilder.newBuilder(localhost, localhost.getHostName()).build();
    assertThat(data.getInetAddress()).isEqualTo(localhost);
    assertThat(data.getHostName()).isEqualTo(localhost.getHostName());
  }

  @Test
  public void testNewBuilderForLocalHost() throws UnknownHostException {
    InetAddress localhost = SocketCreator.getLocalHost();
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname").build();
    assertThat(data.getInetAddress()).isEqualTo(localhost);
    assertThat(data.getHostName()).isEqualTo("hostname");
  }

  @Test
  public void testSetMembershipPort() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setMembershipPort(1234).build();
    assertThat(data.getMembershipPort()).isEqualTo(1234);
  }

  @Test
  public void testSetDirectPort() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setDirectChannelPort(1234).build();
    assertThat(data.getDirectChannelPort()).isEqualTo(1234);
  }

  @Test
  public void testSetVmPid() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setVmPid(1234).build();
    assertThat(data.getVmPid()).isEqualTo(1234);
  }

  @Test
  public void testSetVmKind() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setVmKind(12).build();
    assertThat(data.getVmKind()).isEqualTo((byte) 12);
  }

  @Test
  public void testSetVmViewId() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setVmViewId(1234).build();
    assertThat(data.getVmViewId()).isEqualTo(1234);
  }

  @Test
  public void testSetName() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setName("carlos").build();
    assertThat(data.getName()).isEqualTo("carlos");
  }

  @Test
  public void testSetGroups() {
    String[] groups = new String[] {"group1", "group2"};
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setGroups(groups).build();
    assertThat(data.getGroups()).isEqualTo(groups);
  }

  @Test
  public void testSetDurableId() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setDurableId("myId").build();
    assertThat(data.getDurableId()).isEqualTo("myId");
  }

  @Test
  public void testSetDurableTimeout() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setDurableTimeout(1234).build();
    assertThat(data.getDurableTimeout()).isEqualTo(1234);
  }

  @Test
  public void testSetPreferredForCoordinator() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setPreferredForCoordinator(false).build();
    MemberData data2 = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setPreferredForCoordinator(true).build();
    assertThat(data.isPreferredForCoordinator()).isEqualTo(false);
    assertThat(data2.isPreferredForCoordinator()).isEqualTo(true);
  }

  @Test
  public void testSetNetworkPartitionDetectionEnabled() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setNetworkPartitionDetectionEnabled(false).build();
    MemberData data2 = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setNetworkPartitionDetectionEnabled(true).build();
    assertThat(data.isNetworkPartitionDetectionEnabled()).isEqualTo(false);
    assertThat(data2.isNetworkPartitionDetectionEnabled()).isEqualTo(true);
  }

  @Test
  public void testSetVersionOrdinal() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setVersionOrdinal(Version.CURRENT_ORDINAL).build();
    assertThat(data.getVersionOrdinal()).isEqualTo(Version.CURRENT_ORDINAL);
  }

  @Test
  public void testSetUuidMostSignificantBits() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setUuidMostSignificantBits(1234).build();
    assertThat(data.getUuidMostSignificantBits()).isEqualTo(1234);
  }

  @Test
  public void testSetUuidLeastSignificantBits() {
    MemberData data = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setUuidLeastSignificantBits(1234).build();
    assertThat(data.getUuidLeastSignificantBits()).isEqualTo(1234);
  }
}
