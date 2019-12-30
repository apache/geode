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
package org.apache.geode.distributed.internal.membership;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.gms.api.MemberData;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Unit test for InternalDistributedMember
 */
@Category({MembershipTest.class})
public class InternalDistributedMemberTest {

  @Test
  public void equalsReturnsTrueForSameMember() {
    InternalDistributedMember member = new InternalDistributedMember("", 34567);

    boolean result = member.equals(member);

    assertThat(result).isTrue();
  }

  @Test
  public void equalsReturnsFalseForNull() {
    InternalDistributedMember member = new InternalDistributedMember("", 34567);

    boolean result = member.equals(null);

    assertThat(result).isFalse();
  }

  @Test
  public void equalsReturnsFalseForNonInternalDistributedMember() {
    InternalDistributedMember member = new InternalDistributedMember("", 34567);

    boolean result = member.equals(new Object());

    assertThat(result).isFalse();
  }

  @Test
  public void equalsReturnsFalseIfPortsDiffer() {
    InternalDistributedMember member = new InternalDistributedMember("", 34567);
    InternalDistributedMember other = new InternalDistributedMember("", 34568);

    boolean result = member.equals(other);

    assertThat(result).isFalse();
  }

  @Test
  public void equalsReturnsTrueIfNullInetAddress() throws UnknownHostException {
    InetAddress localHost = InetAddress.getLocalHost();
    MemberData netMember1 = mock(MemberData.class);
    when(netMember1.getInetAddress()).thenReturn(localHost).thenReturn(null);
    MemberData netMember2 = mock(MemberData.class);
    when(netMember2.getInetAddress()).thenReturn(localHost).thenReturn(null);
    InternalDistributedMember member = new InternalDistributedMember(netMember1);
    InternalDistributedMember other = new InternalDistributedMember(netMember2);

    boolean result = member.equals(other);

    assertThat(result).isTrue();
  }

  @Test
  public void equalsReturnsFalseIfInetAddressDiffer() throws UnknownHostException {
    InetAddress host1 = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    InetAddress host2 = InetAddress.getByAddress(new byte[] {127, 0, 0, 2});
    MemberData netMember1 = mock(MemberData.class);
    when(netMember1.getInetAddress()).thenReturn(host1);
    MemberData netMember2 = mock(MemberData.class);
    when(netMember2.getInetAddress()).thenReturn(host2);
    InternalDistributedMember member = new InternalDistributedMember(netMember1);
    InternalDistributedMember other = new InternalDistributedMember(netMember2);

    boolean result = member.equals(other);

    assertThat(result).isFalse();
  }

  @Test
  public void equalsReturnsFalseIfGetViewIdDiffer() throws UnknownHostException {
    InetAddress host1 = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    MemberData netMember1 = mock(MemberData.class);
    when(netMember1.getInetAddress()).thenReturn(host1);
    MemberData netMember2 = mock(MemberData.class);
    when(netMember2.getInetAddress()).thenReturn(host1);
    when(netMember1.getVmViewId()).thenReturn(1);
    when(netMember2.getVmViewId()).thenReturn(2);
    InternalDistributedMember member = new InternalDistributedMember(netMember1);
    InternalDistributedMember other = new InternalDistributedMember(netMember2);

    boolean result = member.equals(other);

    assertThat(result).isFalse();
  }

  @Test
  public void equalsReturnsFalseIfUniqueTagsDiffer() throws UnknownHostException {
    InetAddress host1 = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    MemberData netMember1 = mock(MemberData.class);
    when(netMember1.getInetAddress()).thenReturn(host1);
    MemberData netMember2 = mock(MemberData.class);
    when(netMember2.getInetAddress()).thenReturn(host1);
    InternalDistributedMember member = new InternalDistributedMember(netMember1);
    member.setUniqueTag("tag1");
    InternalDistributedMember other = new InternalDistributedMember(netMember2);
    other.setUniqueTag("tag2");

    boolean result = member.equals(other);

    assertThat(result).isFalse();
  }

  @Test
  public void equalsReturnsFalseIfNotPartialAndNamesDiffer() throws UnknownHostException {
    InetAddress host1 = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    MemberData netMember1 = mock(MemberData.class);
    when(netMember1.getInetAddress()).thenReturn(host1);
    when(netMember1.getName()).thenReturn("name1");
    MemberData netMember2 = mock(MemberData.class);
    when(netMember2.getInetAddress()).thenReturn(host1);
    when(netMember2.getName()).thenReturn("name2");
    InternalDistributedMember member = new InternalDistributedMember(netMember1);
    member.setIsPartial(false);
    InternalDistributedMember other = new InternalDistributedMember(netMember2);
    other.setIsPartial(false);

    boolean result = member.equals(other);

    assertThat(result).isFalse();
  }

  @Test
  public void equalsReturnsFalseIfCompareAdditionalDataDiffer() throws UnknownHostException {
    InetAddress host1 = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    MemberData netMember1 = mock(MemberData.class);
    when(netMember1.getInetAddress()).thenReturn(host1);
    MemberData netMember2 = mock(MemberData.class);
    when(netMember2.getInetAddress()).thenReturn(host1);
    when(netMember1.compareAdditionalData(netMember2)).thenReturn(1);
    when(netMember2.compareAdditionalData(netMember1)).thenReturn(-1);
    InternalDistributedMember member = new InternalDistributedMember(netMember1);
    InternalDistributedMember other = new InternalDistributedMember(netMember2);

    boolean result = member.equals(other);

    assertThat(result).isFalse();
  }

  @Test
  public void equalsReturnsTrueForTwoMembersOnSamePort() {
    InternalDistributedMember member = new InternalDistributedMember("", 34567);
    InternalDistributedMember other = new InternalDistributedMember("", 34567);

    boolean result = member.equals(other);

    assertThat(result).isTrue();
  }

}
