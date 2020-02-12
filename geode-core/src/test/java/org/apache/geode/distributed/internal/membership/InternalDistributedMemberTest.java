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

import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Unit test for InternalDistributedMember
 */
@Category({MembershipTest.class})
public class InternalDistributedMemberTest {

  @Test
  public void equalsReturnsTrueForSameMember() {
    InternalDistributedMember member = new InternalDistributedMember("", 34567);

    assertThat(member.equals(member)).isTrue();
  }

  @Test
  public void equalsReturnsFalseForNull() {
    InternalDistributedMember member = new InternalDistributedMember("", 34567);

    assertThat(member.equals(null)).isFalse();
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
    when(netMember1.getUniqueTag()).thenReturn("tag1");
    MemberData netMember2 = mock(MemberData.class);
    when(netMember2.getInetAddress()).thenReturn(host1);
    when(netMember2.getUniqueTag()).thenReturn("tag2");
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

  @Test
  public void getDurableClientAttributesShouldReturnDefaultsWhenCachedInstanceIsNullAndDurableIdIsNull()
      throws UnknownHostException {
    InetAddress host1 = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    MemberData memberData = mock(MemberData.class);
    when(memberData.getInetAddress()).thenReturn(host1);
    InternalDistributedMember member = new InternalDistributedMember(memberData);

    assertThat(member.durableClientAttributes).isNull();
    assertThat(member.getDurableClientAttributes()).isNotNull();
    assertThat(member.getDurableClientAttributes().getId()).isEqualTo("");
    assertThat(member.getDurableClientAttributes().getTimeout())
        .isEqualTo(InternalDistributedMember.DEFAULT_DURABLE_CLIENT_TIMEOUT);
  }

  @Test
  public void getDurableClientAttributesShouldReturnDefaultsWhenCachedInstanceIsNullAndDurableIdIsEmpty()
      throws UnknownHostException {
    InetAddress host1 = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    MemberData memberData = mock(MemberData.class);
    when(memberData.getDurableId()).thenReturn("");
    when(memberData.getInetAddress()).thenReturn(host1);
    InternalDistributedMember member = new InternalDistributedMember(memberData);

    assertThat(member.durableClientAttributes).isNull();
    assertThat(member.getDurableClientAttributes()).isNotNull();
    assertThat(member.getDurableClientAttributes().getId()).isEqualTo("");
    assertThat(member.getDurableClientAttributes().getTimeout())
        .isEqualTo(InternalDistributedMember.DEFAULT_DURABLE_CLIENT_TIMEOUT);
  }

  @Test
  public void getDurableClientAttributesShouldReturnCustomAttributesWhenCachedInstanceIsNullAndDurableIdIsNotNull()
      throws UnknownHostException {
    InetAddress host1 = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    MemberData memberData = mock(MemberData.class);
    when(memberData.getInetAddress()).thenReturn(host1);
    when(memberData.getDurableId()).thenReturn("durableId");
    when(memberData.getDurableTimeout()).thenReturn(Integer.MAX_VALUE);
    InternalDistributedMember internalDistributedMember = new InternalDistributedMember(memberData);

    assertThat(internalDistributedMember.durableClientAttributes).isNull();
    assertThat(internalDistributedMember.getDurableClientAttributes()).isNotNull();
    assertThat(internalDistributedMember.getDurableClientAttributes().getId())
        .isEqualTo("durableId");
    assertThat(internalDistributedMember.getDurableClientAttributes().getTimeout())
        .isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void getDurableClientAttributesShouldReturnCachedInstanceWhenBackingMemberIdentifierAttributesHaveNotChanged()
      throws UnknownHostException {
    InetAddress host1 = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    MemberData memberData = mock(MemberData.class);
    when(memberData.getInetAddress()).thenReturn(host1);
    when(memberData.getDurableId()).thenReturn("durableId");
    when(memberData.getDurableTimeout()).thenReturn(Integer.MAX_VALUE);
    InternalDistributedMember internalDistributedMember = new InternalDistributedMember(memberData);
    assertThat(internalDistributedMember.durableClientAttributes).isNull();

    // Get Attributes first time - should be instantiated and cached.
    DurableClientAttributes attributes = internalDistributedMember.getDurableClientAttributes();
    assertThat(attributes).isNotNull();
    assertThat(attributes.getId()).isEqualTo("durableId");
    assertThat(attributes.getTimeout()).isEqualTo(Integer.MAX_VALUE);

    // Get Attributes again without changing backing store - should return cached instance .
    assertThat(internalDistributedMember.getDurableClientAttributes()).isSameAs(attributes);
  }

  @Test
  public void setDurableTimeOutShouldNullifyCachedDurableClientAttributes() {
    InternalDistributedMember member = new InternalDistributedMember("", 34567, "name", "uniqueId",
        MemberIdentifier.NORMAL_DM_TYPE, new String[] {}, new DurableClientAttributes("", 500));
    assertThat(member.durableClientAttributes).isNotNull();

    member.setDurableTimeout(600);
    assertThat(member.durableClientAttributes).isNull();
  }

  @Test
  public void setDurableIdShouldNullifyCachedDurableClientAttributes() {
    InternalDistributedMember member = new InternalDistributedMember("", 34567, "name", "uniqueId",
        MemberIdentifier.NORMAL_DM_TYPE, new String[] {}, new DurableClientAttributes("", 500));
    assertThat(member.durableClientAttributes).isNotNull();

    member.setDurableId("testId");
    assertThat(member.durableClientAttributes).isNull();
  }

  @Test
  public void setMemberDataShouldNullifyCachedDurableClientAttributes() {
    InternalDistributedMember member = new InternalDistributedMember("", 34567, "name", "uniqueId",
        MemberIdentifier.NORMAL_DM_TYPE, new String[] {}, new DurableClientAttributes("", 500));
    assertThat(member.durableClientAttributes).isNotNull();

    member.setMemberData(mock(MemberData.class));
    assertThat(member.durableClientAttributes).isNull();
  }
}
