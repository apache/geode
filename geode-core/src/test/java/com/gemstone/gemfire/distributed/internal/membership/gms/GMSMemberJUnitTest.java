/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal.membership.gms;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;

import org.jgroups.util.UUID;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.membership.MemberAttributes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GMSMemberJUnitTest {

  @Test
  public void testEqualsNotSameType() {
    GMSMember member = new GMSMember();
    Assert.assertFalse(member.equals("Not a GMSMember"));
  }
  
  @Test
  public void testEqualsIsSame() {
    GMSMember member = new GMSMember();
    Assert.assertTrue(member.equals(member));
  }
  
  @Test
  public void testCompareToIsSame() {
    GMSMember member = new GMSMember();
    UUID uuid = new UUID(0, 0);
    member.setUUID(uuid);
    Assert.assertEquals(0, member.compareTo(member));
  }
  
  private GMSMember createGMSMember(byte[] inetAddress, int viewId, long msb, long lsb) {
    GMSMember member = new GMSMember();
    InetAddress addr1 = mock(InetAddress.class);
    when(addr1.getAddress()).thenReturn(inetAddress);
    member.setInetAddr(addr1);
    member.setBirthViewId(viewId);
    member.setUUID(new UUID(msb, lsb));
    return member;
  }
  
  @Test
  public void testCompareToInetAddressIsLongerThan() {
    GMSMember member1 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1, 1, 1, 1}, 1, 1, 1);
    Assert.assertEquals(1, member1.compareTo(member2));
  }
  
  @Test
  public void testCompareToInetAddressIsShorterThan() {
    GMSMember member1 = createGMSMember(new byte[] {1, 1, 1, 1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    Assert.assertEquals(-1, member1.compareTo(member2));
  }
  
  @Test
  public void testCompareToInetAddressIsGreater() {
    GMSMember member1 = createGMSMember(new byte[] {1, 2, 1, 1, 1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    Assert.assertEquals(1, member1.compareTo(member2));
  }
  
  @Test
  public void testCompareToInetAddressIsLessThan() {
    GMSMember member1 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1, 2, 1, 1, 1}, 1, 1, 1);
    Assert.assertEquals(-1, member1.compareTo(member2));
  }
  
  @Test
  public void testCompareToMyViewIdLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 2, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1}, 1, 1, 1);
    Assert.assertEquals(1, member1.compareTo(member2));
  }
  
  @Test
  public void testCompareToTheirViewIdLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1}, 2, 1, 1);
    Assert.assertEquals(-1, member1.compareTo(member2));
  }
  
  @Test
  public void testCompareToMyMSBLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 1, 2, 1);
    GMSMember member2 = createGMSMember(new byte[] {1}, 1, 1, 1);
    Assert.assertEquals(1, member1.compareTo(member2));
  }

  @Test
  public void testCompareToTheirMSBLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1}, 1, 2, 1);
    Assert.assertEquals(-1, member1.compareTo(member2));
  }

  @Test
  public void testCompareToMyLSBLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 1, 1, 2);
    GMSMember member2 = createGMSMember(new byte[] {1}, 1, 1, 1);
    Assert.assertEquals(1, member1.compareTo(member2));
  }
  
  @Test
  public void testCompareToTheirLSBLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1}, 1, 1, 2);
    Assert.assertEquals(-1, member1.compareTo(member2));
  }

  
  //Makes sure a NPE is not thrown
  @Test
  public void testNoNPEWhenSetAttributesWithNull() {
    GMSMember member = new GMSMember();
    member.setAttributes(null);
    MemberAttributes attrs = member.getAttributes(); 
    MemberAttributes invalid = MemberAttributes.INVALID;
    Assert.assertEquals(attrs.getVmKind(), invalid.getVmKind());
    Assert.assertEquals(attrs.getPort(), invalid.getPort());
    Assert.assertEquals(attrs.getVmViewId(), invalid.getVmViewId());
    Assert.assertEquals(attrs.getName(), invalid.getName());
  }
  
  @Test
  public void testGetUUIDReturnsNullWhenUUIDIs0() {
    GMSMember member = new GMSMember();
    UUID uuid = new UUID(0, 0);
    member.setUUID(uuid);
    Assert.assertNull(member.getUUID());
  }
  
  @Test
  public void testGetUUID() {
    GMSMember member = new GMSMember();
    UUID uuid = new UUID(1, 1);
    member.setUUID(uuid);
    Assert.assertNotNull(member.getUUID());
  }
}
