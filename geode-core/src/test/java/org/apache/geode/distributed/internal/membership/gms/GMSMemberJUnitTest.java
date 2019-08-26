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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.net.InetAddress;

import org.jgroups.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class GMSMemberJUnitTest {

  @Test
  public void testEqualsNotSameType() {
    GMSMember member = new GMSMember();
    assertFalse(member.equals("Not a GMSMember"));
  }

  @Test
  public void testEqualsIsSame() {
    GMSMember member = new GMSMember();
    assertTrue(member.equals(member));
  }

  @Test
  public void testCompareToIsSame() {
    GMSMember member = new GMSMember();
    UUID uuid = new UUID(0, 0);
    member.setUUID(uuid);
    assertEquals(0, member.compareTo(member));
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
    assertEquals(1, member1.compareTo(member2));
  }

  @Test
  public void testShallowMemberEquals() {
    GMSMember member1 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    GMSMember member2 =
        new GMSMember(member1.getInetAddress(), member1.getPort(), member1.getVersionOrdinal(),
            member1.getUuidMSBs(), member1.getUuidLSBs(), member1.getVmViewId());
    assertEquals(0, member1.compareTo(member2));
  }

  @Test
  public void testShallowMemberNotEquals() {
    GMSMember member1 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    GMSMember member2 = new GMSMember(member1.getInetAddress(), member1.getPort(),
        member1.getVersionOrdinal(), member1.getUuidMSBs(), member1.getUuidLSBs(), 100);
    assertEquals(false, member1.equals(member2));
  }

  @Test
  public void testCompareToInetAddressIsShorterThan() {
    GMSMember member1 = createGMSMember(new byte[] {1, 1, 1, 1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    assertEquals(-1, member1.compareTo(member2));
  }

  @Test
  public void testCompareToInetAddressIsGreater() {
    GMSMember member1 = createGMSMember(new byte[] {1, 2, 1, 1, 1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    assertEquals(1, member1.compareTo(member2));
  }

  @Test
  public void testCompareToInetAddressIsLessThan() {
    GMSMember member1 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1, 2, 1, 1, 1}, 1, 1, 1);
    assertEquals(-1, member1.compareTo(member2));
  }

  @Test
  public void testCompareToMyViewIdLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 2, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1}, 1, 1, 1);
    assertEquals(1, member1.compareTo(member2));
  }

  @Test
  public void testCompareToTheirViewIdLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1}, 2, 1, 1);
    assertEquals(-1, member1.compareTo(member2));
  }

  @Test
  public void testCompareToMyMSBLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 1, 2, 1);
    GMSMember member2 = createGMSMember(new byte[] {1}, 1, 1, 1);
    assertEquals(1, member1.compareTo(member2));
  }

  @Test
  public void testCompareToTheirMSBLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1}, 1, 2, 1);
    assertEquals(-1, member1.compareTo(member2));
  }

  @Test
  public void testCompareToMyLSBLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 1, 1, 2);
    GMSMember member2 = createGMSMember(new byte[] {1}, 1, 1, 1);
    assertEquals(1, member1.compareTo(member2));
  }

  @Test
  public void testCompareToTheirLSBLarger() {
    GMSMember member1 = createGMSMember(new byte[] {1}, 1, 1, 1);
    GMSMember member2 = createGMSMember(new byte[] {1}, 1, 1, 2);
    assertEquals(-1, member1.compareTo(member2));
  }

  @Test
  public void testGetUUIDReturnsNullWhenUUIDIs0() {
    GMSMember member = new GMSMember();
    UUID uuid = new UUID(0, 0);
    member.setUUID(uuid);
    assertNull(member.getUUID());
  }

  @Test
  public void testGetUUID() {
    GMSMember member = new GMSMember();
    UUID uuid = new UUID(1, 1);
    member.setUUID(uuid);
    assertNotNull(member.getUUID());
  }

  /**
   * <p>
   * GEODE-2875 - adds vmKind to on-wire form of GMSMember.writeEssentialData
   * </p>
   * <p>
   * This must be backward-compatible with Geode 1.0 (Version.GFE_90)
   * </p>
   *
   */
  @Test
  public void testGMSMemberBackwardCompatibility() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GMSMember member = new GMSMember();
    DataOutput dataOutput = new DataOutputStream(baos);
    SerializationContext context = InternalDataSerializer.getDSFIDSerializer()
        .createSerializationContext(dataOutput);
    member.writeEssentialData(dataOutput, context);

    // vmKind should be transmitted to a member with the current version
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInput dataInput = new DataInputStream(bais);
    GMSMember newMember = new GMSMember();
    newMember.readEssentialData(dataInput, context);
    assertEquals(member.getVmKind(), newMember.getVmKind());

    // vmKind should not be transmitted to a member with version GFE_90 or earlier
    dataOutput = new HeapDataOutputStream(Version.GFE_90);
    member.writeEssentialData(dataOutput, context);
    bais = new ByteArrayInputStream(baos.toByteArray());
    dataInput = new VersionedDataInputStream(new DataInputStream(bais), Version.GFE_90);
    newMember = new GMSMember();
    newMember.readEssentialData(dataInput, context);
    assertEquals(0, newMember.getVmKind());
  }


}
