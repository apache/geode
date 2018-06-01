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
package org.apache.geode.internal.cache.versions;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Category(UnitTest.class)
public class RegionVersionVectorTest {

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * server1 will simulate doing a sync with another server for operations performed
   * by server2. server3 is another server in the cluster that we don't care about
   */
  @Test
  public void testSynchronizationVectorContainsAllVersionsForOwnerAndNonTarget() {
    final String local = NetworkUtils.getIPLiteral();
    InternalDistributedMember server1 = new InternalDistributedMember(local, 101);
    InternalDistributedMember server2 = new InternalDistributedMember(local, 102);
    InternalDistributedMember server3 = new InternalDistributedMember(local, 103);

    RegionVersionVector rv1 = new VMRegionVersionVector(server1);
    rv1.updateLocalVersion(10);
    rv1.recordVersion(server2, 1);
    rv1.recordVersion(server2, 5);
    rv1.recordVersion(server2, 8);
    rv1.recordVersion(server3, 1);
    rv1.recordVersion(server3, 3);
    RegionVersionVector singletonRVV = rv1.getCloneForTransmission(server2);
    assertTrue(singletonRVV.isForSynchronization());
    assertEquals(singletonRVV.getOwnerId(), server1);
    assertTrue(singletonRVV.getMemberToVersion().containsKey(server2));
    assertFalse(singletonRVV.getMemberToVersion().containsKey(server3));

    assertTrue(singletonRVV.contains(server1, 1));
    assertTrue(singletonRVV.contains(server1, 11));

    assertTrue(singletonRVV.contains(server3, 1));
    assertTrue(singletonRVV.contains(server3, 11));

    assertTrue(singletonRVV.contains(server2, 1));
    assertTrue(singletonRVV.contains(server2, 5));
    assertTrue(singletonRVV.contains(server2, 8));

    assertFalse(singletonRVV.contains(server2, 2));
    assertFalse(singletonRVV.contains(server2, 3));
    assertFalse(singletonRVV.contains(server2, 4));
    assertFalse(singletonRVV.contains(server2, 6));
    assertFalse(singletonRVV.contains(server2, 7));
    assertFalse(singletonRVV.contains(server2, 9));

  }

  @Test
  public void testExceptionsWithContains() {
    DiskStoreID ownerId = new DiskStoreID(0, 0);
    DiskStoreID id1 = new DiskStoreID(0, 1);
    DiskStoreID id2 = new DiskStoreID(1, 0);

    DiskRegionVersionVector rvv = new DiskRegionVersionVector(ownerId);

    doExceptionsWithContains(ownerId, rvv);
    doExceptionsWithContains(id1, rvv);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testRegionVersionVectors() throws Exception {
    // this is just a quick set of unit tests for basic RVV functionality

    final String local = NetworkUtils.getIPLiteral();
    InternalDistributedMember server1 = new InternalDistributedMember(local, 101);
    InternalDistributedMember server2 = new InternalDistributedMember(local, 102);
    InternalDistributedMember server3 = new InternalDistributedMember(local, 103);
    InternalDistributedMember server4 = new InternalDistributedMember(local, 104);

    RegionVersionVector rv1 = null;

    // (a) Test that an exception is mended when the versions that are missing are
    // added
    rv1 = new VMRegionVersionVector(server1);
    rv1.recordVersion(server2, 1);
    rv1.recordVersion(server2, 5);
    rv1.recordVersion(server2, 8);
    System.out.println("for test (a) formed this RVV: " + rv1.fullToString());
    // there should now be two exceptions: 1-5 and 5-8
    assertEquals(8, rv1.getVersionForMember(server2));
    assertEquals(2, rv1.getExceptionCount(server2));
    rv1.recordVersion(server2, 3);
    System.out.println("for test (a) RVV is now: " + rv1.fullToString());
    assertEquals(8, rv1.getVersionForMember(server2));
    assertEquals(2, rv1.getExceptionCount(server2));
    rv1.recordVersion(server2, 4);
    rv1.recordVersion(server2, 2);
    System.out.println("for test (a) RVV is now: " + rv1.fullToString());
    assertEquals(1, rv1.getExceptionCount(server2));
    rv1.recordVersion(server2, 6);
    rv1.recordVersion(server2, 7);
    System.out.println("for test (a) RVV is now: " + rv1.fullToString());
    assertEquals(0, rv1.getExceptionCount(server2));

    // (b) Test the contains() operation
    rv1 = new VMRegionVersionVector(server1);
    rv1.recordVersion(server2, 1);
    rv1.recordVersion(server2, 5);
    rv1.recordVersion(server2, 8);
    rv1.recordVersion(server2, 10);
    System.out.println("for test (b) formed this RVV: " + rv1.fullToString());
    assertTrue(rv1.contains(server2, 1));
    assertTrue(rv1.contains(server2, 5));
    assertTrue(rv1.contains(server2, 8));
    assertTrue(rv1.contains(server2, 10));
    assertFalse(rv1.contains(server2, 2));
    assertFalse(rv1.contains(server2, 3));
    assertFalse(rv1.contains(server2, 4));
    assertFalse(rv1.contains(server2, 9));
    assertFalse(rv1.contains(server2, 11));
    rv1.recordVersion(server2, 3);
    System.out.println("for test (b) RVV is now: " + rv1.fullToString());
    assertTrue(rv1.contains(server2, 3));
    rv1.recordVersion(server2, 2);
    System.out.println("for test (b) RVV is now: " + rv1.fullToString());
    assertTrue(rv1.contains(server2, 2));
    assertTrue(rv1.contains(server2, 3));
    rv1.recordVersion(server2, 4);
    System.out.println("for test (b) RVV is now: " + rv1.fullToString());
    assertTrue(rv1.contains(server2, 1));
    assertTrue(rv1.contains(server2, 2));
    assertTrue(rv1.contains(server2, 3));
    assertTrue(rv1.contains(server2, 4));
    assertTrue(rv1.contains(server2, 5));
    rv1.recordVersion(server2, 11);
    System.out.println("for test (b) RVV is now: " + rv1.fullToString());
    assertTrue(rv1.contains(server2, 11));
    assertTrue(rv1.contains(server2, 10));
    System.out.println("for test (b) RVV is now: " + rv1.fullToString());
    rv1.recordVersion(server2, 6);
    assertTrue(rv1.contains(server2, 2));
    assertTrue(rv1.contains(server2, 5));
    assertTrue(rv1.contains(server2, 6));
    assertFalse(rv1.contains(server2, 7));
    assertTrue(rv1.contains(server2, 8));
    rv1.recordVersion(server2, 7);
    System.out.println("for test (b) RVV is now: " + rv1.fullToString());
    assertTrue(rv1.contains(server2, 7));
    rv1.recordVersion(server2, 9);
    System.out.println("for test (b) RVV is now: " + rv1.fullToString());
    assertTrue(rv1.contains(server2, 9));
    assertTrue(rv1.getExceptionCount(server2) == 0);
    assertTrue(rv1.contains(server2, 8));

    // Test RVV comparisons for GII Delta
    rv1 = new VMRegionVersionVector(server1);
    rv1.recordVersion(server2, 1);
    rv1.recordVersion(server2, 4);
    rv1.recordVersion(server2, 8);
    rv1.recordVersion(server2, 9);
    rv1.recordVersion(server2, 10);
    rv1.recordVersion(server2, 11);
    rv1.recordVersion(server2, 12);

    rv1.recordVersion(server3, 2);
    rv1.recordVersion(server3, 3);
    rv1.recordVersion(server3, 4);
    rv1.recordVersion(server3, 6);
    rv1.recordVersion(server3, 7);

    RegionVersionVector rv2 = rv1.getCloneForTransmission();
    System.out.println("rv1 is " + rv1.fullToString());
    System.out.println("rv2 is " + rv2.fullToString());
    assertFalse(rv1.isNewerThanOrCanFillExceptionsFor(rv2));
    assertFalse(rv2.isNewerThanOrCanFillExceptionsFor(rv1));

    rv1.recordVersion(server2, 6);
    assertTrue(rv1.isNewerThanOrCanFillExceptionsFor(rv2));
    rv2.recordVersion(server2, 6);
    assertFalse(rv1.isNewerThanOrCanFillExceptionsFor(rv2));

    // fill an exception gap
    rv1.recordVersion(server2, 5);
    assertTrue(rv1.isNewerThanOrCanFillExceptionsFor(rv2));
    rv2.recordVersion(server2, 5);
    assertFalse(rv1.isNewerThanOrCanFillExceptionsFor(rv2));
    rv1.recordVersion(server2, 7);
    assertTrue(rv1.isNewerThanOrCanFillExceptionsFor(rv2));
    rv2.recordVersion(server2, 7);
    assertFalse(rv1.isNewerThanOrCanFillExceptionsFor(rv2));

    // add a more recent revision
    rv1.recordVersion(server3, 8);
    assertTrue(rv1.isNewerThanOrCanFillExceptionsFor(rv2));
    rv2.recordVersion(server3, 8);
    assertFalse(rv1.isNewerThanOrCanFillExceptionsFor(rv2));

    // fill another exception gap
    rv1.recordVersion(server3, 5);
    assertTrue(rv1.isNewerThanOrCanFillExceptionsFor(rv2));
    rv2.recordVersion(server3, 5);
    assertFalse(rv1.isNewerThanOrCanFillExceptionsFor(rv2));

    // test that old members are removed from the vector
    InternalDistributedMember server5 = new InternalDistributedMember(local, 105);
    rv1 = new VMRegionVersionVector(server1);
    rv1.recordVersion(server2, 1);
    rv1.recordVersion(server3, 1);
    rv1.recordVersion(server4, 1);
    rv1.recordVersion(server5, 1);
    rv1.memberDeparted(null, server2, false);
    rv1.memberDeparted(null, server4, true);
    assertTrue(rv1.containsMember(server2));
    assertTrue(rv1.containsMember(server3));
    assertTrue(rv1.containsMember(server4));
    Set retain = new HashSet();
    retain.add(server2); // still have data from server2
    retain.add(server3); // still have data from server3
    // no data found from server4 in region
    retain.add(server5); // still have data from server5
    rv1.removeOldMembers(retain);
    assertFalse(rv1.containsMember(server4));

    rv1.memberDeparted(null, server3, false); // {server2, server3(departed), server5}

    // Now test that departed members are transferred with GII. We simulate
    // a new server, server6, doing a GII from server1

    InternalDistributedMember server6 = new InternalDistributedMember(local, 106);
    RegionVersionVector giiReceiverRVV = new VMRegionVersionVector(server6);
    // the gii request will cause server1 to clone its RVV and send it to server6
    rv2 = rv1.getCloneForTransmission();
    // serialize/deserialize to mimic sending the rvv in a message
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
    DataOutputStream out = new DataOutputStream(baos);
    DataSerializer.writeObject(rv2, out);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream in = new DataInputStream(bais);
    RegionVersionVector transmittedVector = (RegionVersionVector) DataSerializer.readObject(in);
    // record the provider's rvv in the receiver of the image
    giiReceiverRVV.recordVersions(transmittedVector);
    // removedMembers in the receiver should hold {server4, server3}. Simulate
    // another member departure to kick out server4
    assertTrue(giiReceiverRVV.containsMember(server2));
    assertTrue(giiReceiverRVV.containsMember(server5));
    assertTrue(giiReceiverRVV.containsMember(server3));
    assertTrue(giiReceiverRVV.isDepartedMember(server3));

    // unit test for bit-set boundary. First boundary is 3/4 of bitset width,
    // which is the amount dumped to the exceptions list when the bitset becomes full
    rv1 = new VMRegionVersionVector(server1);
    long bitSetRollPoint = RegionVersionHolder.BIT_SET_WIDTH + 1;
    long boundary = RegionVersionHolder.BIT_SET_WIDTH * 3 / 4;
    for (long i = 1; i < boundary; i++) {
      rv1.recordVersion(server2, i);
      assertTrue(rv1.contains(server2, i));
    }
    assertFalse(rv1.contains(server2, boundary + 1));

    rv1.recordVersion(server2, bitSetRollPoint);
    rv1.recordVersion(server2, bitSetRollPoint + 1); // bitSet should be rolled at this point
    RegionVersionHolder h = (RegionVersionHolder) rv1.getMemberToVersion().get(server2);
    long versionBoundary = h.getBitSetVersionForTesting();
    assertEquals("expected holder bitset version to roll to this value", boundary - 1,
        versionBoundary);
    assertFalse(rv1.contains(server2, bitSetRollPoint - 1));
    assertTrue(rv1.contains(server2, bitSetRollPoint));
    assertTrue(rv1.contains(server2, bitSetRollPoint + 1));
    assertFalse(rv1.contains(server2, bitSetRollPoint + 2));

    assertTrue(rv1.contains(server2, boundary - 1));
    assertFalse(rv1.contains(server2, boundary));
    assertFalse(rv1.contains(server2, boundary + 1));

    // now test the merge
    System.out.println("testing merge for " + rv1.fullToString());
    assertEquals(1, rv1.getExceptionCount(server2)); // one exception from boundary-1 to
    // bitSetRollPoint
    assertFalse(rv1.contains(server2, bitSetRollPoint - 1));
    assertTrue(rv1.contains(server2, bitSetRollPoint));
    assertTrue(rv1.contains(server2, bitSetRollPoint + 1));
    assertFalse(rv1.contains(server2, bitSetRollPoint + 2));
  }

  @Test
  public void testRVVSerialization() throws Exception {
    DiskStoreID ownerId = new DiskStoreID(0, 0);
    DiskStoreID id1 = new DiskStoreID(0, 1);
    DiskStoreID id2 = new DiskStoreID(1, 0);

    DiskRegionVersionVector rvv = new DiskRegionVersionVector(ownerId);
    rvv.recordVersion(id1, 5);
    rvv.recordVersion(id1, 6);
    rvv.recordVersion(id1, 7);
    rvv.recordVersion(id1, 9);
    rvv.recordVersion(id1, 20);
    rvv.recordVersion(id1, 11);
    rvv.recordVersion(id1, 12);
    rvv.recordGCVersion(id2, 5);
    rvv.recordGCVersion(id1, 3);

    assertTrue(rvv.sameAs(rvv.getCloneForTransmission()));

    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(rvv.getCloneForTransmission(), out);
    byte[] bytes = out.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
    DiskRegionVersionVector rvv2 = DataSerializer.readObject(dis);

    assertTrue(rvv.sameAs(rvv2));
  }

  /**
   * Test that we can copy the member to version map correctly.
   */
  @Test
  public void testCopyMemberToVersion() {
    DiskStoreID id0 = new DiskStoreID(0, 0);
    DiskStoreID id1 = new DiskStoreID(0, 1);
    DiskStoreID id2 = new DiskStoreID(1, 0);

    DiskRegionVersionVector rvv0 = new DiskRegionVersionVector(id0);
    rvv0.getNextVersion();
    rvv0.getNextVersion();
    rvv0.getNextVersion();

    rvv0.recordVersion(id1, 1);
    rvv0.recordVersion(id1, 3);

    DiskRegionVersionVector rvv1 = new DiskRegionVersionVector(id1);
    rvv1.recordVersions(rvv0);

    assertEquals(3, rvv1.getCurrentVersion());
    assertFalse(rvv1.contains(id1, 2));
    assertTrue(rvv1.contains(id1, 1));
    assertTrue(rvv1.contains(id1, 3));

    assertTrue(rvv1.contains(id0, 3));

    assertTrue(rvv0.sameAs(rvv1));

    rvv1.recordVersion(id1, 2);
    assertTrue(rvv1.isNewerThanOrCanFillExceptionsFor(rvv0));
    assertFalse(rvv0.isNewerThanOrCanFillExceptionsFor(rvv1));
    assertTrue(rvv1.dominates(rvv0));
    assertFalse(rvv0.dominates(rvv1));
  }

  @Test
  public void testSpecialException() {
    DiskStoreID id0 = new DiskStoreID(0, 0);
    DiskStoreID id1 = new DiskStoreID(0, 1);
    DiskStoreID id2 = new DiskStoreID(1, 0);

    DiskRegionVersionVector rvv0 = new DiskRegionVersionVector(id0);
    rvv0.getNextVersion();
    rvv0.getNextVersion();
    rvv0.getNextVersion();

    rvv0.recordVersion(id1, 1);
    rvv0.recordVersion(id1, 2);

    DiskRegionVersionVector rvv1 = new DiskRegionVersionVector(id1);
    rvv1.recordVersions(rvv0);

    rvv1.recordVersion(id1, 3);
    RegionVersionHolder holder_at_rvv1 = rvv1.getLocalExceptions();
    RegionVersionHolder holder_at_rvv0 = rvv0.getMemberToVersion().get(id1);
    holder_at_rvv1.addException(2, 4);
    assertFalse(rvv1.isNewerThanOrCanFillExceptionsFor(rvv0));
    assertFalse(rvv0.isNewerThanOrCanFillExceptionsFor(rvv1));
    assertTrue(rvv1.dominates(rvv0));
    assertTrue(rvv0.dominates(rvv1));
  }

  @Test
  public void test48066_1() {
    DiskStoreID id0 = new DiskStoreID(0, 0);
    DiskRegionVersionVector rvv0 = new DiskRegionVersionVector(id0);
    for (int i = 1; i <= 3; i++) {
      rvv0.recordVersion(id0, i);
    }
    System.out.println("rvv0=" + rvv0.fullToString());

    DiskRegionVersionVector rvv1 = (DiskRegionVersionVector) rvv0.getCloneForTransmission();
    System.out.println("after clone, rvv1=" + rvv1.fullToString());

    DiskRegionVersionVector rvv2 = new DiskRegionVersionVector(id0);
    for (int i = 1; i <= 10; i++) {
      rvv2.recordVersion(id0, i);
    }
    rvv2.recordVersions(rvv1);
    System.out.println("after init, rvv2=" + rvv2.fullToString());

    rvv2.recordVersion(id0, 4);
    System.out.println("after record 4, rvv2=" + rvv2.fullToString());
    assertEquals(4, rvv2.getCurrentVersion());

    rvv2.recordVersion(id0, 7);
    System.out.println("after record 7, rvv2=" + rvv2.fullToString());
    assertEquals(7, rvv2.getCurrentVersion());
  }

  @Test
  public void test48066_2() {
    DiskStoreID id0 = new DiskStoreID(0, 0);
    DiskRegionVersionVector rvv0 = new DiskRegionVersionVector(id0);
    for (int i = 1; i <= 10; i++) {
      rvv0.recordVersion(id0, i);
    }

    DiskRegionVersionVector rvv1 = new DiskRegionVersionVector(id0);
    rvv0.recordVersions(rvv1);
    System.out.println("rvv0=" + rvv0.fullToString());

    rvv0.recordVersion(id0, 4);
    System.out.println("after record 4, rvv0=" + rvv0.fullToString());
    assertEquals(4, rvv0.getCurrentVersion());

    rvv0.recordVersion(id0, 7);
    System.out.println("after record 7, rvv0=" + rvv0.fullToString());
    assertEquals(7, rvv0.getCurrentVersion());
    assertFalse(rvv0.contains(id0, 5));

    DiskRegionVersionVector rvv2 = (DiskRegionVersionVector) rvv0.getCloneForTransmission();
    System.out.println("after clone, rvv2=" + rvv2.fullToString());
    assertEquals(11, rvv0.getNextVersion());
    assertFalse(rvv2.contains(id0, 5));
    assertEquals(11, rvv2.getNextVersion());
  }

  /**
   * Test for bug 47023. Make sure recordGCVersion works correctly and doesn't generate exceptions
   * for the local member.
   */
  @Test
  public void testRecordGCVersion() {
    DiskStoreID id0 = new DiskStoreID(0, 0);
    DiskStoreID id1 = new DiskStoreID(0, 1);

    DiskRegionVersionVector rvv0 = new DiskRegionVersionVector(id0);

    // generate 3 local versions
    rvv0.getNextVersion();
    rvv0.getNextVersion();
    rvv0.getNextVersion();

    // record some version from a remote member
    rvv0.recordVersion(id1, 1);
    rvv0.recordVersion(id1, 3);
    rvv0.recordVersion(id1, 5);

    // Assert that the exceptions are present
    {
      Map<DiskStoreID, RegionVersionHolder<DiskStoreID>> memberToVersion =
          rvv0.getMemberToVersion();
      RegionVersionHolder<DiskStoreID> holder1 = memberToVersion.get(id1);

      // Make sure the exceptions are present
      assertFalse(holder1.contains(2));
      assertFalse(holder1.contains(4));
    }

    // Record some GC versions
    rvv0.recordGCVersion(id0, 2);
    rvv0.recordGCVersion(id1, 3);

    {
      Map<DiskStoreID, RegionVersionHolder<DiskStoreID>> memberToVersion =
          rvv0.getMemberToVersion();
      RegionVersionHolder<DiskStoreID> holder0 = memberToVersion.get(id0);
      // Make sure we didn't generate a bogus exception for
      // the local member by calling record GC version - bug 47023
      assertTrue(holder0.getExceptionForTest().isEmpty());
    }

    // Clean up old exceptions
    rvv0.pruneOldExceptions();

    // Make assertions about what exceptions are still present
    Map<DiskStoreID, RegionVersionHolder<DiskStoreID>> memberToVersion = rvv0.getMemberToVersion();
    RegionVersionHolder<DiskStoreID> holder0 = memberToVersion.get(id0);
    RegionVersionHolder<DiskStoreID> holder1 = memberToVersion.get(id1);
    assertTrue(holder0.getExceptionForTest().isEmpty());

    // exceptions less than the GC version should have been removed
    assertTrue(holder1.contains(2));

    // exceptions greater than the GC version should still be there.
    assertFalse(holder1.contains(4));
  }

  @Test
  public void testRemoveOldVersions() {
    DiskStoreID id0 = new DiskStoreID(0, 0);
    DiskStoreID id1 = new DiskStoreID(0, 1);
    DiskStoreID id2 = new DiskStoreID(0, 2);

    DiskRegionVersionVector rvv = new DiskRegionVersionVector(id0);

    // generate 3 local versions
    rvv.getNextVersion();
    rvv.getNextVersion();
    rvv.getNextVersion();

    // record some version from a remote member
    rvv.recordVersion(id1, 1);
    rvv.recordVersion(id1, 3);
    rvv.recordVersion(id1, 5);
    // record a GC version for that member that is older than its version
    rvv.recordGCVersion(id1, 3);

    rvv.recordGCVersion(id2, 4950);
    rvv.recordVersion(id2, 5000);
    rvv.recordVersion(id2, 5001);
    rvv.recordVersion(id2, 5005);

    rvv.removeOldVersions();

    assertEquals("expected gc version to be set to current version for " + rvv.fullToString(),
        rvv.getCurrentVersion(), rvv.getGCVersion(null));
    assertEquals("expected gc version to be set to current version for " + rvv.fullToString(),
        rvv.getVersionForMember(id1), rvv.getGCVersion(id1));
    assertEquals("expected gc version to be set to current version for " + rvv.fullToString(),
        rvv.getVersionForMember(id2), rvv.getGCVersion(id2));

    assertEquals("expected exceptions to be erased for " + rvv.fullToString(),
        rvv.getExceptionCount(id1), 0);
    assertEquals("expected exceptions to be erased for " + rvv.fullToString(),
        rvv.getExceptionCount(id2), 0);
  }

  @Test
  public void testRegionVersionInTags() {
    VMVersionTag tag = new VMVersionTag();
    long version = 0x8080000000L;
    tag.setRegionVersion(version);
    assertEquals("failed test for bug #48576", version, tag.getRegionVersion());
  }

  @Test
  public void testRecordVersionDuringRegionInit() {
    LocalRegion mockRegion = mock(LocalRegion.class);
    when(mockRegion.isInitialized()).thenReturn(false);
    final String local = NetworkUtils.getIPLiteral();
    InternalDistributedMember ownerId = new InternalDistributedMember(local, 101);
    VMVersionTag tag = new VMVersionTag();
    tag.setRegionVersion(1L);

    RegionVersionVector rvv = createRegionVersionVector(ownerId, mockRegion);
    rvv.recordVersion(ownerId, tag);
    assertEquals(1, rvv.getVersionForMember(ownerId));
  }

  @Test
  public void testRecordVersionAfterRegionInitThrowsException() {
    LocalRegion mockRegion = mock(LocalRegion.class);
    when(mockRegion.isInitialized()).thenReturn(true);
    final String local = NetworkUtils.getIPLiteral();
    InternalDistributedMember ownerId = new InternalDistributedMember(local, 101);
    VMVersionTag tag = new VMVersionTag();
    tag.setRegionVersion(1L);

    RegionVersionVector rvv = createRegionVersionVector(ownerId, mockRegion);
    expectedException.expect(InternalGemFireError.class);
    rvv.recordVersion(ownerId, tag);
  }

  @Test
  public void usesNewVersionIfGreaterThanOldVersion() throws Exception {
    VersionSource<InternalDistributedMember> ownerId = mock(VersionSource.class);
    long oldVersion = 1;
    long newVersion = 2;

    RegionVersionVector rvv = new TestableRegionVersionVector(ownerId, oldVersion);
    rvv.updateLocalVersion(newVersion);
    assertThat(rvv.getVersionForMember(ownerId)).isEqualTo(newVersion);
  }

  @Test
  public void usesOldVersionIfGreaterThanNewVersion() throws Exception {
    VersionSource<InternalDistributedMember> ownerId = mock(VersionSource.class);
    long oldVersion = 2;
    long newVersion = 1;

    RegionVersionVector rvv = new TestableRegionVersionVector(ownerId, oldVersion);
    rvv.updateLocalVersion(newVersion);
    assertThat(rvv.getVersionForMember(ownerId)).isEqualTo(oldVersion);
  }

  @Test
  public void doesNothingIfVersionsAreSame() throws Exception {
    VersionSource<InternalDistributedMember> ownerId = mock(VersionSource.class);
    long oldVersion = 2;
    long sameVersion = 2;

    RegionVersionVector rvv = new TestableRegionVersionVector(ownerId, oldVersion);
    rvv.updateLocalVersion(sameVersion);
    assertThat(rvv.getVersionForMember(ownerId)).isEqualTo(oldVersion);
  }

  @Test
  public void doesNotHangIfOtherThreadChangedVersion() throws Exception {
    VersionSource<InternalDistributedMember> ownerId = mock(VersionSource.class);
    long oldVersion = 1;
    long newVersion = 2;

    RegionVersionVector rvv = new VersionRaceConditionRegionVersionVector(ownerId, oldVersion);
    Future<Void> result = executorServiceRule.runAsync(() -> rvv.updateLocalVersion(newVersion));

    assertThatCode(() -> result.get(2, SECONDS)).doesNotThrowAnyException();
    assertThat(rvv.getVersionForMember(ownerId)).isEqualTo(newVersion);
  }

  private RegionVersionVector createRegionVersionVector(InternalDistributedMember ownerId,
      LocalRegion owner) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    RegionVersionVector rvv = new RegionVersionVector(ownerId, owner) {
      @Override
      public int getDSFID() {
        return 0;
      }

      @Override
      protected RegionVersionVector createCopy(VersionSource ownerId, ConcurrentHashMap vector,
          long version, ConcurrentHashMap gcVersions, long gcVersion, boolean singleMember,
          RegionVersionHolder clonedLocalHolder) {
        return null;
      }

      @Override
      protected void writeMember(VersionSource member, DataOutput out) throws IOException {}

      @Override
      protected VersionSource readMember(DataInput in) throws IOException, ClassNotFoundException {
        return null;
      }
    };
    return rvv;
  }

  private void doExceptionsWithContains(DiskStoreID id, DiskRegionVersionVector rvv) {
    rvv.recordVersion(id, 10);

    // Make sure we have exceptions from 0-10
    assertFalse(rvv.contains(id, 5));

    rvv.recordVersion(id, 5);
    assertTrue(rvv.contains(id, 5));
    assertFalse(rvv.contains(id, 4));
    assertFalse(rvv.contains(id, 6));

    for (int i = 0; i < 10; i++) {
      rvv.recordVersion(id, i);
    }

    for (int i = 0; i < 10; i++) {
      assertTrue(rvv.contains(id, i));
    }
    assertEquals(0, rvv.getExceptionCount(id));
  }

  private static class TestableRegionVersionVector
      extends RegionVersionVector<VersionSource<InternalDistributedMember>> {

    TestableRegionVersionVector(VersionSource<InternalDistributedMember> ownerId, long version) {
      super(ownerId, null, version);
    }

    @Override
    protected RegionVersionVector createCopy(VersionSource ownerId, ConcurrentHashMap vector,
        long version, ConcurrentHashMap gcVersions, long gcVersion, boolean singleMember,
        RegionVersionHolder clonedLocalHolder) {
      return null;
    }

    @Override
    protected VersionSource<InternalDistributedMember> readMember(DataInput in)
        throws IOException, ClassNotFoundException {
      return null;
    }

    @Override
    protected void writeMember(VersionSource member, DataOutput out) throws IOException {

    }

    @Override
    public int getDSFID() {
      return 0;
    }
  }

  private static class VersionRaceConditionRegionVersionVector extends TestableRegionVersionVector {

    VersionRaceConditionRegionVersionVector(VersionSource<InternalDistributedMember> ownerId,
        long version) {
      super(ownerId, version);
    }

    @Override
    boolean compareAndSetVersion(long currentVersion, long newVersion) {
      super.compareAndSetVersion(currentVersion, newVersion);
      return false;
    }

  }
}
