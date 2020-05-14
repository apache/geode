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
package org.apache.geode.internal.cache.partitioned.rebalance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.OfflineMemberDetails;
import org.apache.geode.internal.cache.partitioned.OfflineMemberDetailsImpl;
import org.apache.geode.internal.cache.partitioned.PRLoad;
import org.apache.geode.internal.cache.partitioned.PartitionMemberInfoImpl;
import org.apache.geode.internal.cache.partitioned.rebalance.BucketOperator.Completion;
import org.apache.geode.internal.cache.partitioned.rebalance.model.AddressComparor;
import org.apache.geode.internal.cache.partitioned.rebalance.model.PartitionedRegionLoadModel;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.inet.LocalHostUtil;

@RunWith(JUnitParamsRunner.class)
public class PartitionedRegionLoadModelJUnitTest {

  private static final int MAX_MOVES = 5000;
  private static final boolean DEBUG = true;

  private MyBucketOperator bucketOperator;

  @Before
  public void setUp() {
    bucketOperator = new MyBucketOperator();
  }

  /**
   * This test checks basic redundancy satisfaction. It creates two buckets with low redundancy and
   * 1 bucket with full redundancy and excepts copies of the low redundancy buckets to be made.
   */
  @Test
  public void testRedundancySatisfaction() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 2, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 0}, new long[] {1, 1, 1, 0});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {0, 1, 0, 1}, new long[] {0, 0, 0, 1});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);

    Set<PartitionMemberInfo> details = model.getPartitionedMemberDetails("a");
    assertEquals(2, details.size());

    // TODO - make some assertions about what's in the details

    // we expect three moves
    assertEquals(3, doMoves(new CompositeDirector(true, true, false, false), model));

    // TODO - make some assertions about what's in the details

    List<Create> expectedCreates = new ArrayList<>();
    expectedCreates.add(new Create(member2, 0));
    expectedCreates.add(new Create(member2, 2));
    expectedCreates.add(new Create(member1, 3));
    assertEquals(expectedCreates, bucketOperator.creates);
  }

  /**
   * This test creates buckets with low redundancy, but only 1 of the buckets is small enough to be
   * copied. The other bucket should be rejected because it is too big..
   */
  @Test
  public void testRedundancySatisfactionWithSizeLimit() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 2, 3,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);

    // A member with 1 bucket with low redundancy, but it is too big to copy anywhere
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 50, 50, new long[] {30, 0, 0}, new long[] {1, 0, 0});
    // A member with 2 buckets with low redundancy that can be copied
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 40, 40, new long[] {0, 10, 10}, new long[] {0, 1, 1});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);

    // we expect 2 moves
    assertEquals(2, doMoves(new CompositeDirector(true, true, false, false), model));

    List<Create> expectedCreates = new ArrayList<>();
    expectedCreates.add(new Create(member1, 1));
    expectedCreates.add(new Create(member1, 2));
    assertEquals(expectedCreates, bucketOperator.creates);
  }

  @Test
  public void testRedundancySatisfactionWithCriticalMember() throws UnknownHostException {
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 2, 3,
        getAddressComparor(false), Collections.singleton(member1), null);

    // this member has critical heap
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 50, 50, new long[] {10, 0, 0}, new long[] {1, 0, 0});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 40, 40, new long[] {0, 10, 10}, new long[] {0, 1, 1});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);

    // we expect 2 moves
    assertEquals(1, doMoves(new CompositeDirector(true, true, false, false), model));

    List<Create> expectedCreates = new ArrayList<>();
    expectedCreates.add(new Create(member2, 0));
    assertEquals(expectedCreates, bucketOperator.creates);
  }

  /**
   * This test makes sure we ignore the size limit if requested
   */
  @Test
  public void testRedundancySatisfactionDoNotEnforceLocalMaxMemory() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 2, 3,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);

    // A member with 1 bucket with low redundancy, but it is too big to copy anywhere
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 50, 50, new long[] {30, 0, 0}, new long[] {1, 0, 0});
    // A member with 2 buckets with low redundancy that can be copied
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 40, 40, new long[] {0, 10, 10}, new long[] {0, 1, 1});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), false);

    // we expect 2 moves
    assertEquals(3, doMoves(new CompositeDirector(true, true, false, false), model));

    List<Create> expectedCreates = new ArrayList<>();
    expectedCreates.add(new Create(member2, 0));
    expectedCreates.add(new Create(member1, 1));
    expectedCreates.add(new Create(member1, 2));
    assertEquals(expectedCreates, bucketOperator.creates);
    Set<PartitionMemberInfo> afterDetails = model.getPartitionedMemberDetails("a");
    assertEquals(afterDetails.size(), 2);
    for (PartitionMemberInfo member : afterDetails) {
      if (member.getDistributedMember().equals(member1)) {
        assertEquals(details1.getConfiguredMaxMemory(), member.getConfiguredMaxMemory());
      } else {
        assertEquals(details2.getConfiguredMaxMemory(), member.getConfiguredMaxMemory());
      }
    }
  }

  /**
   * Tests to make sure that redundancy satisfaction prefers to make redundant copies on members
   * with remote IP addresses.
   */
  @Test
  public void testRedundancySatisfactionPreferRemoteIp() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 3,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(LocalHostUtil.getLocalHost(), 3);

    // Create some buckets with low redundancy on members 1 and 2
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {30, 0, 0}, new long[] {1, 0, 0});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {0, 30, 30}, new long[] {0, 1, 1});
    PartitionMemberInfoImpl details3 =
        buildDetails(member3, 500, 500, new long[] {0, 0, 0}, new long[] {0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);

    // we expect 3 moves
    assertEquals(3, doMoves(new CompositeDirector(true, true, false, false), model));

    // The buckets should all be created on member 3 because
    // it has a different ip address
    List<Create> expectedCreates = new ArrayList<>();
    expectedCreates.add(new Create(member3, 0));
    expectedCreates.add(new Create(member3, 1));
    expectedCreates.add(new Create(member3, 2));
    assertEquals(expectedCreates, bucketOperator.creates);
  }

  @Test
  public void testRedundancySatisfactionEnforceRemoteIp() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 3,
        getAddressComparor(true), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);

    // Create some buckets with low redundancy on members 1
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {30, 30, 30}, new long[] {1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {0, 0, 0}, new long[] {0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);

    // we expect 0 moves, because we're enforcing that we can't create
    // copies on the same IP.
    assertEquals(0, doMoves(new CompositeDirector(true, true, false, false), model));

    assertEquals(Collections.emptyList(), bucketOperator.creates);
  }

  @Test
  public void testMoveBucketsEnforceRemoteIp() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 0, 3,
        getAddressComparor(true), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);

    // Create some buckets with low redundancy on members 1
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {30, 30, 30}, new long[] {1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {0, 0, 0}, new long[] {0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);

    // we expect 0 moves, because we're enforcing that we can't create
    // copies on the same IP.
    assertEquals(1, doMoves(new CompositeDirector(true, true, true, true), model));

    List<Move> expectedMoves = new ArrayList<>();
    expectedMoves.add(new Move(member1, member2));
    assertEquals(expectedMoves, bucketOperator.bucketMoves);
  }

  /**
   * Tests to make sure that redundancy satisfaction balances between nodes to ensure an even load.
   */
  @Test
  public void testRedundancySatisfactionBalanced() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    // Create some buckets with low redundancy on member 1
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    PartitionMemberInfoImpl details3 =
        buildDetails(member3, 500, 500, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);

    // we expect 4 moves
    assertEquals(4, doMoves(new CompositeDirector(true, true, false, false), model));

    // The bucket creates should alternate between members
    // so that the load is balanced.
    List<Create> expectedCreates = new ArrayList<>();
    expectedCreates.add(new Create(member2, 0));
    expectedCreates.add(new Create(member3, 1));
    expectedCreates.add(new Create(member2, 2));
    expectedCreates.add(new Create(member3, 3));
    assertEquals(expectedCreates, bucketOperator.creates);
  }

  /**
   * Tests to make sure that redundancy satisfaction balances between nodes to ensure an even load.
   */
  @Test
  public void testColocatedRegions() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 12,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    // Create some buckets with low redundancy on member 1
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
            new long[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 250, 250, new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
            new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    PartitionMemberInfoImpl details3 =
        buildDetails(member3, 250, 250, new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
            new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);
    model.addRegion("b", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);

    // we expect 4 moves
    assertEquals(18, doMoves(new CompositeDirector(true, true, false, true), model));

    // The bucket creates should alternate between members
    // so that the load is balanced.
    Set<Create> expectedCreates = new HashSet<>();
    expectedCreates.add(new Create(member2, 0));
    expectedCreates.add(new Create(member3, 1));
    expectedCreates.add(new Create(member2, 2));
    expectedCreates.add(new Create(member3, 3));
    expectedCreates.add(new Create(member2, 4));
    expectedCreates.add(new Create(member3, 5));
    expectedCreates.add(new Create(member2, 6));
    expectedCreates.add(new Create(member3, 7));
    expectedCreates.add(new Create(member2, 8));
    expectedCreates.add(new Create(member3, 9));
    expectedCreates.add(new Create(member2, 10));
    expectedCreates.add(new Create(member3, 11));
    assertEquals(expectedCreates, new HashSet<>(bucketOperator.creates));

    Set<Move> expectedMoves = new HashSet<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member3));
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member3));
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member3));
    assertEquals(expectedMoves, new HashSet<>(bucketOperator.primaryMoves));
  }

  @Test
  public void testIncompleteColocation() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    // Create some buckets with low redundancy on member 1
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    PartitionMemberInfoImpl details3 =
        buildDetails(member3, 500, 500, new long[] {1, 1, 0, 0}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);
    model.addRegion("b", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);
    model.addRegion("c", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);
    // Add a region which is not created on all of the members.

    // Member 3 should not be considered a target for any moves.

    assertEquals(6, doMoves(new CompositeDirector(true, true, false, true), model));

    // Everything should be creatd on member2
    Set<Create> expectedCreates = new HashSet<>();
    expectedCreates.add(new Create(member2, 0));
    expectedCreates.add(new Create(member2, 1));
    expectedCreates.add(new Create(member2, 2));
    expectedCreates.add(new Create(member2, 3));
    assertEquals(expectedCreates, new HashSet<>(bucketOperator.creates));

    Set<Move> expectedMoves = new HashSet<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));
    assertEquals(expectedMoves, new HashSet<>(bucketOperator.primaryMoves));
  }

  /**
   * Test that we enforce local max memory on a per region basis IE if one of the regions has a low
   * lmm, it will prevent a bucket move
   */
  @Test
  public void testColocationEnforceLocalMaxMemory() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    // Create some buckets with low redundancy on member 1
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);


    // Member 2 has a lmm of 2, so it should only accept 2 buckets
    PartitionMemberInfoImpl bDetails1 =
        buildDetails(member1, 2, 2, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    PartitionMemberInfoImpl bDetails2 =
        buildDetails(member2, 2, 2, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    model.addRegion("b", Arrays.asList(bDetails1, bDetails2), new FakeOfflineDetails(), true);


    assertEquals(4, doMoves(new CompositeDirector(true, true, false, true), model));

    // Everything should be create on member2
    Set<Create> expectedCreates = new HashSet<>();
    expectedCreates.add(new Create(member2, 0));
    expectedCreates.add(new Create(member2, 1));
    assertEquals(expectedCreates, new HashSet<>(bucketOperator.creates));

    Set<Move> expectedMoves = new HashSet<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));
    assertEquals(expectedMoves, new HashSet<>(bucketOperator.primaryMoves));
  }

  /**
   * Test that each region individually honors it's enforce local max memory flag.
   */
  @Test
  public void testColocationIgnoreEnforceLocalMaxMemory() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    // Create some buckets with low redundancy on member 1
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);


    // Member 2 has a lmm of 2, so it should only accept 2 buckets
    PartitionMemberInfoImpl bDetails1 =
        buildDetails(member1, 2, 2, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    PartitionMemberInfoImpl bDetails2 =
        buildDetails(member2, 2, 2, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    model.addRegion("b", Arrays.asList(bDetails1, bDetails2), new FakeOfflineDetails(), false);


    assertEquals(6, doMoves(new CompositeDirector(true, true, false, true), model));

    // Everything should be created on member2
    Set<Create> expectedCreates = new HashSet<>();
    expectedCreates.add(new Create(member2, 0));
    expectedCreates.add(new Create(member2, 1));
    expectedCreates.add(new Create(member2, 2));
    expectedCreates.add(new Create(member2, 3));
    assertEquals(expectedCreates, new HashSet<>(bucketOperator.creates));

    Set<Move> expectedMoves = new HashSet<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));
    assertEquals(expectedMoves, new HashSet<>(bucketOperator.primaryMoves));
  }

  /**
   * Test which illustrates the problem with our greedy algorithm. It doesn't necessarily end up
   * with a balanced result.
   *
   * TODO rebalance - change this test or fix the algorithm?
   */
  @Ignore
  @Test
  public void testFoolGreedyAlgorithm() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 50,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    PartitionMemberInfoImpl details1 = buildDetails(member1, 500, 500,
        new long[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0},
        new long[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0});
    PartitionMemberInfoImpl details2 = buildDetails(member2, 500, 500,
        new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1},
        new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1});
    PartitionMemberInfoImpl details3 = buildDetails(member3, 500, 500,
        new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0},
        new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);

    doMoves(new CompositeDirector(true, true, false, false), model);

    // we'd like to have 20 buckets per member, but what we'll find is that member 1
    // will have 15 and 2 and 3 will have 17 and 18.
    for (PartitionMemberInfo details : model.getPartitionedMemberDetails("a")) {
      assertEquals(20, details.getBucketCount());
    }
  }

  /**
   * Tests to make sure that redundancy satisfaction balances between nodes to ensure an even load.
   */
  @Test
  public void testRedundancySatisfactionWithFailures() throws UnknownHostException {
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    final InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);

    MyBucketOperator op = new MyBucketOperator() {
      @Override
      public void createRedundantBucket(InternalDistributedMember targetMember, int i,
          Map<String, Long> colocatedRegionBytes, Completion completion) {
        if (targetMember.equals(member2)) {
          completion.onFailure();
        } else {
          super.createRedundantBucket(targetMember, i, colocatedRegionBytes, completion);
        }
      }
    };

    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(op, 1, 4,
        getAddressComparor(false), Collections.emptySet(), null);

    // Create some buckets with low redundancy on member 1
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    PartitionMemberInfoImpl details3 =
        buildDetails(member3, 500, 500, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);

    // we expect 8 moves
    assertEquals(8, doMoves(new CompositeDirector(true, true, false, false), model));

    // The bucket creates should do all of the creates on member 3
    // because member2 failed.
    List<Create> expectedCreates = new ArrayList<>();
    expectedCreates.add(new Create(member3, 0));
    expectedCreates.add(new Create(member3, 1));
    expectedCreates.add(new Create(member3, 2));
    expectedCreates.add(new Create(member3, 3));
    assertEquals(expectedCreates, op.creates);
  }

  /**
   * Test that redundancy satisfation can handle asynchronous failures and complete the job
   * correctly.
   */
  @Test
  public void testRedundancySatisfactionWithAsyncFailures() throws UnknownHostException {
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);

    BucketOperatorWithFailures operator = new BucketOperatorWithFailures();
    operator.addBadMember(member2);
    bucketOperator = operator;
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 6,
        getAddressComparor(false), Collections.emptySet(), null);
    PartitionMemberInfoImpl details1 = buildDetails(member1, 500, 500,
        new long[] {1, 1, 1, 1, 1, 1}, new long[] {1, 1, 1, 1, 1, 1});
    PartitionMemberInfoImpl details2 = buildDetails(member2, 500, 500,
        new long[] {0, 0, 0, 0, 0, 0}, new long[] {0, 0, 0, 0, 0, 0});
    PartitionMemberInfoImpl details3 = buildDetails(member3, 500, 500,
        new long[] {0, 0, 0, 0, 0, 0}, new long[] {0, 0, 0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);

    Set<PartitionMemberInfo> details = model.getPartitionedMemberDetails("a");
    assertEquals(3, details.size());

    // TODO - make some assertions about what's in the details

    // we expect 6 moves (3 of these will fail)
    assertEquals(6, doMoves(new CompositeDirector(true, true, false, false), model));

    assertEquals(3, bucketOperator.creates.size());
    for (Completion completion : operator.pendingSuccesses) {
      completion.onSuccess();
    }
    for (Completion completion : operator.pendingFailures) {
      completion.onFailure();
    }

    // Now the last two moves will get reattempted to a new location (because the last location
    // failed)
    assertEquals(3, doMoves(new CompositeDirector(true, true, false, false), model));

    List<Create> expectedCreates = new ArrayList<>();
    expectedCreates.add(new Create(member3, 1));
    expectedCreates.add(new Create(member3, 3));
    expectedCreates.add(new Create(member3, 5));
    expectedCreates.add(new Create(member3, 0));
    expectedCreates.add(new Create(member3, 2));
    expectedCreates.add(new Create(member3, 4));
    assertEquals(expectedCreates, bucketOperator.creates);
  }

  /**
   * Very basic test of moving primaries. Creates two nodes and four buckets, with a copy of each
   * bucket on both nodes. All of the primaries are on one node. It expects half the primaries to
   * move to the other node.
   */
  @Test
  public void testMovePrimaries() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 2, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    // Create some imbalanced primaries
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {1, 1, 1, 1}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);

    assertEquals(2, doMoves(new CompositeDirector(false, false, false, true), model));

    assertEquals(Collections.emptyList(), bucketOperator.creates);

    // Two of the primaries should move to member2
    List<Move> expectedMoves = new ArrayList<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));

    assertEquals(expectedMoves, bucketOperator.primaryMoves);
  }

  /**
   * Test that we can move primaries if failures occur during the move. In this case member2 is bad,
   * so primaries should move to member3 instead.
   */
  @Test
  public void testMovePrimariesWithFailures() throws UnknownHostException {
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    final InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    MyBucketOperator op = new MyBucketOperator() {

      @Override
      public boolean movePrimary(InternalDistributedMember source, InternalDistributedMember target,
          int bucketId) {
        if (target.equals(member2)) {
          return false;
        }
        return super.movePrimary(source, target, bucketId);
      }
    };

    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(op, 2, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    // Create some imbalanced primaries
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {1, 1, 1, 1}, new long[] {0, 0, 0, 0});
    PartitionMemberInfoImpl details3 =
        buildDetails(member3, 500, 500, new long[] {1, 1, 1, 1}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);

    assertEquals(8, doMoves(new CompositeDirector(false, false, false, true), model));

    assertEquals(Collections.emptyList(), op.creates);

    // Two of the primaries should move to member2
    List<Move> expectedMoves = new ArrayList<>();
    expectedMoves.add(new Move(member1, member3));
    expectedMoves.add(new Move(member1, member3));

    assertEquals(expectedMoves, op.primaryMoves);
  }

  /**
   * Test of moving primaries when nodes are weighted relative to each other
   */
  @Test
  public void testMovePrimariesWithWeights() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 2, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);

    // member 1 has a lower weight, and all of the primaries
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 1, 500, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    // member 2 has a higher weight
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 3, 500, new long[] {1, 1, 1, 1}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);

    assertEquals(3, doMoves(new CompositeDirector(false, false, false, true), model));

    assertEquals(Collections.emptyList(), bucketOperator.creates);

    // Three of the primaries should move to member2, because it has a higher weight
    List<Move> expectedMoves = new ArrayList<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));

    assertEquals(expectedMoves, bucketOperator.primaryMoves);
  }

  /**
   * This is a more complicated test of primary moving where moving primaries for some buckets then
   * forces other primaries to move.
   *
   * P = primary R = redundant X = not hosting
   *
   * Bucket 0 1 2 3 4 5 6 7 8 Member1 P P P P P P X X X Member2 R R R R R R P P R Member3 X X X X X
   * X R R P
   */
  @Test
  public void testPrimaryShuffle() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 9,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);

    PartitionMemberInfoImpl details1 = buildDetails(member1, 1, 500,
        new long[] {1, 1, 1, 1, 1, 1, 0, 0, 0}, new long[] {1, 1, 1, 1, 1, 1, 0, 0, 0});
    PartitionMemberInfoImpl details2 = buildDetails(member2, 1, 500,
        new long[] {1, 1, 1, 1, 1, 1, 1, 1, 1}, new long[] {0, 0, 0, 0, 0, 0, 1, 1, 0});
    PartitionMemberInfoImpl details3 = buildDetails(member3, 1, 500,
        new long[] {0, 0, 0, 0, 0, 0, 1, 1, 1}, new long[] {0, 0, 0, 0, 0, 0, 0, 0, 1});

    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);


    int moves = doMoves(new CompositeDirector(false, false, false, true), model);
    assertEquals("Actual Moves" + bucketOperator.primaryMoves, 3, moves);


    // Two of the primaries should move off of member1 to member2. And
    // One primaries should move from member 2 to member 3.
    Set<Move> expectedMoves = new HashSet<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member2, member3));
    expectedMoves.add(new Move(member1, member2));

    assertEquals(expectedMoves, new HashSet<>(bucketOperator.primaryMoves));
  }

  /**
   * Test a case where we seem to get into an infinite loop while balancing primaries.
   */
  @Test
  public void testBug39953() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 2, 113,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    InternalDistributedMember member4 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 4);
    // Create some imbalanced primaries
    PartitionMemberInfoImpl details1 = buildDetails(member1, 216, 216,
        new long[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
        new long[] {0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0,
            0, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 0, 0, 0, 0});
    PartitionMemberInfoImpl details2 = buildDetails(member2, 216, 216,
        new long[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
        new long[] {1, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 0, 0, 1, 0,
            1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    PartitionMemberInfoImpl details3 = buildDetails(member3, 216, 216,
        new long[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
        new long[] {0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0,
            0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 1, 0, 0,
            0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0,
            0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1});
    PartitionMemberInfoImpl details4 = buildDetails(member4, 216, 216,
        new long[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
        new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 1,
            0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0,
            1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1,
            0, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3, details4),
        new FakeOfflineDetails(), true);

    assertEquals(0, doMoves(new CompositeDirector(false, false, false, true), model));

    assertEquals(Collections.emptyList(), bucketOperator.creates);

    assertEquals(Collections.emptyList(), bucketOperator.primaryMoves);
  }

  /**
   * Very basic test of moving buckets. Creates two nodes and four buckets, with buckets only on one
   * node. Half of the buckets should move to the other node.
   */
  @Test
  public void testMoveBuckets() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 0, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    // Create some imbalanced nodes
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);

    assertEquals(2, doMoves(new CompositeDirector(false, false, true, true), model));

    assertEquals(Collections.emptyList(), bucketOperator.creates);
    assertEquals(Collections.emptyList(), bucketOperator.primaryMoves);

    // Two of the buckets should move to member2
    List<Move> expectedMoves = new ArrayList<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));

    assertEquals(expectedMoves, bucketOperator.bucketMoves);
  }

  /**
   * Test that moving buckets will work if there are failures while moving buckets member2 refuses
   * the buckets, so the buckets should move to member3
   */
  @Test
  public void testMoveBucketsWithFailures() throws UnknownHostException {
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    final InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);

    MyBucketOperator op = new MyBucketOperator() {
      @Override
      public boolean moveBucket(InternalDistributedMember source, InternalDistributedMember target,
          int id, Map<String, Long> colocatedRegionBytes) {
        if (target.equals(member2)) {
          return false;
        }
        return super.moveBucket(source, target, id, colocatedRegionBytes);
      }
    };

    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(op, 0, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    // Create some imbalanced nodes
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 1}, new long[] {1, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    PartitionMemberInfoImpl details3 =
        buildDetails(member3, 500, 500, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);

    assertEquals(8, doMoves(new CompositeDirector(false, false, true, true), model));

    assertEquals(Collections.emptyList(), op.creates);
    assertEquals(Collections.emptyList(), op.primaryMoves);

    // Two of the buckets should move to member2
    List<Move> expectedMoves = new ArrayList<>();
    expectedMoves.add(new Move(member1, member3));
    expectedMoves.add(new Move(member1, member3));

    assertEquals(expectedMoves, op.bucketMoves);
  }

  /**
   * Test to make sure that we honor the weight of a node while moving buckets.
   */
  @Test
  public void testMoveBucketsWithWeights() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 0, 6,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    // Create some imbalanced nodes
    PartitionMemberInfoImpl details1 = buildDetails(member1, 250, 250,
        new long[] {1, 1, 1, 1, 1, 1}, new long[] {1, 1, 1, 1, 1, 1});
    PartitionMemberInfoImpl details2 = buildDetails(member2, 500, 500,
        new long[] {0, 0, 0, 0, 0, 0}, new long[] {0, 0, 0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);

    assertEquals(4, doMoves(new CompositeDirector(false, false, true, true), model));

    assertEquals(Collections.emptyList(), bucketOperator.creates);
    assertEquals(Collections.emptyList(), bucketOperator.primaryMoves);

    // Four of the buckets should move to member2, because
    // member2 has twice the weight as member1.
    List<Move> expectedMoves = new ArrayList<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));

    assertEquals(expectedMoves, bucketOperator.bucketMoves);
  }

  /**
   * Test to make sure we honor the size of buckets when choosing which buckets to move.
   */
  @Test
  public void testMoveBucketsWithSizes() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 0, 6,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    // Create some imbalanced nodes
    PartitionMemberInfoImpl details1 = buildDetails(member1, 500, 500,
        new long[] {3, 1, 1, 1, 1, 1}, new long[] {1, 1, 1, 1, 1, 1});
    PartitionMemberInfoImpl details2 = buildDetails(member2, 500, 500,
        new long[] {0, 0, 0, 0, 0, 0}, new long[] {0, 0, 0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2), new FakeOfflineDetails(), true);

    assertEquals(4, doMoves(new CompositeDirector(false, false, true, true), model));

    assertEquals(Collections.emptyList(), bucketOperator.creates);
    assertEquals(Collections.emptyList(), bucketOperator.primaryMoves);

    // Four of the buckets should move to member2, because
    // member1 has 1 bucket that is size 3.
    List<Move> expectedMoves = new ArrayList<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));

    assertEquals(expectedMoves, bucketOperator.bucketMoves);
  }

  /**
   * Test to move buckets with redundancy. Makes sure that buckets and primaries are balanced
   */
  @Test
  public void testMoveBucketsWithRedundancy() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 2, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    InternalDistributedMember member4 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 4);
    // Create some imbalanced nodes
    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 500, 500, new long[] {1, 1, 1, 1}, new long[] {1, 1, 0, 0});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 500, 500, new long[] {1, 1, 1, 1}, new long[] {0, 0, 1, 0});
    PartitionMemberInfoImpl details3 =
        buildDetails(member3, 500, 500, new long[] {1, 1, 1, 1}, new long[] {0, 0, 0, 1});
    PartitionMemberInfoImpl details4 =
        buildDetails(member4, 500, 500, new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3, details4),
        new FakeOfflineDetails(), true);

    doMoves(new CompositeDirector(false, false, true, true), model);

    assertEquals(Collections.emptyList(), bucketOperator.creates);

    // One bucket should move from each member to member4
    Set<Move> expectedMoves = new HashSet<>();
    expectedMoves.add(new Move(member1, member4));
    expectedMoves.add(new Move(member2, member4));
    expectedMoves.add(new Move(member3, member4));

    assertEquals(expectedMoves, new HashSet<>(bucketOperator.bucketMoves));

    // We don't know how many primaries will move, because
    // the move buckets algorithm could move the primary or
    // it could move a redundant copy. But after we're done, we should
    // only have one primary per member.
    Set<PartitionMemberInfo> detailSet = model.getPartitionedMemberDetails("a");
    for (PartitionMemberInfo member : detailSet) {
      assertEquals(1, member.getPrimaryCount());
      assertEquals(3, member.getBucketCount());
    }
  }

  /**
   * Test to move buckets with some large buckets (to make sure there are no issues with buffer
   * overflow); Makes sure that buckets and primaries are balanced
   */
  @Test
  public void testMoveLargeBucketsWithRedundancy() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 2, 4,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    InternalDistributedMember member4 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 4);
    // Create some imbalanced nodes

    long bigBucket = Integer.MAX_VALUE * 5L + 10L;
    PartitionMemberInfoImpl details1 = buildDetails(member1, 500, Long.MAX_VALUE,
        new long[] {bigBucket, bigBucket, bigBucket, bigBucket}, new long[] {1, 1, 0, 0});
    PartitionMemberInfoImpl details2 = buildDetails(member2, 500, Long.MAX_VALUE,
        new long[] {bigBucket, bigBucket, bigBucket, bigBucket}, new long[] {0, 0, 1, 0});
    PartitionMemberInfoImpl details3 = buildDetails(member3, 500, Long.MAX_VALUE,
        new long[] {bigBucket, bigBucket, bigBucket, bigBucket}, new long[] {0, 0, 0, 1});
    PartitionMemberInfoImpl details4 = buildDetails(member4, 500, Long.MAX_VALUE,
        new long[] {0, 0, 0, 0}, new long[] {0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3, details4),
        new FakeOfflineDetails(), true);

    doMoves(new CompositeDirector(false, false, true, true), model);

    assertEquals(Collections.emptyList(), bucketOperator.creates);

    // One bucket should move from each member to member4
    Set<Move> expectedMoves = new HashSet<>();
    expectedMoves.add(new Move(member1, member4));
    expectedMoves.add(new Move(member2, member4));
    expectedMoves.add(new Move(member3, member4));

    assertEquals(expectedMoves, new HashSet<>(bucketOperator.bucketMoves));

    // We don't know how many primaries will move, because
    // the move buckets algorithm could move the primary or
    // it could move a redundant copy. But after we're done, we should
    // only have one primary per member.
    Set<PartitionMemberInfo> detailSet = model.getPartitionedMemberDetails("a");
    for (PartitionMemberInfo member : detailSet) {
      assertEquals(1, member.getPrimaryCount());
      assertEquals(3, member.getBucketCount());
    }
  }

  /**
   * Test to make sure that moving buckets honors size restrictions for VMs.
   */
  @Test
  public void testMoveBucketsWithSizeLimits() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 0, 6,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    // Create some imbalanced nodes
    PartitionMemberInfoImpl details1 = buildDetails(member1, 50, 50,
        new long[] {30, 30, 30, 0, 0, 0}, new long[] {1, 1, 1, 0, 0, 0});
    PartitionMemberInfoImpl details2 = buildDetails(member2, 50, 50,
        new long[] {0, 0, 0, 10, 10, 10}, new long[] {0, 0, 0, 1, 1, 1});
    // this member has a lower size that can't fit buckets of size 30
    PartitionMemberInfoImpl details3 =
        buildDetails(member3, 50, 20, new long[] {0, 0, 0, 0, 0, 0}, new long[] {0, 0, 0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);

    assertEquals(3, doMoves(new CompositeDirector(false, false, true, true), model));

    assertEquals(Collections.emptyList(), bucketOperator.creates);

    // One bucket should move from each member to member4
    Set<Move> expectedMoves = new HashSet<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member2, member3));
    expectedMoves.add(new Move(member2, member3));

    assertEquals(expectedMoves, new HashSet<>(bucketOperator.bucketMoves));

    Set<PartitionMemberInfo> detailSet = model.getPartitionedMemberDetails("a");
    for (PartitionMemberInfo member : detailSet) {
      assertEquals(2, member.getPrimaryCount());
      assertEquals(2, member.getBucketCount());
    }
  }

  /**
   * Test to make sure that moving buckets honors size critical members
   */
  @Test
  public void testMoveBucketsWithCriticalMember() throws UnknownHostException {
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 0, 6,
        getAddressComparor(false), Collections.singleton(member3), null);
    // Create some imbalanced nodes
    PartitionMemberInfoImpl details1 = buildDetails(member1, 50, 50,
        new long[] {10, 10, 10, 10, 10, 10}, new long[] {1, 1, 1, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 50, 50, new long[] {0, 0, 0, 0, 0, 0}, new long[] {0, 0, 0, 0, 0, 0});
    // this member has a critical heap
    PartitionMemberInfoImpl details3 =
        buildDetails(member3, 50, 50, new long[] {0, 0, 0, 0, 0, 0}, new long[] {0, 0, 0, 0, 0, 0});
    model.addRegion("a", Arrays.asList(details1, details2, details3), new FakeOfflineDetails(),
        true);

    assertEquals(3, doMoves(new CompositeDirector(false, false, true, true), model));

    assertEquals(Collections.emptyList(), bucketOperator.creates);

    // The buckets should only move to member2, because member3 is critical
    Set<Move> expectedMoves = new HashSet<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));

    assertEquals(expectedMoves, new HashSet<>(bucketOperator.bucketMoves));
  }

  /**
   * Test to make sure two runs with the same information perform the same moves.
   */
  @Test
  public void testRepeatableRuns() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 0, 113,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 22893);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 25655);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 22959);
    InternalDistributedMember member4 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 22984);
    InternalDistributedMember member5 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 28609);
    InternalDistributedMember member6 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 22911);
    InternalDistributedMember member7 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 29562);
    PartitionMemberInfoImpl details1 = buildDetails(member1, 50 * 1024 * 1024, 50 * 1024 * 1024,
        new long[] {23706, 0, 23347, 23344, 0, 0, 0, 11386, 0, 0, 0, 0, 0, 10338, 0, 9078, 6413,
            10411, 5297, 1226, 0, 2594, 2523, 0, 1297, 0, 3891, 2523, 0, 0, 2594, 0, 1297, 0, 1297,
            2594, 1, 0, 10375, 5188, 9078, 0, 1297, 0, 0, 1226, 1, 1, 0, 0, 1297, 11672, 0, 0, 0, 0,
            7782, 0, 11673, 0, 2594, 1, 0, 2593, 3891, 1, 0, 7711, 7710, 2594, 0, 6485, 0, 1, 7711,
            6485, 7711, 3891, 1297, 0, 10303, 2594, 3820, 0, 2523, 3999, 0, 1, 0, 2522, 1, 5188,
            5188, 0, 2594, 3891, 2523, 2594, 0, 1297, 1, 1, 1226, 0, 1297, 0, 3891, 1226, 2522,
            11601, 10376, 0, 2594},
        new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0});
    PartitionMemberInfoImpl details2 = buildDetails(member2, 50 * 1024 * 1024, 50 * 1024 * 1024,
        new long[] {0, 24674, 0, 23344, 0, 19312, 19421, 11386, 7889, 0, 0, 6413, 12933, 10338,
            18088, 9078, 0, 0, 0, 1226, 0, 2594, 0, 0, 0, 2594, 0, 2523, 0, 1, 0, 0, 1297, 0, 0, 0,
            0, 2594, 0, 5188, 9078, 0, 0, 0, 1, 1226, 1, 0, 1297, 5187, 0, 0, 0, 0, 0, 1, 0, 11602,
            0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 7710, 0, 10304, 6485, 0, 0, 0, 0, 0, 3891, 0, 0, 10303, 0,
            0, 1, 2523, 3999, 0, 0, 1, 0, 0, 5188, 0, 5116, 2594, 3891, 2523, 0, 2522, 1297, 1, 0,
            0, 1297, 0, 1297, 3891, 1226, 2522, 0, 10376, 0, 0},
        new long[] {0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0});
    PartitionMemberInfoImpl details3 = buildDetails(member3, 50 * 1024 * 1024, 50 * 1024 * 1024,
        new long[] {23706, 24674, 0, 0, 20901, 0, 19421, 0, 7889, 11708, 0, 0, 12933, 10338, 18088,
            0, 6413, 10411, 5297, 0, 7782, 2594, 0, 1297, 0, 2594, 3891, 0, 2523, 1, 0, 2523, 1297,
            1297, 1297, 0, 1, 2594, 0, 0, 0, 1297, 0, 1297, 1, 0, 0, 1, 1297, 5187, 0, 0, 13007, 0,
            11672, 0, 7782, 11602, 0, 0, 0, 0, 2594, 2593, 3891, 1, 7782, 7711, 0, 0, 10304, 0,
            7711, 0, 7711, 6485, 7711, 0, 1297, 1297, 10303, 2594, 3820, 1, 2523, 0, 1, 0, 1, 2522,
            1, 5188, 5188, 5116, 2594, 3891, 2523, 2594, 0, 0, 0, 1, 1226, 1297, 1297, 1297, 0, 0,
            2522, 0, 0, 2523, 0},
        new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 0,
            0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0});
    PartitionMemberInfoImpl details4 = buildDetails(member4, 50 * 1024 * 1024, 50 * 1024 * 1024,
        new long[] {23706, 24674, 23347, 0, 20901, 19312, 0, 0, 7889, 11708, 12933, 6413, 0, 0, 0,
            9078, 6413, 10411, 5297, 1226, 7782, 0, 2523, 1297, 0, 0, 0, 2523, 0, 0, 2594, 2523, 0,
            1297, 0, 2594, 1, 0, 10375, 0, 0, 1297, 1297, 1297, 1, 1226, 1, 0, 1297, 0, 1297, 0,
            13007, 7781, 11672, 1, 7782, 11602, 11673, 5225, 2594, 1, 2594, 2593, 3891, 0, 7782, 0,
            7710, 0, 10304, 0, 0, 1, 7711, 6485, 7711, 0, 0, 0, 0, 0, 3820, 1, 0, 3999, 1, 1, 1, 0,
            1, 0, 5188, 0, 0, 3891, 0, 0, 2522, 1297, 1, 0, 0, 0, 1297, 1297, 0, 0, 2522, 11601,
            10376, 2523, 2594},
        new long[] {1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0,
            0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0});
    PartitionMemberInfoImpl details5 = buildDetails(member5, 50 * 1024 * 1024, 50 * 1024 * 1024,
        new long[] {23706, 24674, 0, 23344, 0, 0, 19421, 0, 0, 11708, 12933, 6413, 12933, 10338,
            18088, 0, 0, 10411, 0, 1226, 7782, 2594, 2523, 1297, 1297, 2594, 3891, 0, 2523, 1, 2594,
            2523, 0, 1297, 1297, 2594, 0, 2594, 10375, 0, 0, 1297, 0, 1297, 0, 1226, 1, 1, 0, 5187,
            1297, 11672, 13007, 7781, 11672, 1, 0, 11602, 11673, 5225, 2594, 1, 0, 2593, 3891, 1,
            7782, 0, 0, 2594, 0, 6485, 7711, 1, 7711, 0, 7711, 3891, 0, 1297, 0, 2594, 3820, 0,
            2523, 0, 1, 1, 0, 2522, 0, 0, 0, 5116, 0, 0, 0, 0, 2522, 0, 0, 1, 0, 1297, 1297, 1297,
            3891, 0, 0, 0, 0, 0, 2594},
        new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1,
            0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0,
            0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0});
    PartitionMemberInfoImpl details6 = buildDetails(member6, 50 * 1024 * 1024, 50 * 1024 * 1024,
        new long[] {0, 0, 23347, 0, 20901, 19312, 0, 11386, 7889, 0, 12933, 6413, 0, 0, 18088, 0,
            6413, 0, 5297, 0, 7782, 0, 2523, 0, 1297, 2594, 0, 0, 2523, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 5188, 9078, 0, 1297, 0, 1, 0, 0, 1, 0, 0, 1297, 11672, 13007, 7781, 0, 0, 0, 0, 0,
            5225, 0, 0, 2594, 0, 0, 0, 7782, 7711, 0, 2594, 0, 0, 7711, 0, 0, 0, 0, 0, 1297, 1297,
            0, 0, 0, 1, 0, 3999, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 2523, 2594, 0, 0, 0, 0, 1226, 1297,
            0, 0, 3891, 1226, 0, 11601, 10376, 2523, 0},
        new long[] {0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0,
            0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0});
    PartitionMemberInfoImpl details7 = buildDetails(member7, 50 * 1024 * 1024, 50 * 1024 * 1024,
        new long[] {0, 0, 23347, 23344, 20901, 19312, 19421, 11386, 0, 11708, 12933, 0, 12933, 0, 0,
            9078, 0, 0, 0, 0, 0, 0, 0, 1297, 1297, 0, 3891, 2523, 2523, 1, 2594, 2523, 1297, 1297,
            1297, 2594, 1, 2594, 10375, 5188, 9078, 1297, 1297, 1297, 0, 0, 0, 0, 1297, 5187, 0,
            11672, 0, 7781, 11672, 1, 7782, 0, 11673, 5225, 2594, 0, 2594, 0, 0, 1, 0, 7711, 7710,
            2594, 10304, 6485, 7711, 1, 0, 6485, 0, 3891, 1297, 1297, 10303, 2594, 0, 0, 0, 0, 0, 1,
            0, 2522, 0, 5188, 5188, 5116, 2594, 0, 0, 2594, 2522, 1297, 1, 1, 1226, 0, 0, 0, 0,
            1226, 0, 11601, 0, 2523, 2594},
        new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 1, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1});

    model.addRegion("a",
        Arrays.asList(details1, details2, details3, details4, details5, details6, details7),
        new FakeOfflineDetails(), true);

    doMoves(new CompositeDirector(false, false, true, true), model);
    List<Move> bucketMoves1 = new ArrayList<>(bucketOperator.bucketMoves);
    List<Create> bucketCreates1 = new ArrayList<>(bucketOperator.creates);
    List<Move> primaryMoves1 = new ArrayList<>(bucketOperator.primaryMoves);
    bucketOperator.bucketMoves.clear();
    bucketOperator.creates.clear();
    bucketOperator.primaryMoves.clear();


    model = new PartitionedRegionLoadModel(bucketOperator, 0, 113, getAddressComparor(false),
        Collections.emptySet(), null);
    model.addRegion("a",
        Arrays.asList(details1, details2, details4, details3, details5, details6, details7),
        new FakeOfflineDetails(), true);

    doMoves(new CompositeDirector(false, false, true, true), model);
    assertEquals(bucketCreates1, bucketOperator.creates);
    assertEquals(bucketMoves1, bucketOperator.bucketMoves);
    assertEquals(primaryMoves1, bucketOperator.primaryMoves);
  }

  /**
   * This is more of a simulation than a test
   */
  @Ignore("not a real test")
  @Test
  public void testRandom() throws UnknownHostException {
    long seed = System.nanoTime();
    System.out.println("random seed=" + seed);

    Random rand = new Random(seed);
    int MAX_MEMBERS = 20;
    int MAX_BUCKETS = 200;
    int MAX_REDUNDANCY = 3;
    float IMAIRED_PERCENTAGE = 0.1f; // probability that a bucket will have impaired redundancy
    int AVERAGE_BUCKET_SIZE = 10;
    int AVERAGE_MAX_MEMORY = 200;
    int members = rand.nextInt(MAX_MEMBERS) + 2;
    int buckets = rand.nextInt(MAX_BUCKETS) + members;
    int redundancy = rand.nextInt(MAX_REDUNDANCY);
    long[][] bucketLocations = new long[members][buckets];
    long[][] bucketPrimaries = new long[members][buckets];
    for (int i = 0; i < buckets; i++) {
      int bucketSize = rand.nextInt(AVERAGE_BUCKET_SIZE * 2);

      int remainingCopies = redundancy + 1;
      if (rand.nextFloat() <= IMAIRED_PERCENTAGE) {
        remainingCopies = redundancy == 0 ? 0 : rand.nextInt(redundancy);
      }

      if (remainingCopies > 0) {
        int primary = rand.nextInt(members);
        bucketLocations[primary][i] = bucketSize;
        bucketPrimaries[primary][i] = 1;
        remainingCopies--;
      }

      while (remainingCopies > 0) {
        int member = rand.nextInt(members);
        if (bucketLocations[member][i] == 0) {
          bucketLocations[member][i] = bucketSize;
          remainingCopies--;
        }
      }
    }

    PartitionedRegionLoadModel model =
        new PartitionedRegionLoadModel(bucketOperator, redundancy, buckets,
            getAddressComparor(false), Collections.emptySet(), null);
    PartitionMemberInfoImpl[] details = new PartitionMemberInfoImpl[members];
    for (int i = 0; i < members; i++) {
      InternalDistributedMember member =
          new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), i);
      int maxMemory = rand.nextInt(AVERAGE_MAX_MEMORY * 2);
      details[i] =
          buildDetails(member, maxMemory, maxMemory, bucketLocations[i], bucketPrimaries[i]);
    }

    model.addRegion("a", Arrays.asList(details), new FakeOfflineDetails(), true);

    doMoves(new CompositeDirector(true, true, true, true), model);
  }

  /**
   * This test makes sure that we rebalance correctly with multiple levels of colocation. See bug
   * #40943
   */
  @Test
  @Parameters({"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"})
  @TestCaseName("{method}({params})")
  public void testManyColocatedRegions(int colocatedRegions) throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 5,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);
    InternalDistributedMember member3 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 3);
    // Create some buckets with low redundancy on member 1
    PartitionMemberInfoImpl details1 = buildDetails(member1, 200, 200,
        new long[] {30, 23, 28, 30, 23}, new long[] {1, 1, 1, 0, 0});
    PartitionMemberInfoImpl details2 = buildDetails(member2, 200, 200,
        new long[] {30, 23, 28, 30, 23}, new long[] {0, 0, 0, 1, 1});
    PartitionMemberInfoImpl details3 =
        buildDetails(member3, 200, 200, new long[] {0, 0, 0, 0, 0}, new long[] {0, 0, 0, 0, 0});
    model.addRegion("primary", Arrays.asList(details1, details2, details3),
        new FakeOfflineDetails(), true);
    for (int i = 0; i < colocatedRegions; i++) {
      model.addRegion("colocated" + i, Arrays.asList(details1, details2, details3),
          new FakeOfflineDetails(), true);
    }

    // we expect 3 moves
    assertEquals(4, doMoves(new CompositeDirector(true, true, true, true), model));
  }

  /**
   * Test to make sure than redundancy satisfaction ignores offline members
   */
  @Test
  public void testRedundancySatisfactionWithOfflineMembers() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 5,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);

    PartitionMemberInfoImpl details1 =
        buildDetails(member1, 200, 200, new long[] {30, 0, 28, 30, 23}, new long[] {1, 0, 1, 1, 1});
    PartitionMemberInfoImpl details2 =
        buildDetails(member2, 200, 200, new long[] {0, 23, 0, 0, 0}, new long[] {0, 1, 0, 0, 0});

    // Two buckets have an offline members
    Set<PersistentMemberID> o = Collections.singleton(new PersistentMemberID());
    Set<PersistentMemberID> x = Collections.emptySet();
    final OfflineMemberDetailsImpl offlineDetails =
        new OfflineMemberDetailsImpl(new Set[] {x, x, o, o, x});

    model.addRegion("primary", Arrays.asList(details1, details2), offlineDetails, true);

    assertEquals(3, doMoves(new CompositeDirector(true, true, false, false), model));

    List<Create> expectedCreates = new ArrayList<>();
    expectedCreates.add(new Create(member2, 0));
    expectedCreates.add(new Create(member1, 1));
    expectedCreates.add(new Create(member2, 4));
    assertEquals(expectedCreates, bucketOperator.creates);
  }

  @Test
  public void testRebalancingWithOfflineMembers() throws UnknownHostException {
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 1, 6,
        getAddressComparor(false), Collections.emptySet(), null);
    InternalDistributedMember member1 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    InternalDistributedMember member2 =
        new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 2);

    PartitionMemberInfoImpl details1 = buildDetails(member1, 480, 480,
        new long[] {1, 1, 1, 1, 1, 1}, new long[] {1, 1, 1, 1, 1, 1});
    PartitionMemberInfoImpl details2 = buildDetails(member2, 480, 480,
        new long[] {0, 0, 0, 0, 0, 0}, new long[] {0, 0, 0, 0, 0, 0});

    // Each bucket has an offline member
    Set<PersistentMemberID> o = Collections.singleton(new PersistentMemberID());
    Set<PersistentMemberID> x = Collections.emptySet();
    final OfflineMemberDetailsImpl offlineDetails =
        new OfflineMemberDetailsImpl(new Set[] {o, o, o, o, o, o});

    model.addRegion("primary", Arrays.asList(details1, details2), offlineDetails, true);
    assertEquals(3, doMoves(new CompositeDirector(true, true, true, true), model));

    Collection<Move> expectedMoves = new ArrayList<>();
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));
    expectedMoves.add(new Move(member1, member2));
    assertEquals(expectedMoves, bucketOperator.bucketMoves);
  }

  private int doMoves(RebalanceDirector director, PartitionedRegionLoadModel model) {
    double initialVariance = model.getVarianceForTest();
    double initialPrimaryVariance = model.getPrimaryVarianceForTest();
    if (DEBUG) {
      System.out.println("Initial Model\n" + model + "\nVariance= " + initialVariance
          + ", Primary variance=" + initialPrimaryVariance + "\n---------------");
    }
    model.initialize();
    director.initialize(model);
    int moveCount = 0;
    while (director.nextStep() && moveCount < MAX_MOVES) {

      moveCount++;
      double variance = model.getVarianceForTest();
      double primaryVariance = model.getPrimaryVarianceForTest();
      if (DEBUG) {
        System.out.println("---------------\nMove " + moveCount + "\n" + model + "\nVariance= "
            + variance + ", Primary variance=" + primaryVariance + "\n---------------");
      }

      if (MoveType.MOVE_BUCKET.equals(bucketOperator.lastMove)) {
        // Assert that the variance is monotonically descreasing
        // (ie our balance is improving after each step)
        assertTrue("Moving a bucket did not improve variance. Old Variance " + initialVariance
            + " new variance " + variance, initialVariance > variance);
      }
      if (MoveType.MOVE_PRIMARY.equals(bucketOperator.lastMove)) {
        assertTrue("Moving a primary did not improve variance. Old Variance "
            + initialPrimaryVariance + " new variance " + primaryVariance,
            initialPrimaryVariance > primaryVariance);
      }

      initialVariance = variance;
      initialPrimaryVariance = primaryVariance;
    }

    return moveCount;
  }

  private PartitionMemberInfoImpl buildDetails(InternalDistributedMember id, float weight,
      long localMaxMemory, long[] loads, long[] primaryLoads) {
    PRLoad load1 = new PRLoad(loads.length, weight);
    int size = 0;
    int primaryCount = 0;
    int bucketCount = 0;
    long[] bucketSizes = new long[loads.length];
    for (int i = 0; i < loads.length; i++) {
      load1.addBucket(i, loads[i], primaryLoads[i]);
      bucketSizes[i] = loads[i];
      size += bucketSizes[i];
      if (loads[i] != 0) {
        bucketCount++;
      }
      if (primaryLoads[i] != 0) {
        primaryCount++;
      }
    }
    return new PartitionMemberInfoImpl(id, localMaxMemory, size,
        bucketCount, primaryCount, load1, bucketSizes);
  }

  private static AddressComparor getAddressComparor(final boolean enforceUniqueZones) {
    return new AddressComparor() {

      @Override
      public boolean areSameZone(InternalDistributedMember member1,
          InternalDistributedMember member2) {
        return member1.getInetAddress().equals(member2.getInetAddress());
      }

      @Override
      public boolean enforceUniqueZones() {
        return enforceUniqueZones;
      }
    };
  }

  private static class Create {

    private final InternalDistributedMember targetMember;
    private final int bucketId;

    public Create(InternalDistributedMember targetMember, int bucketId) {
      this.targetMember = targetMember;
      this.bucketId = bucketId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + bucketId;
      result = prime * result + (targetMember == null ? 0 : targetMember.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Create other = (Create) obj;
      if (bucketId != other.bucketId)
        return false;
      if (targetMember == null) {
        if (other.targetMember != null)
          return false;
      } else if (!targetMember.equals(other.targetMember))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "Create[member=" + targetMember + ",bucketId=" + bucketId + "]";
    }
  }

  private static class Remove {

    private final InternalDistributedMember targetMember;
    private final int bucketId;

    public Remove(InternalDistributedMember targetMember, int bucketId) {
      this.targetMember = targetMember;
      this.bucketId = bucketId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + bucketId;
      result = prime * result + (targetMember == null ? 0 : targetMember.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Create other = (Create) obj;
      if (bucketId != other.bucketId)
        return false;
      if (targetMember == null) {
        if (other.targetMember != null)
          return false;
      } else if (!targetMember.equals(other.targetMember))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "Remove[member=" + targetMember + ",bucketId=" + bucketId + "]";
    }
  }

  private static class Move {

    private final InternalDistributedMember sourceMember;
    private final InternalDistributedMember targetMember;

    public Move(InternalDistributedMember sourceMember, InternalDistributedMember targetMember) {
      this.sourceMember = sourceMember;
      this.targetMember = targetMember;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (sourceMember == null ? 0 : sourceMember.hashCode());
      result = prime * result + (targetMember == null ? 0 : targetMember.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Move other = (Move) obj;
      if (sourceMember == null) {
        if (other.sourceMember != null)
          return false;
      } else if (!sourceMember.equals(other.sourceMember))
        return false;
      if (targetMember == null) {
        if (other.targetMember != null)
          return false;
      } else if (!targetMember.equals(other.targetMember))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "Move[source=" + sourceMember + ",target=" + targetMember + "]";
    }
  }

  private static class MyBucketOperator extends SimulatedBucketOperator {

    private List<Create> creates = new ArrayList<>();
    private List<Remove> removes = new ArrayList<>();
    private List<Move> primaryMoves = new ArrayList<>();
    private List<Move> bucketMoves = new ArrayList<>();
    private MoveType lastMove;

    @Override
    public void createRedundantBucket(InternalDistributedMember targetMember, int i,
        Map<String, Long> colocatedRegionBytes, Completion completion) {
      creates.add(new Create(targetMember, i));
      if (DEBUG) {
        System.out.println("Created bucket " + i + " on " + targetMember);
      }
      lastMove = MoveType.CREATE;
      completion.onSuccess();
    }

    @Override
    public boolean movePrimary(InternalDistributedMember source, InternalDistributedMember target,
        int bucketId) {
      primaryMoves.add(new Move(source, target));
      if (DEBUG) {
        System.out.println("Moved primary " + bucketId + " from " + source + " to " + target);
      }
      lastMove = MoveType.MOVE_PRIMARY;
      return true;
    }

    @Override
    public boolean moveBucket(InternalDistributedMember source, InternalDistributedMember target,
        int id, Map<String, Long> colocatedRegionBytes) {
      bucketMoves.add(new Move(source, target));
      if (DEBUG) {
        System.out.println("Moved bucket " + id + " from " + source + " to " + target);
      }
      lastMove = MoveType.MOVE_BUCKET;
      return true;
    }

    @Override
    public boolean removeBucket(InternalDistributedMember memberId, int id,
        Map<String, Long> colocatedRegionSizes) {
      removes.add(new Remove(memberId, id));
      if (DEBUG) {
        System.out.println("Moved bucket " + id + " from " + memberId);
      }
      lastMove = MoveType.REMOVE;
      return true;
    }
  }

  private static class BucketOperatorWithFailures extends MyBucketOperator {

    private List<Completion> pendingSuccesses = new ArrayList<>();
    private List<Completion> pendingFailures = new ArrayList<>();
    private Set<InternalDistributedMember> badMembers = new HashSet<>();

    public void addBadMember(InternalDistributedMember member) {
      badMembers.add(member);
    }

    @Override
    public void createRedundantBucket(InternalDistributedMember targetMember, int i,
        Map<String, Long> colocatedRegionBytes, Completion completion) {
      if (badMembers.contains(targetMember)) {
        pendingFailures.add(completion);
      } else {
        super.createRedundantBucket(targetMember, i, colocatedRegionBytes, new Completion() {

          @Override
          public void onSuccess() {}

          @Override
          public void onFailure() {}
        });

        pendingSuccesses.add(completion);
      }
    }
  }

  private enum MoveType {
    CREATE, MOVE_PRIMARY, MOVE_BUCKET, REMOVE;
  }

  private static class FakeOfflineDetails implements OfflineMemberDetails {

    private Set<PersistentMemberID> offlineMembers;

    public FakeOfflineDetails() {
      this(Collections.emptySet());
    }

    public FakeOfflineDetails(Set<PersistentMemberID> offlineMembers) {
      this.offlineMembers = offlineMembers;
    }

    @Override
    public Set<PersistentMemberID> getOfflineMembers(int bucketId) {
      return offlineMembers;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      offlineMembers = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeObject(offlineMembers, out);
    }
  }
}
