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
package org.apache.geode.cache.partitioned.rebalance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.OfflineMemberDetails;
import org.apache.geode.internal.cache.partitioned.PRLoad;
import org.apache.geode.internal.cache.partitioned.PartitionMemberInfoImpl;
import org.apache.geode.internal.cache.partitioned.rebalance.SimulatedBucketOperator;
import org.apache.geode.internal.cache.partitioned.rebalance.model.AddressComparor;
import org.apache.geode.internal.cache.partitioned.rebalance.model.PartitionedRegionLoadModel;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;

class RebalanceModelBuilder {
  private static final int DEFAULT_MEAN_BUCKET_SIZE = 100;

  private final Random random = new Random();
  private final int members;
  private final int buckets;
  private int newMembers;
  private int bucketSizeDeviation;

  RebalanceModelBuilder(int members, int buckets) {
    this.members = members;
    this.buckets = buckets;
  }

  RebalanceModelBuilder withNewMembers(int newMembers) {
    this.newMembers = newMembers;
    return this;
  }

  RebalanceModelBuilder withBucketSizeStandardDeviation(int deviation) {
    bucketSizeDeviation = deviation;
    return this;
  }

  PartitionedRegionLoadModel createModel() throws UnknownHostException {
    SimulatedBucketOperator bucketOperator = new SimulatedBucketOperator();
    PartitionedRegionLoadModel model = new PartitionedRegionLoadModel(bucketOperator, 0, buckets,
        comparor, Collections.emptySet(), null);
    double bucketsPerMember = (double) buckets / members;
    int bucketOffset = 0;
    List<PartitionMemberInfoImpl> members = new ArrayList<>();

    for (int memberId = 0; memberId < this.members; memberId++) {
      int bucketsOnMember = getBucketsOnMember(bucketsPerMember, memberId);
      long[] loads = new long[buckets];
      long[] primaryLoads = new long[buckets];
      for (int bucketId = bucketOffset; bucketId < bucketOffset + bucketsOnMember; bucketId++) {
        loads[bucketId] = getBucketSize(memberId);
        primaryLoads[bucketId] = 1;
      }

      InternalDistributedMember member =
          new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), memberId);
      PartitionMemberInfoImpl memberInfo =
          buildDetails(member, 500, Integer.MAX_VALUE, loads, primaryLoads);
      members.add(memberInfo);

      bucketOffset += bucketsOnMember;
    }

    members.addAll(getNewMembers());
    model.addRegion("a", members, new FakeOfflineDetails(), true);
    return model;
  }

  private int getBucketsOnMember(double bucketsPerMember, double memberId) {
    if (memberId / members < (bucketsPerMember - (int) bucketsPerMember)) {
      return (int) Math.ceil(bucketsPerMember);
    } else {
      return (int) Math.floor(bucketsPerMember);
    }
  }

  private int getBucketSize(int memberId) {
    int loadDeviation = (int) (random.nextGaussian() * bucketSizeDeviation);

    if (memberId % 2 == 0) {
      return DEFAULT_MEAN_BUCKET_SIZE + Math.abs(loadDeviation);
    } else {
      return DEFAULT_MEAN_BUCKET_SIZE - Math.abs(loadDeviation);
    }
  }

  private List<PartitionMemberInfoImpl> getNewMembers() throws UnknownHostException {
    List<PartitionMemberInfoImpl> members = new ArrayList<>();
    for (int i = 0; i < newMembers; i++) {
      InternalDistributedMember newMember =
          new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), this.members + i);
      PartitionMemberInfoImpl newMemberInfo =
          buildDetails(newMember, 500, Integer.MAX_VALUE, new long[buckets], new long[buckets]);
      members.add(newMemberInfo);
    }
    return members;
  }


  private PartitionMemberInfoImpl buildDetails(InternalDistributedMember id, float weight,
      long localMaxMemory, long[] loads, long[] primaryLoads) {
    PRLoad prLoad = new PRLoad(loads.length, weight);
    int size = 0;
    int primaryCount = 0;
    int bucketCount = 0;
    long[] bucketSizes = new long[loads.length];
    for (int bucketId = 0; bucketId < loads.length; bucketId++) {
      prLoad.addBucket(bucketId, loads[bucketId], primaryLoads[bucketId]);
      bucketSizes[bucketId] = loads[bucketId];
      size += bucketSizes[bucketId];
      if (loads[bucketId] != 0) {
        bucketCount++;
      }
      if (primaryLoads[bucketId] != 0) {
        primaryCount++;
      }
    }
    return new PartitionMemberInfoImpl(id, localMaxMemory, size, bucketCount, primaryCount, prLoad,
        bucketSizes);
  }

  private static final AddressComparor comparor = new AddressComparor() {
    @Override
    public boolean enforceUniqueZones() {
      return false;
    }

    @Override
    public boolean areSameZone(InternalDistributedMember member1,
        InternalDistributedMember member2) {
      return false;
    }
  };

  private static class FakeOfflineDetails implements OfflineMemberDetails {

    private Set<PersistentMemberID> offlineMembers;

    FakeOfflineDetails() {
      this(Collections.emptySet());
    }

    FakeOfflineDetails(Set<PersistentMemberID> offlineMembers) {
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
