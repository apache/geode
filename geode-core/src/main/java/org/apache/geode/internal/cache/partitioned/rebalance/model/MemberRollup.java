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
package org.apache.geode.internal.cache.partitioned.rebalance.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * Represents the sum of all of the colocated regions on a given member. Also, holds a map of all
 * of the colocated regions hosted on this member.
 */
class MemberRollup extends Member {
  private final Map<String, Member> colocatedMembers = new HashMap<>();

  MemberRollup(AddressComparor addressComparor, InternalDistributedMember memberId,
      boolean isCritical, boolean enforceLocalMaxMemory) {
    super(addressComparor, memberId, isCritical, enforceLocalMaxMemory);
  }

  /**
   * Indicates that this member doesn't have all of the colocated regions
   */
  public boolean isInvalid() {
    return false;
  }

  public void addColocatedMember(String region, Member member) {
    if (!getColocatedMembers().containsKey(region)) {
      getColocatedMembers().put(region, member);
      weight += member.weight;
      changeLocalMaxMemory(member.getConfiguredMaxMemory());
    }
  }


  public Member getColocatedMember(String region) {
    return getColocatedMembers().get(region);
  }

  /**
   * Update the load on this member rollup with a change in size of one of the bucket rollups
   * hosted by this member
   */
  public void updateLoad(float load, float primaryLoad, float bytes) {
    changeTotalLoad(load);
    changePrimaryLoad(primaryLoad);
    changeTotalBytes(bytes);
  }

  @Override
  public boolean addBucket(Bucket bucket) {
    if (super.addBucket(bucket)) {
      BucketRollup bucketRollup = (BucketRollup) bucket;
      for (Map.Entry<String, Member> entry : getColocatedMembers().entrySet()) {
        String region = entry.getKey();
        Member member = entry.getValue();
        Bucket colocatedBucket = bucketRollup.getColocatedBuckets().get(region);
        if (colocatedBucket != null) {
          member.addBucket(colocatedBucket);
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean removeBucket(Bucket bucket) {
    if (super.removeBucket(bucket)) {
      BucketRollup bucketRollup = (BucketRollup) bucket;
      for (Map.Entry<String, Member> entry : getColocatedMembers().entrySet()) {
        String region = entry.getKey();
        Member member = entry.getValue();
        Bucket colocatedBucket = bucketRollup.getColocatedBuckets().get(region);
        if (colocatedBucket != null) {
          member.removeBucket(colocatedBucket);
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean addPrimary(Bucket bucket) {
    if (super.addPrimary(bucket)) {
      BucketRollup bucketRollup = (BucketRollup) bucket;
      for (Map.Entry<String, Member> entry : getColocatedMembers().entrySet()) {
        String region = entry.getKey();
        Member member = entry.getValue();
        Bucket colocatedBucket = bucketRollup.getColocatedBuckets().get(region);
        if (colocatedBucket != null) {
          member.addPrimary(colocatedBucket);
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean removePrimary(Bucket bucket) {
    if (super.removePrimary(bucket)) {
      BucketRollup bucketRollup = (BucketRollup) bucket;
      for (Map.Entry<String, Member> entry : getColocatedMembers().entrySet()) {
        String region = entry.getKey();
        Member member = entry.getValue();
        Bucket colocatedBucket = bucketRollup.getColocatedBuckets().get(region);
        if (colocatedBucket != null) {
          member.removePrimary(colocatedBucket);
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public RefusalReason willAcceptBucket(Bucket bucket, Member source, boolean checkIPAddress) {
    RefusalReason reason = super.willAcceptBucket(bucket, source, checkIPAddress);
    if (reason.willAccept()) {
      BucketRollup bucketRollup = (BucketRollup) bucket;
      MemberRollup sourceRollup = (MemberRollup) source;
      for (Map.Entry<String, Member> entry : getColocatedMembers().entrySet()) {
        String region = entry.getKey();
        Member member = entry.getValue();
        Bucket colocatedBucket = bucketRollup.getColocatedBuckets().get(region);
        Member colocatedSource =
            sourceRollup == null ? null : sourceRollup.getColocatedMembers().get(region);
        if (colocatedBucket != null) {
          reason = member.willAcceptBucket(colocatedBucket, colocatedSource, checkIPAddress);
          if (!reason.willAccept()) {
            return reason;
          }
        }
      }
      return RefusalReason.NONE;
    }
    return reason;
  }

  Map<String, Member> getColocatedMembers() {
    return colocatedMembers;
  }
}
