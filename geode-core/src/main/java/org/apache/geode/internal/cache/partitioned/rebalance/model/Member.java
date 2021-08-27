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

import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Represents a single member of the distributed system.
 */
public class Member implements Comparable<Member> {
  private static final Logger logger = LogService.getLogger();

  private final AddressComparor addressComparor;
  private final InternalDistributedMember memberId;
  protected float weight;
  private float totalLoad;
  private float totalPrimaryLoad;
  private long totalBytes;
  private long localMaxMemory;
  private final Set<Bucket> buckets = new TreeSet<>();
  private final Set<Bucket> primaryBuckets = new TreeSet<>();
  private final boolean isCritical;
  private final boolean enforceLocalMaxMemory;

  Member(AddressComparor addressComparor, InternalDistributedMember memberId, boolean isCritical,
      boolean enforceLocalMaxMemory) {
    this.addressComparor = addressComparor;
    this.memberId = memberId;
    this.isCritical = isCritical;
    this.enforceLocalMaxMemory = enforceLocalMaxMemory;
  }

  Member(AddressComparor addressComparor, InternalDistributedMember memberId, float weight,
      long localMaxMemory, boolean isCritical, boolean enforceLocalMaxMemory) {
    this(addressComparor, memberId, isCritical, enforceLocalMaxMemory);
    this.weight = weight;
    this.localMaxMemory = localMaxMemory;
  }

  /**
   * @param sourceMember the member we will be moving this bucket off of
   * @param checkZone true if we should not put two copies of a bucket on two nodes with the same
   *        IP address.
   */
  public RefusalReason canDelete(Bucket bucket, InternalDistributedMember sourceMember,
      boolean checkZone) {
    // Check the ip address
    if (checkZone) {
      if (addressComparor.enforceUniqueZones()
          && !addressComparor.areSameZone(getMemberId(), sourceMember)) {
        logger.info(
            "Member {} cannot delete {} because it is in a different redundancy zone than {}",
            getMemberId(), bucket, sourceMember);
        return RefusalReason.DIFFERENT_ZONE;
      } else {
        logger.info(
            "Member {} will delete  {} because it is already on another member with the same redundancy zone",
            this, bucket);
      }
    }
    return RefusalReason.NONE;
  }


  /**
   * @param sourceMember the member we will be moving this bucket off of
   * @param checkZone true if we should not put two copies of a bucket on two nodes with the same
   *        IP address.
   */
  public RefusalReason willAcceptBucket(Bucket bucket, Member sourceMember, boolean checkZone) {
    // make sure this member is not already hosting this bucket
    if (getBuckets().contains(bucket)) {
      return RefusalReason.ALREADY_HOSTING;
    }
    // Check the ip address
    if (checkZone) {
      // If the source member is equivalent to the target member, go
      // ahead and allow the bucket move (it's not making our redundancy worse).
      // TODO we could have some logic to prefer moving to different ip addresses
      // Probably that logic should be another stage after redundancy recovery, like
      // improveRedundancy.
      boolean sourceIsEquivalent = sourceMember != null
          && addressComparor.areSameZone(getMemberId(), sourceMember.getDistributedMember());
      if (sourceMember == null || !sourceIsEquivalent) {
        for (Member hostingMember : bucket.getMembersHosting()) {
          if ((!hostingMember.equals(sourceMember) || addressComparor.enforceUniqueZones())
              && addressComparor.areSameZone(getMemberId(), hostingMember.getDistributedMember())) {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Member {} would prefer not to host {} because it is already on another member with the same redundancy zone",
                  this, bucket);
            }
            return RefusalReason.SAME_ZONE;
          }
        }
      }
    }

    // check the localMaxMemory
    if (this.enforceLocalMaxMemory && this.totalBytes + bucket.getBytes() > this.localMaxMemory) {
      if (logger.isDebugEnabled()) {
        logger.debug("Member {} won't host bucket {} because it doesn't have enough space", this,
            bucket);
      }
      return RefusalReason.LOCAL_MAX_MEMORY_FULL;
    }

    // check to see if the heap is critical
    if (isCritical) {
      if (logger.isDebugEnabled()) {
        logger.debug("Member {} won't host bucket {} because it's heap is critical", this, bucket);
      }
      return RefusalReason.CRITICAL_HEAP;
    }

    return RefusalReason.NONE;
  }

  public boolean addBucket(Bucket bucket) {
    if (getBuckets().add(bucket)) {
      bucket.addMember(this);
      this.totalBytes += bucket.getBytes();
      this.totalLoad += bucket.getLoad();
      return true;
    }
    return false;
  }

  public boolean removeBucket(Bucket bucket) {
    if (getBuckets().remove(bucket)) {
      bucket.removeMember(this);
      this.totalBytes -= bucket.getBytes();
      this.totalLoad -= bucket.getLoad();
      return true;
    }
    return false;
  }

  public boolean removePrimary(Bucket bucket) {
    if (getPrimaryBuckets().remove(bucket)) {
      this.totalPrimaryLoad -= bucket.getPrimaryLoad();
      return true;
    }
    return false;
  }

  public boolean addPrimary(Bucket bucket) {
    if (getPrimaryBuckets().add(bucket)) {
      this.totalPrimaryLoad += bucket.getPrimaryLoad();
      return true;
    }
    return false;
  }

  public int getBucketCount() {
    return getBuckets().size();
  }

  public long getConfiguredMaxMemory() {
    return this.localMaxMemory;
  }

  public InternalDistributedMember getDistributedMember() {
    return getMemberId();
  }

  public int getPrimaryCount() {
    int primaryCount = 0;
    for (Bucket bucket : getBuckets()) {
      if (this.equals(bucket.getPrimary())) {
        primaryCount++;
      }
    }
    return primaryCount;
  }

  public long getSize() {
    return this.totalBytes;
  }

  public float getTotalLoad() {
    return this.totalLoad;
  }

  public float getWeight() {
    return this.weight;
  }

  @Override
  public String toString() {
    return "Member(id=" + getMemberId() + ")";
  }

  public float getPrimaryLoad() {
    return this.totalPrimaryLoad;
  }

  public Set<Bucket> getBuckets() {
    return this.buckets;
  }

  InternalDistributedMember getMemberId() {
    return this.memberId;
  }

  Set<Bucket> getPrimaryBuckets() {
    return this.primaryBuckets;
  }

  void changeLocalMaxMemory(long change) {
    localMaxMemory += change;
  }

  void changeTotalLoad(float change) {
    totalLoad += change;
  }

  void changePrimaryLoad(float change) {
    totalPrimaryLoad += change;
  }

  void changeTotalBytes(float change) {
    totalBytes += (long) Math.round(change);
  }

  @Override
  public int hashCode() {
    return memberId.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Member)) {
      return false;
    }
    Member o = (Member) other;
    return this.memberId.equals(o.memberId);
  }

  @Override
  public int compareTo(Member other) {
    // memberId is InternalDistributedMember which implements Comparable
    return this.memberId.compareTo(other.memberId);
  }
}
