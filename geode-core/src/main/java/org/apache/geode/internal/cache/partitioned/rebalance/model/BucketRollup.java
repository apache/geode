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

/**
 * Represents the sum of all of colocated buckets with a given bucket id.
 */
public class BucketRollup extends Bucket {
  private final Map<String, Bucket> colocatedBuckets = new HashMap<>();

  BucketRollup(int id) {
    super(id);
  }

  void addColocatedBucket(String region, Bucket b) {
    if (!getColocatedBuckets().containsKey(region)) {
      getColocatedBuckets().put(region, b);
      changeLoad(b.getLoad());
      changePrimaryLoad(b.getPrimaryLoad());
      changeBytes(b.getBytes());
      addOfflineMembers(b.getOfflineMembers());

      // Update the load on the members hosting this bucket
      // to reflect the fact that the bucket is larger now.
      for (Member member : getMembersHosting()) {
        MemberRollup rollup = (MemberRollup) member;
        float primaryLoad = 0;
        if (getPrimary() == member) {
          primaryLoad = b.getPrimaryLoad();
        }
        rollup.updateLoad(b.getLoad(), primaryLoad, b.getBytes());
      }
    }
  }

  @Override
  public boolean addMember(Member targetMember) {
    if (super.addMember(targetMember)) {
      MemberRollup memberRollup = (MemberRollup) targetMember;
      for (Map.Entry<String, Bucket> entry : getColocatedBuckets().entrySet()) {
        String region = entry.getKey();
        Bucket bucket = entry.getValue();
        Member member = memberRollup.getColocatedMembers().get(region);
        if (member != null) {
          bucket.addMember(member);
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean removeMember(Member targetMember) {
    if (super.removeMember(targetMember)) {
      MemberRollup memberRollup = (MemberRollup) targetMember;
      for (Map.Entry<String, Bucket> entry : getColocatedBuckets().entrySet()) {
        String region = entry.getKey();
        Bucket bucket = entry.getValue();
        Member member = memberRollup.getColocatedMembers().get(region);
        if (member != null) {
          bucket.removeMember(member);
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public void setPrimary(Member targetMember, float primaryLoad) {
    super.setPrimary(targetMember, primaryLoad);
    if (targetMember != null) {
      MemberRollup memberRollup = (MemberRollup) targetMember;
      for (Map.Entry<String, Bucket> entry : getColocatedBuckets().entrySet()) {
        String region = entry.getKey();
        Bucket bucket = entry.getValue();
        Member member = memberRollup.getColocatedMembers().get(region);
        if (member != null) {
          bucket.setPrimary(member, primaryLoad);
        }
      }
    }
  }

  Map<String, Bucket> getColocatedBuckets() {
    return colocatedBuckets;
  }
}
