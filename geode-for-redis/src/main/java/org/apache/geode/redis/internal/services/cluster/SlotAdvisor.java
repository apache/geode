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

package org.apache.geode.redis.internal.services.cluster;

import static org.apache.geode.redis.internal.services.RegionProvider.REDIS_REGION_BUCKETS;
import static org.apache.geode.redis.internal.services.RegionProvider.REDIS_SLOTS_PER_BUCKET;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.services.RegionProvider;

public class SlotAdvisor {

  private static final Logger logger = LogService.getLogger();
  private static final int HOSTPORT_RETRIEVAL_ATTEMPTS = 20;
  private static final int HOSTPORT_RETRIEVAL_INTERVAL = 100;

  /**
   * Cache of member ids to member IP address and redis listening port
   */
  private final Map<DistributedMember, RedisMemberInfo> memberInfos = new HashMap<>();
  private final PartitionedRegion dataRegion;
  private final InternalDistributedMember thisMember;

  public SlotAdvisor(Region<RedisKey, RedisData> dataRegion, InternalDistributedMember thisMember) {
    this.dataRegion = (PartitionedRegion) dataRegion;
    this.thisMember = thisMember;
  }

  public boolean isLocal(RedisKey key) {
    // This call returns early with the member if the bucket already exists
    DistributedMember primaryMember = dataRegion.createBucket(key.getBucketId(), 1, null);
    return thisMember.equals(primaryMember);
  }

  public RedisMemberInfo getMemberInfo(RedisKey key) throws InterruptedException {
    return getMemberInfo(key.getBucketId());
  }

  /**
   * This returns a list of {@link MemberBucketSlot}s where each entry corresponds to a bucket. If
   * the details for a given bucket cannot be determined, that entry will contain {@code null}.
   */
  public synchronized List<MemberBucketSlot> getBucketSlots() throws InterruptedException {
    List<MemberBucketSlot> memberBucketSlots = new ArrayList<>(RegionProvider.REDIS_REGION_BUCKETS);
    for (int bucketId = 0; bucketId < REDIS_REGION_BUCKETS; bucketId++) {
      RedisMemberInfo memberInfo = getMemberInfo(bucketId);
      if (memberInfo != null) {
        memberBucketSlots.add(
            new MemberBucketSlot(bucketId, memberInfo.getMember(), memberInfo.getHostAddress(),
                memberInfo.getRedisPort()));
      } else {
        memberBucketSlots.add(null);
      }
    }

    return memberBucketSlots;
  }

  private InternalDistributedMember getOrCreateMember(int bucketId) {
    return dataRegion.getOrCreateNodeForBucketWrite(bucketId, null);
  }

  /**
   * This method will retry for {@link #HOSTPORT_RETRIEVAL_ATTEMPTS} attempts and return null if
   * no information could be retrieved.
   */
  private RedisMemberInfo getMemberInfo(int bucketId) throws InterruptedException {
    RedisMemberInfo response;

    for (int i = 0; i < HOSTPORT_RETRIEVAL_ATTEMPTS; i++) {
      response = getMemberInfo0(bucketId);
      if (response != null) {
        return response;
      }

      Thread.sleep(HOSTPORT_RETRIEVAL_INTERVAL);
    }

    logger.error("Unable to retrieve host and redis port for member with bucketId: {}", bucketId);

    return null;
  }

  @SuppressWarnings("unchecked")
  private RedisMemberInfo getMemberInfo0(int bucketId) {
    InternalDistributedMember member = getOrCreateMember(bucketId);

    if (memberInfos.containsKey(member)) {
      return memberInfos.get(member);
    }

    List<RedisMemberInfo> memberInfos;
    ResultCollector<RedisMemberInfo, List<RedisMemberInfo>> resultCollector =
        FunctionService.onRegion(dataRegion).execute(RedisMemberInfoRetrievalFunction.ID);
    memberInfos = resultCollector.getResult();

    this.memberInfos.clear();

    for (RedisMemberInfo memberInfo : memberInfos) {
      if (memberInfo == null) {
        continue;
      }
      this.memberInfos.put(memberInfo.getMember(), memberInfo);
    }

    return this.memberInfos.get(member);
  }

  public static class MemberBucketSlot {
    private final int bucketId;
    private final DistributedMember member;
    private final String primaryIpAddress;
    private final int primaryPort;
    private final int slotStart;
    private final int slotEnd;

    public MemberBucketSlot(int bucketId, DistributedMember member, String primaryIpAddress,
        int primaryPort) {
      this.bucketId = bucketId;
      this.member = member;
      this.primaryIpAddress = primaryIpAddress;
      this.primaryPort = primaryPort;
      this.slotStart = bucketId * REDIS_SLOTS_PER_BUCKET;
      this.slotEnd = ((bucketId + 1) * REDIS_SLOTS_PER_BUCKET) - 1;
    }

    public int getBucketId() {
      return bucketId;
    }

    public DistributedMember getMember() {
      return member;
    }

    public String getPrimaryIpAddress() {
      return primaryIpAddress;
    }

    public int getPrimaryPort() {
      return primaryPort;
    }

    public int getSlotStart() {
      return slotStart;
    }

    public int getSlotEnd() {
      return slotEnd;
    }

    @Override
    public String toString() {
      return "BucketSlot{" +
          "bucketId=" + bucketId +
          ", primaryIpAddress='" + primaryIpAddress + '\'' +
          ", primaryPort=" + primaryPort +
          ", slotStart=" + slotStart +
          ", slotEnd=" + slotEnd +
          '}';
    }
  }

}
