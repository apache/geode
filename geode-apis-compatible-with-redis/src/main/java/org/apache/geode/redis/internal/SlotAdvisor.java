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

package org.apache.geode.redis.internal;

import static org.apache.geode.redis.internal.RegionProvider.REDIS_REGION_BUCKETS;
import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS_PER_BUCKET;
import static org.apache.geode.redis.internal.cluster.RedisMemberInfoRetrievalFunction.RedisMemberInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.partition.PartitionRegionInfo;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.cluster.RedisMemberInfoRetrievalFunction;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;

public class SlotAdvisor {

  private static final Logger logger = LogService.getLogger();

  /**
   * Mapping buckets to slot information
   */
  private final List<MemberBucketSlot> memberBucketSlots;

  /**
   * Cache of member ids to member IP address and redis listening port
   */
  private final Map<String, Pair<String, Integer>> hostPorts = new HashMap<>();
  private final PartitionedRegion dataRegion;
  private final Region<String, Object> configRegion;

  SlotAdvisor(Region<RedisKey, RedisData> dataRegion, Region<String, Object> configRegion) {
    this.dataRegion = (PartitionedRegion) dataRegion;
    this.configRegion = configRegion;
    memberBucketSlots = new ArrayList<>(RegionProvider.REDIS_REGION_BUCKETS);
    for (int i = 0; i < REDIS_REGION_BUCKETS; i++) {
      memberBucketSlots.add(null);
    }
  }

  public boolean isLocal(RedisKey key) {
    return dataRegion.getRegionAdvisor().getBucket(key.getBucketId()).getBucketAdvisor()
        .isPrimary();
  }

  public Pair<String, Integer> getHostAndPortForKey(RedisKey key) {
    int bucketId = key.getBucketId();
    MemberBucketSlot mbs = updateBucketDetails(bucketId);

    return Pair.of(mbs.getPrimaryIpAddress(), mbs.getPrimaryPort());
  }

  public Map<String, List<Integer>> getMemberBuckets() {
    initializeBucketsIfNecessary();

    Map<String, List<Integer>> memberBuckets = new HashMap<>();
    for (int bucketId = 0; bucketId < REDIS_REGION_BUCKETS; bucketId++) {
      String memberId =
          dataRegion.getRegionAdvisor().getBucketAdvisor(bucketId).getPrimary().getUniqueId();
      memberBuckets.computeIfAbsent(memberId, k -> new ArrayList<>()).add(bucketId);
    }

    return memberBuckets;
  }

  public synchronized List<MemberBucketSlot> getBucketSlots() {
    initializeBucketsIfNecessary();

    for (int bucketId = 0; bucketId < REDIS_REGION_BUCKETS; bucketId++) {
      updateBucketDetails(bucketId);
    }

    return Collections.unmodifiableList(memberBucketSlots);
  }

  private synchronized MemberBucketSlot updateBucketDetails(int bucketId) {
    MemberBucketSlot mbs = null;
    try {
      Pair<String, Integer> hostPort = getHostPort(bucketId);

      mbs = memberBucketSlots.get(bucketId);
      if (mbs == null) {
        mbs = new MemberBucketSlot(bucketId, hostPort.getLeft(), hostPort.getRight());
        memberBucketSlots.set(bucketId, mbs);
      } else {
        mbs.setPrimaryIpAddress(hostPort.getLeft());
        mbs.setPrimaryPort(hostPort.getRight());
      }
    } catch (Exception ex) {
      logger.error("Unable to update bucket detail for bucketId: {}", bucketId, ex);
    }

    return mbs;
  }

  private void initializeBucketsIfNecessary() {
    if (dataRegion.getDataStore() != null &&
        dataRegion.getDataStore().getAllLocalBucketIds().isEmpty()) {
      PartitionRegionHelper.assignBucketsToPartitions(dataRegion);
    }
  }

  @SuppressWarnings("unchecked")
  private Pair<String, Integer> getHostPort(int bucketId) {
    String memberId =
        dataRegion.getRegionAdvisor().getBucketAdvisor(bucketId).getPrimary().getUniqueId();

    if (hostPorts.containsKey(memberId)) {
      return hostPorts.get(memberId);
    }

    Set<DistributedMember> membersWithDataRegion = new HashSet<>();
    for (PartitionMemberInfo memberInfo : getRegionMembers(dataRegion)) {
      membersWithDataRegion.add(memberInfo.getDistributedMember());
    }

    ResultCollector<RedisMemberInfo, List<RedisMemberInfo>> resultCollector =
        FunctionService.onMembers(membersWithDataRegion)
            .execute(RedisMemberInfoRetrievalFunction.ID);

    hostPorts.clear();

    for (RedisMemberInfo memberInfo : resultCollector.getResult()) {
      Pair<String, Integer> hostPort =
          Pair.of(memberInfo.getHostAddress(), memberInfo.getRedisPort());
      hostPorts.put(memberInfo.getMemberId(), hostPort);
    }

    if (!hostPorts.containsKey(memberId)) {
      // There is a very tiny window where this might happen - a member was hosting a bucket and
      // died before the fn call above could even complete.
      throw new RuntimeException("Unable to retrieve host and redis port for member: " + memberId);
    }

    return hostPorts.get(memberId);
  }

  private Set<PartitionMemberInfo> getRegionMembers(PartitionedRegion dataRegion) {
    PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(dataRegion);
    assert info != null; // Mostly to appease IJ since the region is always a PR

    return info.getPartitionMemberInfo();
  }

  public static class MemberBucketSlot {
    private final Integer bucketId;
    private String primaryIpAddress;
    private Integer primaryPort;
    private final Integer slotStart;
    private final Integer slotEnd;

    public MemberBucketSlot(Integer bucketId, String primaryIpAddress, Integer primaryPort) {
      this.bucketId = bucketId;
      this.primaryIpAddress = primaryIpAddress;
      this.primaryPort = primaryPort;
      this.slotStart = bucketId * REDIS_SLOTS_PER_BUCKET;
      this.slotEnd = ((bucketId + 1) * REDIS_SLOTS_PER_BUCKET) - 1;
    }

    public Integer getBucketId() {
      return bucketId;
    }

    public String getPrimaryIpAddress() {
      return primaryIpAddress;
    }

    public void setPrimaryIpAddress(String primaryIpAddress) {
      this.primaryIpAddress = primaryIpAddress;
    }

    public Integer getPrimaryPort() {
      return primaryPort;
    }

    public void setPrimaryPort(Integer primaryPort) {
      this.primaryPort = primaryPort;
    }

    public Integer getSlotStart() {
      return slotStart;
    }

    public Integer getSlotEnd() {
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
