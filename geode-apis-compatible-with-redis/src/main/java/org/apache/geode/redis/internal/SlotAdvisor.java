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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.partition.PartitionRegionInfo;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.cluster.RedisMemberInfo;
import org.apache.geode.redis.internal.cluster.RedisMemberInfoRetrievalFunction;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;

public class SlotAdvisor {

  private static final Logger logger = LogService.getLogger();
  private static final int HOSTPORT_RETRIEVAL_ATTEMPTS = 10;
  private static final int HOSTPORT_RETRIEVAL_INTERVAL = 6_000;

  /**
   * Cache of member ids to member IP address and redis listening port
   */
  private final Map<DistributedMember, Pair<String, Integer>> hostPorts = new HashMap<>();
  private final PartitionedRegion dataRegion;

  SlotAdvisor(Region<RedisKey, RedisData> dataRegion) {
    this.dataRegion = (PartitionedRegion) dataRegion;
  }

  public boolean isLocal(RedisKey key) {
    return dataRegion.getRegionAdvisor().getBucket(key.getBucketId()).getBucketAdvisor()
        .isPrimary();
  }

  public Pair<String, Integer> getHostAndPortForKey(RedisKey key) {
    return getHostPort(key.getBucketId());
  }

  public Map<String, List<Integer>> getMemberBuckets() {
    initializeBucketsIfNecessary();

    Map<String, List<Integer>> memberBuckets = new HashMap<>();
    for (int bucketId = 0; bucketId < REDIS_REGION_BUCKETS; bucketId++) {
      String memberId = getOrCreateMember(bucketId).getUniqueId();
      memberBuckets.computeIfAbsent(memberId, k -> new ArrayList<>()).add(bucketId);
    }

    return memberBuckets;
  }

  /**
   * This returns a list of {@link MemberBucketSlot}s where each entry corresponds to a bucket. If
   * the details for a given bucket cannot be determined, that entry will contain {@code null}.
   */
  public synchronized List<MemberBucketSlot> getBucketSlots() {
    initializeBucketsIfNecessary();

    List<MemberBucketSlot> memberBucketSlots = new ArrayList<>(RegionProvider.REDIS_REGION_BUCKETS);
    for (int bucketId = 0; bucketId < REDIS_REGION_BUCKETS; bucketId++) {
      Pair<String, Integer> hostPort = getHostPort(bucketId);
      if (hostPort != null) {
        memberBucketSlots.add(
            new MemberBucketSlot(bucketId, hostPort.getLeft(), hostPort.getRight()));
      }
    }

    return memberBucketSlots;
  }

  private InternalDistributedMember getOrCreateMember(int bucketId) {
    return dataRegion.getOrCreateNodeForBucketWrite(bucketId, null);
  }

  private void initializeBucketsIfNecessary() {
    if (dataRegion.getDataStore() != null &&
        dataRegion.getDataStore().getAllLocalBucketIds().isEmpty()) {
      PartitionRegionHelper.assignBucketsToPartitions(dataRegion);
    }
  }

  /**
   * This method will retry for {@link #HOSTPORT_RETRIEVAL_ATTEMPTS} attempts and return null if
   * no information could be retrieved.
   */
  private Pair<String, Integer> getHostPort(int bucketId) {
    Pair<String, Integer> response;

    for (int i = 0; i < HOSTPORT_RETRIEVAL_ATTEMPTS; i++) {
      response = getHostPort0(bucketId);
      if (response != null) {
        return response;
      }

      try {
        Thread.sleep(HOSTPORT_RETRIEVAL_INTERVAL);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    logger.error("Unable to retrieve host and redis port for member with bucketId: {}", bucketId);

    return null;
  }

  @SuppressWarnings("unchecked")
  private Pair<String, Integer> getHostPort0(int bucketId) {
    InternalDistributedMember member = getOrCreateMember(bucketId);

    if (hostPorts.containsKey(member)) {
      return hostPorts.get(member);
    }

    ResultCollector<RedisMemberInfo, List<RedisMemberInfo>> resultCollector;
    try {
      resultCollector =
          FunctionService.onRegion(dataRegion).execute(RedisMemberInfoRetrievalFunction.ID);
    } catch (FunctionException e) {
      return null;
    }

    hostPorts.clear();

    for (RedisMemberInfo memberInfo : resultCollector.getResult()) {
      Pair<String, Integer> hostPort =
          Pair.of(memberInfo.getHostAddress(), memberInfo.getRedisPort());
      hostPorts.put(memberInfo.getMember(), hostPort);
    }

    return hostPorts.get(member);
  }

  private Set<PartitionMemberInfo> getRegionMembers(PartitionedRegion dataRegion) {
    PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(dataRegion);
    assert info != null; // Mostly to appease IJ since the region is always a PR

    return info.getPartitionMemberInfo();
  }

  public static class MemberBucketSlot {
    private final Integer bucketId;
    private final String primaryIpAddress;
    private final Integer primaryPort;
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

    public Integer getPrimaryPort() {
      return primaryPort;
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
