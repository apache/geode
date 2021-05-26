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

package org.apache.geode.redis.internal.executor.cluster;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_CLUSTER_SUBCOMMAND;
import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS;
import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS_PER_BUCKET;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.partition.PartitionRegionInfo;
import org.apache.geode.redis.internal.SlotAdvisor;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ClusterExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context)
      throws Exception {

    List<byte[]> args = command.getProcessedCommand();
    String subCommand = new String(args.get(1));

    switch (subCommand.toLowerCase()) {
      case "info":
        return getInfo(context);
      case "nodes":
        return getNodes(context);
      case "slots":
        return getSlots(context);
      default: {
        return RedisResponse.error(
            String.format(ERROR_UNKNOWN_CLUSTER_SUBCOMMAND, subCommand));
      }
    }
  }

  private RedisResponse getSlots(ExecutionHandlerContext ctx) throws InterruptedException {
    List<Object> slots = new ArrayList<>();

    for (SlotAdvisor.MemberBucketSlot mbs : ctx.getRegionProvider().getSlotAdvisor()
        .getBucketSlots()) {
      if (mbs == null) {
        continue;
      }

      List<Object> entry = new ArrayList<>();
      entry.add(mbs.getSlotStart());
      entry.add(mbs.getSlotEnd());
      entry.add(Arrays.asList(mbs.getPrimaryIpAddress(), mbs.getPrimaryPort()));

      slots.add(entry);
    }

    return RedisResponse.array(slots);
  }

  /**
   * The format being produced is something like this:
   *
   * <pre>
   * 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002 master - 0 1426238316232 2 connected 5461-10922
   * 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master - 0 1426238318243 3 connected 10923-16383
   * e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 connected 0-5460
   * </pre>
   *
   * Note that there are no 'slave' entries since Geode does not host all secondary data apart from
   * primary as redis does. The cluster port is provided only for consistency with the format of the
   * output.
   */
  private RedisResponse getNodes(ExecutionHandlerContext ctx) throws InterruptedException {
    String memberId = ctx.getMemberName();
    List<SlotAdvisor.MemberBucketSlot> memberBucketSlots =
        ctx.getRegionProvider().getSlotAdvisor().getBucketSlots();
    Map<String, List<Integer>> memberBuckets = getMemberBuckets(memberBucketSlots);

    StringBuilder response = new StringBuilder();
    for (Map.Entry<String, List<Integer>> member : memberBuckets.entrySet()) {
      List<Integer> buckets = member.getValue();
      SlotAdvisor.MemberBucketSlot mbs = memberBucketSlots.get(buckets.get(0));
      if (mbs == null) {
        continue;
      }

      response.append(String.format("%s %s:%3$d@%3$d master",
          member.getKey(), mbs.getPrimaryIpAddress(), mbs.getPrimaryPort()));

      if (member.getKey().equals(memberId)) {
        response.append(",myself");
      }
      response.append(" - 0 0 1 connected");

      for (int bucket : member.getValue()) {
        response.append(" ");
        response.append(bucket * REDIS_SLOTS_PER_BUCKET);
        response.append("-");
        response.append(((bucket + 1) * REDIS_SLOTS_PER_BUCKET) - 1);
      }

      response.append("\n");
    }

    return RedisResponse.bulkString(response.toString());
  }

  private Map<String, List<Integer>> getMemberBuckets(
      List<SlotAdvisor.MemberBucketSlot> bucketSlots) {
    Map<String, List<Integer>> memberBuckets = new HashMap<>();

    for (SlotAdvisor.MemberBucketSlot mbs : bucketSlots) {
      memberBuckets.computeIfAbsent(mbs.getMember().getUniqueId(), k -> new ArrayList<>())
          .add(mbs.getBucketId());
    }

    return memberBuckets;
  }

  private RedisResponse getInfo(ExecutionHandlerContext ctx) {
    int memberCount = getRegionMembers(ctx).size();

    return RedisResponse.bulkString(
        "cluster_state:ok\r\n"
            + "cluster_slots_assigned:" + REDIS_SLOTS + "\r\n"
            + "cluster_slots_ok:" + REDIS_SLOTS + "\r\n"
            + "cluster_slots_pfail:0\r\n"
            + "cluster_slots_fail:0\r\n"
            + "cluster_known_nodes:" + memberCount + "\r\n"
            + "cluster_size:" + memberCount + "\r\n"
            + "cluster_current_epoch:1\r\n"
            + "cluster_my_epoch:1\r\n"
            + "cluster_stats_messages_sent:0\r\n"
            + "cluster_stats_messages_received:0\r\n");
  }

  private Set<PartitionMemberInfo> getRegionMembers(ExecutionHandlerContext ctx) {
    Region<RedisKey, RedisData> dataRegion = ctx.getRegionProvider().getDataRegion();
    PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(dataRegion);
    assert info != null; // Mostly to appease IJ since the region is always a PR

    return info.getPartitionMemberInfo();
  }
}
