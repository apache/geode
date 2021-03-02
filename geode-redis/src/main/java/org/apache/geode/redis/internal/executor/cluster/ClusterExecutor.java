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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_COMMAND;
import static org.apache.geode.redis.internal.RegionProvider.REDIS_REGION_BUCKETS;
import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS_PER_BUCKET;
import static org.apache.geode.redis.internal.cluster.BucketRetrievalFunction.MemberBuckets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.partition.PartitionRegionInfo;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.cluster.BucketRetrievalFunction;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ClusterExecutor extends AbstractExecutor {

  private static final Logger logger = LogService.getLogger();

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {

    List<byte[]> args = command.getProcessedCommand();
    String subCommand = new String(args.get(1));

    StringBuilder strArgs = new StringBuilder();
    args.forEach(x -> strArgs.append(new String(x)).append(" "));

    switch (subCommand.toLowerCase()) {
      case "info":
        return getInfo(context);
      case "nodes":
        return getNodes(context);
      case "slots":
        return getSlots(context);
      default: {
        return RedisResponse.error(ERROR_UNKNOWN_COMMAND);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private RedisResponse getSlots(ExecutionHandlerContext ctx) {
    Region<RedisKey, RedisData> dataRegion = ctx.getRegionProvider().getDataRegion();

    // Really only need this in situations where the cluster is empty and no data has been
    // added yet.
    PartitionRegionHelper.assignBucketsToPartitions(dataRegion);

    PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(dataRegion);
    Set<DistributedMember> membersWithDataRegion = new HashSet<>();
    for (PartitionMemberInfo memberInfo : info.getPartitionMemberInfo()) {
      membersWithDataRegion.add(memberInfo.getDistributedMember());
    }

    ResultCollector<MemberBuckets, List<MemberBuckets>> resultCollector =
        FunctionService.onMembers(membersWithDataRegion).execute(BucketRetrievalFunction.ID);

    Map<Integer, String> primaryBucketToMemberMap = new HashMap<>();
    Map<Integer, List<String>> secondaryBucketToMemberMap = new HashMap<>();
    Map<String, Pair<String, Integer>> memberToHostPortMap = new TreeMap<>();
    int retrievedBucketCount = 0;

    for (MemberBuckets m : resultCollector.getResult()) {
      memberToHostPortMap.put(m.getMemberId(), Pair.of(m.getHostAddress(), m.getPort()));
      for (Integer id : m.getPrimaryBucketIds()) {
        primaryBucketToMemberMap.put(id, m.getMemberId());
        retrievedBucketCount++;
      }

      for (Map.Entry<Integer, List<String>> entry : m.getSecondaryBucketMembers().entrySet()) {
        secondaryBucketToMemberMap.put(entry.getKey(), entry.getValue());
      }
    }

    if (retrievedBucketCount != REDIS_REGION_BUCKETS) {
      return RedisResponse.error("Internal error: bucket count mismatch " + retrievedBucketCount
          + " != " + REDIS_REGION_BUCKETS);
    }

    int index = 0;
    List<Object> slots = new ArrayList<>();

    for (int i = 0; i < REDIS_REGION_BUCKETS; i++) {
      Pair<String, Integer> primaryHostAndPort =
          memberToHostPortMap.get(primaryBucketToMemberMap.get(i));

      List<Object> entry = new ArrayList<>();
      entry.add(index * REDIS_SLOTS_PER_BUCKET);
      entry.add(((index + 1) * REDIS_SLOTS_PER_BUCKET) - 1);
      entry.add(Arrays.asList(primaryHostAndPort.getLeft(), primaryHostAndPort.getRight()));

      List<String> secondaryMembers = secondaryBucketToMemberMap.get(i);
      if (secondaryMembers != null) {
        for (String m : secondaryMembers) {
          Pair<String, Integer> hostPort = memberToHostPortMap.get(m);
          entry.add(Arrays.asList(hostPort.getLeft(), hostPort.getRight()));
        }
      }

      slots.add(entry);
      index++;
    }

    return RedisResponse.array(slots);
  }

  private RedisResponse getNodes(ExecutionHandlerContext ctx) {
    return RedisResponse.error("not yet!");
  }

  private RedisResponse getInfo(ExecutionHandlerContext ctx) {
    return RedisResponse.bulkString(
        "cluster_state:ok\r\n"
            + "cluster_slots_assigned:16384\r\n"
            + "cluster_slots_ok:16384\r\n"
            + "cluster_slots_pfail:0\r\n"
            + "cluster_slots_fail:0\r\n"
            + "cluster_known_nodes:6\r\n"
            + "cluster_size:3\r\n"
            + "cluster_current_epoch:6\r\n"
            + "cluster_my_epoch:2\r\n"
            + "cluster_stats_messages_sent:1483972\r\n"
            + "cluster_stats_messages_received:1483968\r\n"
    );
  }
}
