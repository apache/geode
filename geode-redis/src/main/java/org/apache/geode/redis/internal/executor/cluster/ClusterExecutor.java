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
import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS;
import static org.apache.geode.redis.internal.cluster.BucketRetrievalFunction.MemberBuckets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.cluster.BucketRetrievalFunction;
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

    logger.info("CLUSTER args: {}", strArgs);

    RedisResponse response;
    switch (subCommand.toLowerCase()) {
      case "slots": {
        response = getSlots(context);
        break;
      }
      default: {
        response = RedisResponse.error(ERROR_UNKNOWN_COMMAND);
      }
    }

    return response;
  }

  // @SuppressWarnings("unchecked")
  private RedisResponse getSlots(ExecutionHandlerContext ctx) {
    Region<?, ?> region = ctx.getRegionProvider().getDataRegion();

    Execution<Void, MemberBuckets, List<MemberBuckets>> execution =
        FunctionService.onRegion(region);
    ResultCollector<MemberBuckets, List<MemberBuckets>> resultCollector =
        execution.execute(new BucketRetrievalFunction());

    SortedMap<Integer, String> bucketToMemberMap = new TreeMap<>();
    int retrievedBucketCount = 0;
    for (MemberBuckets m : resultCollector.getResult()) {
      for (Integer id : m.getBucketIds()) {
        bucketToMemberMap.put(id, m.getHostAddress());
        retrievedBucketCount++;
      }
    }

    if (retrievedBucketCount != REDIS_REGION_BUCKETS) {
      return RedisResponse.error("Internal error: bucket count mismatch " + retrievedBucketCount
          + " != " + REDIS_REGION_BUCKETS);
    }

    int slotsPerBucket = REDIS_SLOTS / REDIS_REGION_BUCKETS;
    int index = 0;
    List<Object> slots = new ArrayList<>();

    for (String member : bucketToMemberMap.values()) {
      List<?> entry = Arrays.asList(
          index * slotsPerBucket,
          ((index + 1) * slotsPerBucket) - 1,
          Arrays.asList(member, ctx.getServerPort()));

      slots.add(entry);
      index++;
    }

    return RedisResponse.array(slots);
  }
}
