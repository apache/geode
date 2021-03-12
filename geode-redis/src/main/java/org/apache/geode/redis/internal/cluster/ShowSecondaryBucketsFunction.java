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

package org.apache.geode.redis.internal.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisKey;

/**
 * Helper function to show the distribution of secondary buckets for a given member.
 */
public class ShowSecondaryBucketsFunction implements InternalFunction<Void> {

  public static final String ID = "redis-secondary-buckets";

  public static void register() {
    FunctionService.registerFunction(new ShowSecondaryBucketsFunction());
  }

  @Override
  public void execute(FunctionContext<Void> context) {
    Region<RedisKey, ByteArrayWrapper> region =
        context.getCache().getRegion(RegionProvider.REDIS_DATA_REGION);

    String memberId =
        context.getCache().getDistributedSystem().getDistributedMember().getUniqueId();
    LocalDataSet localPrimary = (LocalDataSet) PartitionRegionHelper.getLocalPrimaryData(region);
    RegionAdvisor advisor = ((PartitionedRegion) region).getRegionAdvisor();
    Map<String, Integer> secondariesBucketCount = new HashMap<>();

    for (Integer bucketId : localPrimary.getBucketSet()) {
      List<String> a = advisor.getBucketOwners(bucketId).stream()
          .map(InternalDistributedMember::getId)
          .filter(x -> !x.equals(memberId))
          .collect(Collectors.toList());

      a.forEach(x -> secondariesBucketCount.compute(x, (k, v) -> (v == null) ? 1 : v + 1));
    }

    context.getResultSender().lastResult(secondariesBucketCount);
  }

  @Override
  public String getId() {
    return ID;
  }

}
