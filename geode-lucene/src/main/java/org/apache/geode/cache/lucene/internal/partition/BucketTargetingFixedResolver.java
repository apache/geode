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
package org.apache.geode.cache.lucene.internal.partition;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.internal.cache.PartitionedRegion;

import java.util.Map;
import java.util.Set;

/**
 * A partition resolver that expects the actual bucket id to be the callback argument of all
 * operations. The partition resolver reverse engineers the fixed partition name and bucket number
 * from the target partitioning.
 *
 * This is a bit messy, mostly because there's no good way to get the FixedPartition from the actual
 * bucket id without iterating over all of the fixed partitions.
 */
public class BucketTargetingFixedResolver implements FixedPartitionResolver {

  @Override
  public Object getRoutingObject(final EntryOperation opDetails) {
    int targetBucketId = (Integer) opDetails.getCallbackArgument();
    final Map.Entry<String, Integer[]> targetPartition = getFixedPartition(opDetails);

    return targetBucketId - targetPartition.getValue()[0];
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

  @Override
  public void close() {

  }

  @Override
  public String getPartitionName(final EntryOperation opDetails,
      @Deprecated final Set targetPartitions) {
    final Map.Entry<String, Integer[]> targetPartition = getFixedPartition(opDetails);
    return targetPartition.getKey();
  }

  protected Map.Entry<String, Integer[]> getFixedPartition(final EntryOperation opDetails) {
    PartitionedRegion region = (PartitionedRegion) opDetails.getRegion();
    int targetBucketId = (Integer) opDetails.getCallbackArgument();
    Map<String, Integer[]> partitions = region.getPartitionsMap();

    return partitions.entrySet().stream().filter(entry -> withinPartition(entry, targetBucketId))
        .findFirst().get();
  }

  private boolean withinPartition(Map.Entry<String, Integer[]> entry, int bucketId) {
    int startingBucket = entry.getValue()[0];
    int endingBucket = startingBucket + entry.getValue()[1];
    return startingBucket <= bucketId && bucketId < endingBucket;
  }
}
