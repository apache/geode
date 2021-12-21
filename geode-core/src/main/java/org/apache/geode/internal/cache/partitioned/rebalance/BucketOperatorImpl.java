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
package org.apache.geode.internal.cache.partitioned.rebalance;

import java.util.Map;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionRebalanceOp;

public class BucketOperatorImpl implements BucketOperator {

  private final PartitionedRegionRebalanceOp rebalanceOp;

  public BucketOperatorImpl(PartitionedRegionRebalanceOp rebalanceOp) {
    this.rebalanceOp = rebalanceOp;
  }

  @Override
  public boolean moveBucket(InternalDistributedMember source, InternalDistributedMember target,
      int bucketId, Map<String, Long> colocatedRegionBytes) {

    InternalResourceManager.getResourceObserver().movingBucket(rebalanceOp.getLeaderRegion(),
        bucketId, source, target);
    return rebalanceOp.moveBucketForRegion(source, target, bucketId);
  }

  @Override
  public boolean movePrimary(InternalDistributedMember source, InternalDistributedMember target,
      int bucketId) {

    InternalResourceManager.getResourceObserver().movingPrimary(rebalanceOp.getLeaderRegion(),
        bucketId, source, target);
    return rebalanceOp.movePrimaryBucketForRegion(target, bucketId);
  }

  @Override
  public void createRedundantBucket(InternalDistributedMember targetMember, int bucketId,
      Map<String, Long> colocatedRegionBytes, Completion completion) {
    boolean result = false;
    try {
      result = rebalanceOp.createRedundantBucketForRegion(targetMember, bucketId);
    } finally {
      if (result) {
        completion.onSuccess();
      } else {
        completion.onFailure();
      }
    }
  }

  @Override
  public void waitForOperations() {
    // do nothing, all operations are synchronous
  }

  @Override
  public boolean removeBucket(InternalDistributedMember targetMember, int bucketId,
      Map<String, Long> colocatedRegionBytes) {
    return rebalanceOp.removeRedundantBucketForRegion(targetMember, bucketId);
  }
}
