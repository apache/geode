/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

import java.util.Map;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * A BucketOperator which does nothing. Used for simulations.
 *
 */
public class SimulatedBucketOperator implements BucketOperator {

  public void createRedundantBucket(
      InternalDistributedMember targetMember, int i, Map<String, Long> colocatedRegionBytes, 
      BucketOperator.Completion completion) {
    completion.onSuccess();
  }
  
  public boolean moveBucket(InternalDistributedMember source,
      InternalDistributedMember target, int id,
      Map<String, Long> colocatedRegionBytes) {
    return true;
  }

  public boolean movePrimary(InternalDistributedMember source,
      InternalDistributedMember target, int bucketId) {
    return true;
  }

  public boolean removeBucket(InternalDistributedMember memberId, int id,
      Map<String, Long> colocatedRegionSizes) {
    return true;
  }

  @Override
  public void waitForOperations() {
  }
}