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
package com.gemstone.gemfire.internal.cache;

import java.util.*;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.SetUtils;
import com.gemstone.gemfire.internal.cache.partitioned.Bucket;


/**
 * This class encapsulates the Bucket Related heuristics/algos for a PR.
 * 
 *  
 */
public class PartitionedRegionBucketMgmtHelper
  {

  /**
   * 
   * @param b Bucket to evaluate
   * @param moveSource 
   * @return true if it is allowed to be recovered
   * @since GemFire 5.9
   */
  public static boolean bucketIsAllowedOnThisHost(Bucket b, InternalDistributedMember moveSource) {
    if (b.getDistributionManager().enforceUniqueZone()) {
      Set<InternalDistributedMember> hostingMembers = b.getBucketOwners();
      Set<InternalDistributedMember> buddyMembers = b.getDistributionManager().getMembersInThisZone();
      boolean intersects =  SetUtils.intersectsWith(hostingMembers, buddyMembers);
      boolean sourceIsOneThisHost = moveSource != null && buddyMembers.contains(moveSource);
      return !intersects || sourceIsOneThisHost;
    }
    else { 
      return true;
    }
  }
}
