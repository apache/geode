/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.*;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.SetUtils;
import com.gemstone.gemfire.internal.cache.partitioned.Bucket;


/**
 * This class encapsulates the Bucket Related heuristics/algos for a PR.
 * 
 * @author rreja, modified by tnegi
 *  
 */
public class PartitionedRegionBucketMgmtHelper
  {

  /**
   * 
   * @param b Bucket to evaluate
   * @param moveSource 
   * @return true if it is allowed to be recovered
   * @since gemfire59poc
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
