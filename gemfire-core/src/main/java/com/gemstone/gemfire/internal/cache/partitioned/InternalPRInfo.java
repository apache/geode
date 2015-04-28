/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;

import java.util.Set;

/**
 * Extends <code>PartitionRegionInfo</code> with internal-only methods.
 * 
 * @author Kirk Lund
 */
public interface InternalPRInfo 
extends PartitionRegionInfo, Comparable<InternalPRInfo> {
  /**
   * Returns an immutable set of <code>InternalPartitionDetails</code> 
   * representing every member that is configured to provide storage space to
   * the partitioned region.
   * 
   * @return set of member details configured for storage space
   */
  public Set<InternalPartitionDetails> getInternalPartitionDetails();
  
  /**
   * Returns a set of members that host a bucket, but are currently offline.
   */
  public OfflineMemberDetails getOfflineMembers();
}
