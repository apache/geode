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
package org.apache.geode.internal.cache.partitioned;

import org.apache.geode.cache.partition.PartitionRegionInfo;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;

import java.util.Set;

/**
 * Extends <code>PartitionRegionInfo</code> with internal-only methods.
 * 
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
