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
package org.apache.geode.internal.cache.partitioned;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import org.apache.geode.cache.partition.PartitionMemberInfo;

public class PartitionRegionInfoImpl implements InternalPRInfo, Serializable {

  private static final long serialVersionUID = 6462414089469761476L;
  private final String regionPath;
  private final int configuredBucketCount;
  private final int createdBucketCount;
  private final int lowRedundancyBucketCount;
  private final int configuredRedundantCopies;
  private final int actualRedundantCopies;
  private final Set<InternalPartitionDetails> memberDetails;
  private final String colocatedWith;
  private final OfflineMemberDetails offlineMembers;

  public PartitionRegionInfoImpl(String regionPath, int configuredBucketCount,
      int createdBucketCount, int lowRedundancyBucketCount, int configuredRedundantCopies,
      int actualRedundantCopies, Set<InternalPartitionDetails> memberDetails, String colocatedPath,
      OfflineMemberDetails offlineMembers) {
    this.regionPath = regionPath;
    this.configuredBucketCount = configuredBucketCount;
    this.createdBucketCount = createdBucketCount;
    this.lowRedundancyBucketCount = lowRedundancyBucketCount;
    this.configuredRedundantCopies = configuredRedundantCopies;
    this.actualRedundantCopies = actualRedundantCopies;
    this.memberDetails = memberDetails;
    colocatedWith = colocatedPath;
    this.offlineMembers = offlineMembers;
  }

  @Override
  public int getActualRedundantCopies() {
    return actualRedundantCopies;
  }

  @Override
  public String getColocatedWith() {
    return colocatedWith;
  }

  @Override
  public int getConfiguredBucketCount() {
    return configuredBucketCount;
  }

  @Override
  public int getConfiguredRedundantCopies() {
    return configuredRedundantCopies;
  }

  @Override
  public int getCreatedBucketCount() {
    return createdBucketCount;
  }

  @Override
  public int getLowRedundancyBucketCount() {
    return lowRedundancyBucketCount;
  }

  @Override
  public Set<PartitionMemberInfo> getPartitionMemberInfo() {
    return Collections.unmodifiableSet(memberDetails);
  }

  @Override
  public Set<InternalPartitionDetails> getInternalPartitionDetails() {
    return Collections
        .unmodifiableSet(memberDetails);
  }


  @Override
  public String getRegionPath() {
    return regionPath;
  }

  @Override
  public OfflineMemberDetails getOfflineMembers() {
    return offlineMembers;
  }

  @Override
  public String toString() {
    return "[PartitionRegionInfoImpl: " + "regionPath=" + regionPath
        + ", configuredBucketCount=" + configuredBucketCount
        + ", createdBucketCount=" + createdBucketCount
        + ", lowRedundancyBucketCount=" + lowRedundancyBucketCount
        + ", configuredRedundantCopies=" + configuredRedundantCopies
        + ", actualRedundantCopies=" + actualRedundantCopies
        + ", memberDetails=" + memberDetails
        + ", colocatedWith=" + colocatedWith
        + ", offlineMembers=" + offlineMembers
        + "]";
  }

  /**
   * hashCode is defined for this class to make sure that the before details and after details for
   * RebalanceResults are in the same order. This makes debugging printouts easier, and it also
   * removes discrepancies due to rounding errors when calculating the stddev in tests.
   */
  @Override
  public int hashCode() {
    return regionPath.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PartitionRegionInfoImpl)) {
      return false;
    }
    PartitionRegionInfoImpl o = (PartitionRegionInfoImpl) other;
    return regionPath.equals(o.regionPath);
  }

  @Override
  public int compareTo(InternalPRInfo other) {
    return regionPath.compareTo(other.getRegionPath());
  }
}
