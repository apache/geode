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
package org.apache.geode.internal.cache.control;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;

public class RebalanceResultsImpl implements RebalanceResults, Serializable {
  private Set<PartitionRebalanceInfo> detailSet = new TreeSet<PartitionRebalanceInfo>();
  private long totalBucketCreateBytes;
  private long totalBucketCreateTime;
  private int totalBucketCreatesCompleted;
  private long totalBucketTransferBytes;
  private long totalBucketTransferTime;
  private int totalBucketTransfersCompleted;
  private long totalPrimaryTransferTime;
  private int totalPrimaryTransfersCompleted;
  private long totalTime;
  private int totalNumOfMembers;

  public void addDetails(PartitionRebalanceInfo details) {
    this.detailSet.add(details);
    totalBucketCreateBytes += details.getBucketCreateBytes();
    totalBucketCreateTime += details.getBucketCreateTime();
    totalBucketCreatesCompleted += details.getBucketCreatesCompleted();
    totalBucketTransferBytes += details.getBucketTransferBytes();
    totalBucketTransferTime += details.getBucketTransferTime();
    totalBucketTransfersCompleted += details.getBucketTransfersCompleted();
    totalPrimaryTransferTime += details.getPrimaryTransferTime();
    totalPrimaryTransfersCompleted += details.getPrimaryTransfersCompleted();
    if (totalNumOfMembers == 0)
      totalNumOfMembers += details.getNumberOfMembersExecutedOn();
    totalTime += details.getTime();
  }

  public void addDetails(RebalanceResultsImpl details) {
    this.detailSet.addAll(details.detailSet);
    totalBucketCreateBytes += details.totalBucketCreateBytes;
    totalBucketCreateTime += details.totalBucketCreateTime;
    totalBucketCreatesCompleted += details.totalBucketCreatesCompleted;
    totalBucketTransferBytes += details.totalBucketTransferBytes;
    totalBucketTransferTime += details.totalBucketTransferTime;
    totalBucketTransfersCompleted += details.totalBucketTransfersCompleted;
    totalPrimaryTransferTime += details.totalPrimaryTransferTime;
    totalPrimaryTransfersCompleted += details.totalPrimaryTransfersCompleted;
    totalNumOfMembers += details.totalNumOfMembers;
    if (details.totalTime > totalTime)
      totalTime = details.totalTime;
  }

  @Override
  public Set<PartitionRebalanceInfo> getPartitionRebalanceDetails() {
    return detailSet;
  }

  @Override
  public long getTotalBucketCreateBytes() {
    return this.totalBucketCreateBytes;
  }

  @Override
  public long getTotalBucketCreateTime() {
    return this.totalBucketCreateTime;
  }

  @Override
  public int getTotalBucketCreatesCompleted() {
    return this.totalBucketCreatesCompleted;
  }

  @Override
  public long getTotalBucketTransferBytes() {
    return this.totalBucketTransferBytes;
  }

  @Override
  public long getTotalBucketTransferTime() {
    return this.totalBucketTransferTime;
  }

  @Override
  public int getTotalBucketTransfersCompleted() {
    return this.totalBucketTransfersCompleted;
  }

  @Override
  public long getTotalPrimaryTransferTime() {
    return this.totalPrimaryTransferTime;
  }

  @Override
  public int getTotalMembersExecutedOn() {
    return this.totalNumOfMembers;
  }

  @Override
  public int getTotalPrimaryTransfersCompleted() {
    return this.totalPrimaryTransfersCompleted;
  }

  @Override
  public long getTotalTime() {
    return this.totalTime;
  }

  @Override
  public String toString() {
    return "{totalBucketCreateBytes=" + totalBucketCreateBytes +
        ", totalBucketCreateTimeInMilliseconds=" + totalBucketCreateTime +
        ", totalBucketCreatesCompleted=" + totalBucketCreatesCompleted +
        ", totalBucketTransferBytes=" + totalBucketTransferBytes +
        ", totalBucketTransferTimeInMilliseconds=" + totalBucketTransferTime +
        ", totalBucketTransfersCompleted=" + totalBucketTransfersCompleted +
        ", totalPrimaryTransferTimeInMilliseconds=" + totalPrimaryTransferTime +
        ", totalPrimaryTransfersCompleted=" + totalPrimaryTransfersCompleted +
        ", totalTimeInMilliseconds=" + totalTime +
        ", totalNumOfMembers=" + totalNumOfMembers +
        "}";
  }
}
