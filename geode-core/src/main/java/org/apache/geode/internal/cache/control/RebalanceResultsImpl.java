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
    if(details.totalTime > totalTime)
    totalTime = details.totalTime;
  }

  public Set<PartitionRebalanceInfo> getPartitionRebalanceDetails() {
    return detailSet;
  }

  public long getTotalBucketCreateBytes() {
    return this.totalBucketCreateBytes;
  }

  public long getTotalBucketCreateTime() {
    return this.totalBucketCreateTime;
  }

  public int getTotalBucketCreatesCompleted() {
    return this.totalBucketCreatesCompleted;
  }

  public long getTotalBucketTransferBytes() {
    return this.totalBucketTransferBytes;
  }

  public long getTotalBucketTransferTime() {
    return this.totalBucketTransferTime;
  }

  public int getTotalBucketTransfersCompleted() {
    return this.totalBucketTransfersCompleted;
  }

  public long getTotalPrimaryTransferTime() {
    return this.totalPrimaryTransferTime;
  }

  public int getTotalPrimaryTransfersCompleted() {
    return this.totalPrimaryTransfersCompleted;
  }

  public long getTotalTime() {
    return this.totalTime;
  }
}
