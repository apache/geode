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

package org.apache.geode.management.internal.operation;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.geode.management.runtime.RebalanceResult;

public class RebalanceResultImpl implements RebalanceResult {
  private Map<String, PerRegionStats> rebalanceSummary = new LinkedHashMap<>();

  RebalanceResultImpl() {}

  @Override
  public Map<String, PerRegionStats> getRebalanceStats() {
    return rebalanceSummary;
  }

  void setRebalanceSummary(Map<String, PerRegionStats> rebalanceSummary) {
    this.rebalanceSummary = rebalanceSummary;
  }

  @Override
  public String toString() {
    return "{" + rebalanceSummary.entrySet().stream()
        .map(e -> e.getKey() + ": " + e.getValue()).collect(Collectors.joining(",\n ")) + "}";
  }

  public static class PerRegionStatsImpl implements PerRegionStats {
    long bucketCreateBytes;
    long bucketCreateTimeInMilliseconds;
    int bucketCreatesCompleted;
    long bucketTransferBytes;
    long bucketTransferTimeInMilliseconds;
    int bucketTransfersCompleted;
    long primaryTransferTimeInMilliseconds;
    int primaryTransfersCompleted;
    long timeInMilliseconds;

    @Override
    public long getBucketCreateBytes() {
      return bucketCreateBytes;
    }

    public void setBucketCreateBytes(long getBucketCreateBytes) {
      this.bucketCreateBytes = getBucketCreateBytes;
    }

    @Override
    public long getBucketCreateTimeInMilliseconds() {
      return bucketCreateTimeInMilliseconds;
    }

    public void setBucketCreateTimeInMilliseconds(long bucketCreateTimeInMilliseconds) {
      this.bucketCreateTimeInMilliseconds = bucketCreateTimeInMilliseconds;
    }

    @Override
    public int getBucketCreatesCompleted() {
      return bucketCreatesCompleted;
    }

    public void setBucketCreatesCompleted(int bucketCreatesCompleted) {
      this.bucketCreatesCompleted = bucketCreatesCompleted;
    }

    @Override
    public long getBucketTransferBytes() {
      return bucketTransferBytes;
    }

    public void setBucketTransferBytes(long bucketTransferBytes) {
      this.bucketTransferBytes = bucketTransferBytes;
    }

    @Override
    public long getBucketTransferTimeInMilliseconds() {
      return bucketTransferTimeInMilliseconds;
    }

    public void setBucketTransferTimeInMilliseconds(long bucketTransferTimeInMilliseconds) {
      this.bucketTransferTimeInMilliseconds = bucketTransferTimeInMilliseconds;
    }

    @Override
    public int getBucketTransfersCompleted() {
      return bucketTransfersCompleted;
    }

    public void setBucketTransfersCompleted(int bucketTransfersCompleted) {
      this.bucketTransfersCompleted = bucketTransfersCompleted;
    }

    @Override
    public long getPrimaryTransferTimeInMilliseconds() {
      return primaryTransferTimeInMilliseconds;
    }

    public void setPrimaryTransferTimeInMilliseconds(long primaryTransferTimeInMilliseconds) {
      this.primaryTransferTimeInMilliseconds = primaryTransferTimeInMilliseconds;
    }

    @Override
    public int getPrimaryTransfersCompleted() {
      return primaryTransfersCompleted;
    }

    public void setPrimaryTransfersCompleted(int primaryTransfersCompleted) {
      this.primaryTransfersCompleted = primaryTransfersCompleted;
    }

    @Override
    public long getTimeInMilliseconds() {
      return timeInMilliseconds;
    }

    public void setTimeInMilliseconds(long timeInMilliseconds) {
      this.timeInMilliseconds = timeInMilliseconds;
    }

    @Override
    public String toString() {
      return "{" +
          "bucketCreateBytes=" + bucketCreateBytes +
          ", bucketCreateTimeInMilliseconds=" + bucketCreateTimeInMilliseconds +
          ", bucketCreatesCompleted=" + bucketCreatesCompleted +
          ", bucketTransferBytes=" + bucketTransferBytes +
          ", bucketTransferTimeInMilliseconds=" + bucketTransferTimeInMilliseconds +
          ", bucketTransfersCompleted=" + bucketTransfersCompleted +
          ", primaryTransferTimeInMilliseconds=" + primaryTransferTimeInMilliseconds +
          ", primaryTransfersCompleted=" + primaryTransfersCompleted +
          ", timeInMilliseconds=" + timeInMilliseconds +
          '}';
    }
  }
}
