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

import org.apache.geode.management.runtime.RebalanceRegionResult;

public class RebalanceRegionResultImpl implements RebalanceRegionResult {
  private String regionName;
  private long bucketCreateBytes;
  private long bucketCreateTimeInMilliseconds;
  private int bucketCreatesCompleted;
  private long bucketTransferBytes;
  private long bucketTransferTimeInMilliseconds;
  private int bucketTransfersCompleted;
  private long primaryTransferTimeInMilliseconds;
  private int primaryTransfersCompleted;
  private long timeInMilliseconds;
  private int numOfMembers;

  @Override
  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

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
  public int getNumOfMembers() {
    return numOfMembers;
  }

  public void setNumOfMembers(int numOfMembers) {
    this.numOfMembers = numOfMembers;
  }

  @Override
  public String toString() {
    return "{bucketCreateBytes=" + bucketCreateBytes +
        ", bucketCreateTimeInMilliseconds=" + bucketCreateTimeInMilliseconds +
        ", bucketCreatesCompleted=" + bucketCreatesCompleted +
        ", bucketTransferBytes=" + bucketTransferBytes +
        ", bucketTransferTimeInMilliseconds=" + bucketTransferTimeInMilliseconds +
        ", bucketTransfersCompleted=" + bucketTransfersCompleted +
        ", primaryTransferTimeInMilliseconds=" + primaryTransferTimeInMilliseconds +
        ", primaryTransfersCompleted=" + primaryTransfersCompleted +
        ", timeInMilliseconds=" + timeInMilliseconds +
        ", numOfMembers=" + numOfMembers +
        ", regionName=" + regionName +
        '}';
  }
}
