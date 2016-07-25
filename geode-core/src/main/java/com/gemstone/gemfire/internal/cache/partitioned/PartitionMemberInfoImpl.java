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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.Serializable;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Implements PartitionMemberInfo. Serializable form is used to allow JMX 
 * MBeans to use this as a remotable return type.
 * 
 */
public class PartitionMemberInfoImpl 
implements InternalPartitionDetails, Serializable {

  private static final long serialVersionUID = 8245020687604034289L;
  private final DistributedMember distributedMember;
  private final long configuredMaxMemory;
  private final long size; // megabytes
  private final int bucketCount;
  private final int primaryCount;
  private final transient PRLoad prLoad; // do not serialize for JMX mbeans
  private final transient long[] bucketSizes; // do not serialize for JMX mbeans

  public PartitionMemberInfoImpl(DistributedMember distributedMember,
                             long configuredMaxMemory,
                             long size,
                             int bucketCount,
                             int primaryCount) {
    this.distributedMember = distributedMember;
    this.configuredMaxMemory = configuredMaxMemory;
    this.size = size;
    this.bucketCount = bucketCount;
    this.primaryCount = primaryCount;
    this.prLoad = null;
    this.bucketSizes = null;
  }
  
  public PartitionMemberInfoImpl(DistributedMember distributedMember,
                             long configuredMaxMemory,
                             long size,
                             int bucketCount,
                             int primaryCount,
                             PRLoad prLoad,
                             long[] bucketSizes) {
    //TODO rebalance disabling this unit bug 39868 is fixed. 
//    Assert.assertTrue(size >= 0);
    this.distributedMember = distributedMember;
    this.configuredMaxMemory = configuredMaxMemory;
    this.size = size;
    this.bucketCount = bucketCount;
    this.primaryCount = primaryCount;
    this.prLoad = prLoad;
    this.bucketSizes = bucketSizes;
  }
  
  public int getBucketCount() {
    return this.bucketCount;
  }

  public long getConfiguredMaxMemory() {
    return this.configuredMaxMemory;
  }

  public DistributedMember getDistributedMember() {
    return this.distributedMember;
  }

  public int getPrimaryCount() {
    return this.primaryCount;
  }

  public long getSize() {
    return this.size; // bytes
  }

  public PRLoad getPRLoad() {
    return this.prLoad;
  }
  
  public long getBucketSize(int bucketId) { // bytes
    if (this.bucketSizes == null) {
      throw new IllegalStateException(this + " has no bucketSizes");
    }
    if (bucketId < 0 || bucketId > this.bucketSizes.length) {
      throw new IllegalArgumentException(
          "bucketId must be between 0 and " + this.bucketSizes.length);
    }
    return this.bucketSizes[bucketId];
  }
  
  /**
   * Returns the backed array of bucket bytes. Index is the BID and the value
   * is either 0 or positive. Please do not change the values! Internal use
   * only.
   * 
   * @return the backed array of bucket bytes
   */
  public long[] getBucketSizes() {
    return this.bucketSizes; // bytes
  }
  
  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("[PartitionMemberInfoImpl: ");
    sb.append("distributedMember=").append(this.distributedMember);
    sb.append(", configuredMaxMemory=").append(this.configuredMaxMemory);
    sb.append(", size=").append(this.size);
    sb.append(", bucketCount=").append(this.bucketCount);
    sb.append(", primaryCount=").append(this.primaryCount);
    if (this.prLoad != null) {
      sb.append(", prLoad=").append(this.prLoad);
      sb.append(", bucketSizes=[");
      if (this.bucketSizes == null) {
        sb.append("null");
      } else {
        for (int i = 0; i < this.bucketSizes.length; i ++) {
          sb.append(this.bucketSizes[i]).append(", ");
        }
        sb.append("]");
      }
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * hashCode is defined for this class to make
   * sure that the before details and after details for
   * RebalanceResults are in the same order. This makes
   * debugging printouts easier, and it also removes
   * discrepancies due to rounding errors when calculating
   * the stddev in tests.
   */
  @Override
  public int hashCode() {
    return distributedMember.hashCode();
  }
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PartitionMemberInfoImpl)) {
      return false;
    }
    PartitionMemberInfoImpl o = (PartitionMemberInfoImpl)other;
    return this.distributedMember.equals(o.distributedMember);
  }
  public int compareTo(InternalPartitionDetails other) {
    // memberId is InternalDistributedMember which implements Comparable
    return this.distributedMember.compareTo(other.getDistributedMember());
  }
}
