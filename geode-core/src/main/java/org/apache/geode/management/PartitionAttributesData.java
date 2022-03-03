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
package org.apache.geode.management;

import java.beans.ConstructorProperties;
import java.util.Arrays;

import org.apache.geode.internal.cache.PartitionedRegion;

/**
 * Composite date type used to distribute attributes for a {@link PartitionedRegion}.
 *
 * @since GemFire 7.0
 */
public class PartitionAttributesData {

  private final int redundantCopies;
  private final long totalMaxMemory;
  // Total number of buckets for whole region
  private final int totalNumBuckets;
  private final int localMaxMemory;
  private final String colocatedWith;
  private final String partitionResolver;
  private final long recoveryDelay;
  private final long startupRecoveryDelay;
  private final String[] partitionListeners;

  /**
   * This constructor is to be used by internal JMX framework only. User should not try to create an
   * instance of this class.
   */
  @ConstructorProperties({"redundantCopies", "totalMaxMemory", "totalNumBuckets", "localMaxMemory",
      "colocatedWith", "partitionResolver", "recoveryDelay", "startupRecoveryDelay",
      "partitionListeners"})
  public PartitionAttributesData(int redundantCopies, long totalMaxMemory, int totalNumBuckets,
      int localMaxMemory, String colocatedWith, String partitionResolver, long recoveryDelay,
      long startupRecoveryDelay, String[] partitionListeners) {

    this.redundantCopies = redundantCopies;
    this.totalMaxMemory = totalMaxMemory;
    this.totalNumBuckets = totalNumBuckets;
    this.localMaxMemory = localMaxMemory;
    this.colocatedWith = colocatedWith;
    this.partitionResolver = partitionResolver;
    this.recoveryDelay = recoveryDelay;
    this.startupRecoveryDelay = startupRecoveryDelay;
    this.partitionListeners = partitionListeners;
  }

  /**
   * Returns the number of redundant copies for this PartitionedRegion.
   */
  public int getRedundantCopies() {
    return redundantCopies;
  }

  /**
   * Returns the maximum total size (in megabytes) of the Region.
   */
  public long getTotalMaxMemory() {
    return totalMaxMemory;
  }

  /**
   * Returns the total number of buckets for the whole region.
   */
  public int getTotalNumBuckets() {
    return totalNumBuckets;
  }

  /**
   * Returns the maximum amount of local memory that can be used by the region.
   */
  public int getLocalMaxMemory() {
    return localMaxMemory;
  }

  /**
   * Returns the name of the PartitionedRegion that this PartitionedRegion is colocated with.
   */
  public String getColocatedWith() {
    return colocatedWith;
  }

  /**
   * Returns a list of Classes that are configured as resolvers for the Region.
   */
  public String getPartitionResolver() {
    return partitionResolver;
  }

  /**
   * Returns the delay (in milliseconds) that a member will wait while trying to satisfy the
   * redundancy of data hosted on other members.
   */
  public long getRecoveryDelay() {
    return recoveryDelay;
  }

  /**
   * Returns the delay (in milliseconds) that a new member will wait while trying to satisfy the
   * redundancy of data hosted on other members.
   */
  public long getStartupRecoveryDelay() {
    return startupRecoveryDelay;
  }

  /**
   * Returns a list of Classes that are configured as listeners for the Region.
   */
  public String[] getPartitionListeners() {
    return partitionListeners;
  }

  /**
   * String representation of PartitionAttributesData
   */
  @Override
  public String toString() {
    return "PartitionAttributesData [colocatedWith=" + colocatedWith
        + ", localMaxMemory=" + localMaxMemory
        + ", partitionListeners=" + Arrays.toString(partitionListeners)
        + ", partitionResolver=" + partitionResolver
        + ", recoveryDelay=" + recoveryDelay
        + ", redundantCopies=" + redundantCopies
        + ", startupRecoveryDelay=" + startupRecoveryDelay
        + ", totalMaxMemory=" + totalMaxMemory
        + ", totalNumBuckets=" + totalNumBuckets + "]";
  }
}
