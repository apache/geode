/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import java.beans.ConstructorProperties;
import java.util.Arrays;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * Composite date type used to distribute attributes for a {@link PartitionedRegion}.
 * 
 * @author rishim
 * @since 7.0
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
   * This constructor is to be used by internal JMX framework only. User should
   * not try to create an instance of this class.
   */
  @ConstructorProperties({ "redundantCopies", "totalMaxMemory", "totalNumBuckets", "localMaxMemory", "colocatedWith", "partitionResolver",
      "recoveryDelay", "startupRecoveryDelay", "partitionListeners" })
  public PartitionAttributesData(int redundantCopies, long totalMaxMemory, int totalNumBuckets, int localMaxMemory, String colocatedWith,
      String partitionResolver, long recoveryDelay, long startupRecoveryDelay, String[] partitionListeners) {

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
   * Returns the name of the PartitionedRegion that this PartitionedRegion is
   * colocated with.
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
   * Returns the delay (in milliseconds) that a member will wait while trying
   * to satisfy the redundancy of data hosted on other members.
   */
  public long getRecoveryDelay() {
    return recoveryDelay;
  }

  /**
   * Returns the delay (in milliseconds) that a new member will wait while trying
   * to satisfy the redundancy of data hosted on other members.
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
    StringBuilder sb = new StringBuilder();
    sb.append("PartitionAttributesData [colocatedWith=").append(colocatedWith);
    sb.append(", localMaxMemory=").append(localMaxMemory);
    sb.append(", partitionListeners=").append(Arrays.toString(partitionListeners));
    sb.append(", partitionResolver=").append(partitionResolver);
    sb.append(", recoveryDelay=").append(recoveryDelay);
    sb.append(", redundantCopies=").append(redundantCopies);
    sb.append(", startupRecoveryDelay=").append(startupRecoveryDelay);
    sb.append(", totalMaxMemory=").append(totalMaxMemory);
    sb.append(", totalNumBuckets=").append(totalNumBuckets + "]");
    return sb.toString();
  }
}
