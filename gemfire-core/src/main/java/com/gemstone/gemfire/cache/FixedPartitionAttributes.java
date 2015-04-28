/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.internal.cache.FixedPartitionAttributesImpl;

/**
 * Composite date type used to distribute the attributes for a fixed partition.
 * </p>
 * 
 * {@link com.gemstone.gemfire.cache.PartitionAttributes#getFixedPartitionAttributes()}
 * returns all fixed partitions in Partitioned Region attributes. </p>
 * {@link com.gemstone.gemfire.cache.PartitionAttributesFactory#addFixedPartitionAttributes(FixedPartitionAttributes)}
 * configures <code>FixedPartitionAttributes</Code> in
 * <code>PartitionedRegionAttributes</code> </p>
 * 
 * @see com.gemstone.gemfire.cache.PartitionAttributes
 * @see com.gemstone.gemfire.cache.PartitionAttributesFactory
 * 
 * @author kbachhav
 * @since 6.6
 */

public abstract class FixedPartitionAttributes {
  
  private final static boolean DEFAULT_PRIMARY_STATUS = false;
  
  private final static int DEFAULT_NUM_BUCKETS = 1;

  /**
   * Creates an instance of <code>FixedPartitionAttributes</code>.
   * 
   * @param name
   *          Name of the fixed partition.
   */
  final public static FixedPartitionAttributes createFixedPartition(String name) {
    return new FixedPartitionAttributesImpl().setPartitionName(name).isPrimary(
        DEFAULT_PRIMARY_STATUS).setNumBuckets(DEFAULT_NUM_BUCKETS);
  }

  /**
   * Creates an instance of <code>FixedPartitionAttributes</code>.
   * 
   * @param name
   *          Name of the fixed partition.
   * @param isPrimary
   *          True if this member is the primary for the partition.
   */
  final public static FixedPartitionAttributes createFixedPartition(
      String name, boolean isPrimary) {
    return new FixedPartitionAttributesImpl().setPartitionName(name).isPrimary(
        isPrimary).setNumBuckets(DEFAULT_NUM_BUCKETS);
  }

  /**
   * Creates an instance of <code>FixedPartitionAttributes</code>.
   * 
   * @param name
   *          Name of the fixed partition.
   * @param isPrimary
   *          True if this member is the primary for the partition.
   * @param numBuckets
   *          Number of buckets allowed for the partition.
   */
  final public static FixedPartitionAttributes createFixedPartition(
      String name, boolean isPrimary, int numBuckets) {
    return new FixedPartitionAttributesImpl().setPartitionName(name).isPrimary(
        isPrimary).setNumBuckets(numBuckets);
  }

  /**
   * Creates an instance of <code>FixedPartitionAttributes</code>.
   * 
   * @param name
   *          Name of the fixed partition.
   * @param numBuckets
   *          Number of buckets allowed for the partition.
   */
  final public static FixedPartitionAttributes createFixedPartition(
      String name, int numBuckets) {
    return new FixedPartitionAttributesImpl().setPartitionName(name).isPrimary(
        DEFAULT_PRIMARY_STATUS).setNumBuckets(numBuckets);
  }

  /**
   * Returns the name of the fixed partition.
   */
  public abstract String getPartitionName();

  /**
   * Returns whether this member is the primary for the partition.
   * 
   * @return True if this member is the primary, false otherwise.
   */
  public abstract boolean isPrimary();

  /**
   * Returns the number of buckets allowed for the partition.
   */
  public abstract int getNumBuckets();
}
