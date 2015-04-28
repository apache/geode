/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management;

import java.beans.ConstructorProperties;

import com.gemstone.gemfire.cache.Region;

/**
 * Composite date type used to distribute the fixed partition attributes for
 * a {@link Region}.
 * 
 * @author rishim
 * @since 7.0
 */
public class FixedPartitionAttributesData {

  /**
   * Name of the Fixed partition
   */
  private String name;

  /**
   * whether this is the primary partition
   */
  private boolean primary;

  /**
   * Number of buckets in the partition
   */
  private int numBucket;

  @ConstructorProperties( { "name", "primary", "numBucket"

  })
  public FixedPartitionAttributesData(String name, boolean primary,
      int numBucket) {
    this.name = name;
    this.primary = primary;
    this.numBucket = numBucket;
  }

  /**
   * Returns the name of the partition.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns whether this member is the primary for the partition.
   * 
   * @return True if this member is the primary, false otherwise.
   */
  public boolean isPrimary() {
    return primary;
  }

  /**
   * Returns the number of buckets allowed for the partition.
   */
  public int getNumBucket() {
    return numBucket;
  }

  @Override
  public String toString() {
    return "FixedPartitionAttributesData [name=" + name + ", numBucket="
        + numBucket + ", primary=" + primary + "]";
  }


}
