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

import com.gemstone.gemfire.cache.Region;


/**
 * Composite data type used to distribute metrics for the JVM running
 * a GemFire member.
 * 
 * @author rishim
 * @since 7.0
 *
 */
public class JVMMetrics {

  /**
   * Number of GCs performed
   */
  private long gcCount;

  /**
   * Total gc time in milliseconds
   */
  private long gcTimeMillis;

  /**
   * Initial memory the vm requested from the operating system for this area in
   * bytes
   */
  private long initMemory;

  /**
   * The amount of used memory for this area, measured in bytes
   */
  private long committedMemory;

  /**
   * The amount of used memory for this area, measured in bytes
   */
  private long usedMemory;

  /**
   * represents the maximum amount of memory (in bytes) that can be used for memory
   * management
   */
  private long maxMemory;

  /**
   * Total number of threads
   */
  private int totalThreads;

  @ConstructorProperties( { "gcCount", "gcTimeMillis", "initMemory",
      "committedMemory", "usedMemory", "maxMemory", "totalThreads"
  })
  public JVMMetrics(long gcCount, long gcTimeMillis, long initMemory,
      long committedMemory, long usedMemory, long maxMemory,
      int totalThreads) {
    this.gcCount = gcCount;
    this.gcTimeMillis = gcTimeMillis;
    this.initMemory = initMemory;
    this.committedMemory = committedMemory;
    this.usedMemory = usedMemory;
    this.maxMemory = maxMemory;
    this.totalThreads = totalThreads;
  }

  /**
   * Returns the number of times garbage collection has occured.
   */
  public long getGcCount() {
    return gcCount;
  }

  /**
   * Returns the amount of time (in milliseconds) spent on garbage collection.
   */
  public long getGcTimeMillis() {
    return gcTimeMillis;
  }

  /**
   * Returns the initial number of megabytes of memory requested from the
   * operating system.
   */
  public long getInitMemory() {
    return initMemory;
  }

  /**
   * Returns the current number of megabytes of memory allocated.
   */
  public long getCommittedMemory() {
    return committedMemory;
  }
  
  /**
   * Returns the current number of megabytes of memory being used.
   */
  public long getUsedMemory() {
    return usedMemory;
  }

  /**
   * Returns the maximum number of megabytes of memory available from the
   * operating system.
   */
  public long getMaxMemory() {
    return maxMemory;
  }

  /**
   * Returns the number of threads in use.
   */
  public int getTotalThreads() {
    return totalThreads;
  }

  @Override
  public String toString() {
    return "{JVMMetrics : gcCount = " + gcCount + " gcTimeMillis = "
        + gcTimeMillis + " initMemory = " + initMemory + " committedMemory = "
        + committedMemory + " usedMemory = " + usedMemory + " maxMemory = "
        + maxMemory + " totalThreads = " + totalThreads + "}";
  }
}
