/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

/**
 * Test only hand back object passed to
 * 
 * @author Amardeep Rajpal
 * 
 */
public class TestHeapThresholdObserver {

  private int threshold;

  private long maxMemory;

  private long thresholdInBytes;

  private long delta;

  public TestHeapThresholdObserver(int threshold) {
    this.threshold = threshold;
  }

  public long getDelta() {
    return delta;
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public int getThreshold() {
    return threshold;
  }

  public long getThresholdInBytes() {
    return thresholdInBytes;
  }

  public void setNotificationInfo(int threshold, long maxMemory,
      long thresholdInBytes, long delta) {
    this.threshold = threshold;
    this.maxMemory = maxMemory;
    this.thresholdInBytes = thresholdInBytes;
    this.delta = delta;
  }
}
