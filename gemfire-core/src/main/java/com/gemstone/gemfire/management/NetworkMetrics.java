/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import com.gemstone.gemfire.cache.DiskStore;

/**
 * Composite data type used to distribute network related metrics for a member.
 * 
 * @author rishim
 * @since 7.0
 *
 */
public class NetworkMetrics {
  private float bytesReceivedRate;
  private float bytesSentRate;
  
  /**
   * Returns the average number of bytes per second received.
   */
  public float getBytesReceivedRate() {
    return bytesReceivedRate;
  }

  /**
   * Returns the average number of bytes per second sent.
   */
  public float getBytesSentRate() {
    return bytesSentRate;
  }

  /**
   * Sets the average number of bytes per second received.
   */
  public void setBytesReceivedRate(float bytesReceivedRate) {
    this.bytesReceivedRate = bytesReceivedRate;
  }

  /**
   * Sets the average number of bytes per second sent.
   */
  public void setBytesSentRate(float bytesSentRate) {
    this.bytesSentRate = bytesSentRate;
  }
}
