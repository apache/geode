/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.cache.hdfs.internal;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * Class for tracking failures and backing off if necessary.
 * @author dsmith
 *
 */
public class FailureTracker  extends ThreadLocal<MutableInt> {
  private final long minTime;
  private final long maxTime;
  private final float rate;
  private final FailureCount waitTime = new FailureCount();
  
  
  /**
   * @param minTime the minimum wait time after a failure in ms.
   * @param maxTime the maximum wait tim after a failure, in ms.
   * @param rate the rate of growth of the failures
   */
  public FailureTracker(long minTime, long maxTime, float rate) {
    this.minTime = minTime;
    this.maxTime = maxTime;
    this.rate = rate;
  }
  
  /**
   * Wait for the current wait time.
   */
  public void sleepIfRetry() throws InterruptedException {
      Thread.sleep(waitTime());
  }

  /**
   * @return the wait time = rate^(num_failures) * minTime
   */
  public long waitTime() {
    return waitTime.get().longValue();
  }
  
  public void record(boolean success) {
    if(success) {
      success();
    } else {
      failure();
    }
    
  }
  
  public void success() {
    waitTime.get().setValue(0);
    
  }
  public void failure() {
    long current = waitTime.get().intValue();
    if(current == 0) {
      current=minTime;
    }
    else if(current < maxTime) {
      current = (long) (current * rate);
    }
    waitTime.get().setValue(Math.min(current, maxTime));
  }


  private static class FailureCount extends ThreadLocal<MutableLong> {

    @Override
    protected MutableLong initialValue() {
      return new MutableLong();
    }
  }


  
}
