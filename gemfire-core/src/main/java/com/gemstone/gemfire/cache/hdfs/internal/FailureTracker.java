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
