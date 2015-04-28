package com.gemstone.gemfire.internal.cache.wan.parallel;

import com.gemstone.gemfire.GemFireException;

/**
 * The exception could be thrown if the BucketRegionQueue on which the operation
 * is happening is unavailable for some reason such as the BucketRegionQueue is 
 * being destroyed and hence getting cleaned up.
 */
public class BucketRegionQueueUnavailableException extends GemFireException {

  public BucketRegionQueueUnavailableException() {
  }
  
  public BucketRegionQueueUnavailableException(String msg) {
    super(msg);
  }
  
}
