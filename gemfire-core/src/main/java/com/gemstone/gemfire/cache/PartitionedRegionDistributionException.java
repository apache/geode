/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * Indicates a failure to perform a distributed operation on a Partitioned Region 
 * after multiple attempts.
 *
 * @since 5.1
 * @author Mitch Thomas
 */
public class PartitionedRegionDistributionException extends
   CacheRuntimeException
{
  private static final long serialVersionUID = -3004093739855972548L;
   
  public PartitionedRegionDistributionException() {
    super();
  }

  public PartitionedRegionDistributionException(String msg) {
    super(msg);
  }

  public PartitionedRegionDistributionException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public PartitionedRegionDistributionException(Throwable cause) {
    super(cause);
  }
}
