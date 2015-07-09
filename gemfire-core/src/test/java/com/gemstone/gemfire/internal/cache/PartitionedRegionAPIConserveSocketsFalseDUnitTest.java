/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * Test all the PartitionedRegion api calls when ConserveSockets is set to false
 * @author mthomas
 * @since 5.0
 * @see com.gemstone.gemfire.distributed.DistributedSystem#setThreadsSocketPolicy(boolean)
 */
public class PartitionedRegionAPIConserveSocketsFalseDUnitTest extends
    PartitionedRegionAPIDUnitTest
{

  public PartitionedRegionAPIConserveSocketsFalseDUnitTest(String name) {
    super(name);
  }


  public Properties getDistributedSystemProperties()
  {
    Properties ret = new Properties();
    ret.setProperty(DistributionConfig.CONSERVE_SOCKETS_NAME, "false");
    return ret; 
  }

}
