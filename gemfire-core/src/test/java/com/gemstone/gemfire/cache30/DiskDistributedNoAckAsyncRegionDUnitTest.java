/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.*;
import java.io.*;
import com.gemstone.gemfire.internal.OSProcess;

/**
 *
 * @author Eric Zoerner
 *
 */
public class DiskDistributedNoAckAsyncRegionDUnitTest extends DiskDistributedNoAckRegionTestCase {
  
  /** Creates a new instance of DiskDistributedNoAckSyncOverflowRegionTest */
  public DiskDistributedNoAckAsyncRegionDUnitTest(String name) {
    super(name);
  }
  
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    
    File[] diskDirs = new File[1];
    diskDirs[0] = new File("diskRegionDirs/" + OSProcess.getId());
    diskDirs[0].mkdirs();
    factory.setDiskStoreName(getCache().createDiskStoreFactory()
                             .setDiskDirs(diskDirs)
                             .setTimeInterval(1000)
                             .setQueueSize(0)
                             .create("DiskDistributedNoAckAsyncRegionDUnitTest")
                             .getName());
    factory.setDiskSynchronous(false);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    return factory.create();
  }
 
}
