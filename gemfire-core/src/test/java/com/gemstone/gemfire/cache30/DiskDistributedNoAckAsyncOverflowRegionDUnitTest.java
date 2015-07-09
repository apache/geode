/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache30;

import java.io.File;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.OSProcess;

/**
 *
 * @author Eric Zoerner
 *
 */
public class DiskDistributedNoAckAsyncOverflowRegionDUnitTest extends DiskDistributedNoAckRegionTestCase {
  
  /** Creates a new instance of DiskDistributedNoAckSyncOverflowRegionTest */
  public DiskDistributedNoAckAsyncOverflowRegionDUnitTest(String name) {
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
                             .create("DiskDistributedNoAckAsyncOverflowRegionDUnitTest")
                             .getName());
   
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUMemoryAttributes(1, null, EvictionAction.OVERFLOW_TO_DISK));
    factory.setDiskSynchronous(false);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    return factory.create();
  }
}
