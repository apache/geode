/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import java.io.File;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.DiskWriteAttributesFactory;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;

import dunit.AsyncInvocation;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * @author dsmith
 *
 */
public class PersistentRecoveryOrderOldConfigDUnitTest extends
    PersistentRecoveryOrderDUnitTest {

  public PersistentRecoveryOrderOldConfigDUnitTest(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }
  
  @Override
  protected AsyncInvocation createPersistentRegionAsync(final VM vm) {
    SerializableRunnable createRegion = new SerializableRunnable("Create persistent region") {
      public void run() {
        Cache cache = getCache();
        File dir = getDiskDirForVM(vm);
        dir.mkdirs();
        RegionFactory rf = new RegionFactory();
//        rf.setDiskSynchronous(true);
        rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.setDiskDirs(new File[] {dir});
        DiskWriteAttributesFactory dwf = new DiskWriteAttributesFactory();
        dwf.setMaxOplogSize(1);
        dwf.setSynchronous(true);
        rf.setDiskWriteAttributes(dwf.create());
        rf.create(REGION_NAME);
      } 
    };
    return vm.invokeAsync(createRegion);
  }

}
