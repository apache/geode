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
package com.gemstone.gemfire.internal.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.RegionFactoryImpl;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

public abstract class PersistentReplicatedTestBase extends CacheTestCase {

  protected static final int MAX_WAIT = 30 * 1000;
  protected static String REGION_NAME = "region";
  protected File diskDir;
  protected static String SAVED_ACK_WAIT_THRESHOLD;

  public PersistentReplicatedTestBase(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    Invoke.invokeInEveryVM(PersistentReplicatedTestBase.class,"setRegionName", new Object[]{getUniqueName()});
    setRegionName(getUniqueName());
    diskDir = new File("diskDir-" + getName()).getAbsoluteFile();
    com.gemstone.gemfire.internal.FileUtil.delete(diskDir);
    diskDir.mkdir();
    diskDir.deleteOnExit();
  }

  public static void setRegionName(String testName) {
    REGION_NAME = testName + "Region";
  }
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    com.gemstone.gemfire.internal.FileUtil.delete(diskDir);
    postTearDownPersistentReplicatedTestBase();
  }
  
  protected void postTearDownPersistentReplicatedTestBase() throws Exception {
  }

  protected void waitForBlockedInitialization(VM vm) {
    vm.invoke(new SerializableRunnable() {
  
      public void run() {
        Wait.waitForCriterion(new WaitCriterion() {
  
          public String description() {
            return "Waiting for another persistent member to come online";
          }
          
          public boolean done() {
            GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
            PersistentMemberManager mm = cache.getPersistentMemberManager();
            Map<String, Set<PersistentMemberID>> regions = mm.getWaitingRegions();
            boolean done = !regions.isEmpty();
            return done;
          }
          
        }, MAX_WAIT, 100, true);
        
      }
      
    });
  }

  protected SerializableRunnable createPersistentRegionWithoutCompaction(final VM vm0) {
    SerializableRunnable createRegion = new SerializableRunnable("Create persistent region") {
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        File dir = getDiskDirForVM(vm0);
        dir.mkdirs();
        dsf.setDiskDirs(new File[] {dir});
        dsf.setMaxOplogSize(1);
        dsf.setAutoCompact(false);
        dsf.setAllowForceCompaction(true);
        dsf.setCompactionThreshold(20);
        DiskStore ds = dsf.create(REGION_NAME);
        RegionFactory rf = new RegionFactory();
        rf.setDiskStoreName(ds.getName());
        rf.setDiskSynchronous(true);
        rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.create(REGION_NAME);
      }
    };
    vm0.invoke(createRegion);
    return createRegion;
  }

  protected void closeRegion(final VM vm) {
    SerializableRunnable closeRegion = new SerializableRunnable("Close persistent region") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.close();
      }
    };
    vm.invoke(closeRegion);
  }

  protected void closeCache(final VM vm) {
    SerializableRunnable closeCache = new SerializableRunnable("close cache") {
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    };
    vm.invoke(closeCache);
  }

  protected AsyncInvocation closeCacheAsync(VM vm0) {
    SerializableRunnable close = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    };
    
    return vm0.invokeAsync(close);
  }

  protected void createNonPersistentRegion(VM vm) throws Throwable {
    SerializableRunnable createRegion = new SerializableRunnable("Create non persistent region") {
      public void run() {
        Cache cache = getCache();
        RegionFactory rf = new RegionFactory();
        rf.setDataPolicy(DataPolicy.REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.create(REGION_NAME);
      }
    };
    vm.invoke(createRegion);
  }

  protected AsyncInvocation createPersistentRegionWithWait(VM vm) throws Throwable {
    return _createPersistentRegion(vm, true);
  }
  protected void createPersistentRegion(VM vm) throws Throwable {
    _createPersistentRegion(vm, false);
  }
  private AsyncInvocation _createPersistentRegion(VM vm, boolean wait) throws Throwable {
    AsyncInvocation future = createPersistentRegionAsync(vm);
    long waitTime = wait ? 500 : MAX_WAIT;
    future.join(waitTime);
    if(future.isAlive() && !wait) {
      fail("Region not created within" + MAX_WAIT);
    }
    if (!future.isAlive() && wait) {
      fail("Did not expecte region creation to complete");
    }
    if(!wait && future.exceptionOccurred()) {
      throw new RuntimeException(future.getException());
    }
    return future;
  }

  protected AsyncInvocation createPersistentRegionAsync(final VM vm) {
    SerializableRunnable createRegion = new SerializableRunnable("Create persistent region") {
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        File dir = getDiskDirForVM(vm);
        dir.mkdirs();
        dsf.setDiskDirs(new File[] {dir});
        dsf.setMaxOplogSize(1);
        DiskStore ds = dsf.create(REGION_NAME);
        RegionFactory rf = new RegionFactory();
        rf.setDiskStoreName(ds.getName());
        rf.setDiskSynchronous(true);
        rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.create(REGION_NAME);
      }
    };
    return vm.invokeAsync(createRegion);
  }

  protected File getDiskDirForVM(final VM vm) {
    File dir = new File(diskDir, String.valueOf(vm.getPid()));
    return dir;
  }

  protected void backupDir(VM vm) throws IOException {
    File dirForVM = getDiskDirForVM(vm);
    File backFile = new File(dirForVM.getParent(), dirForVM.getName() + ".bk");
    FileUtil.copy(dirForVM, backFile);
  }

  protected void restoreBackup(VM vm) throws IOException {
    File dirForVM = getDiskDirForVM(vm);
    File backFile = new File(dirForVM.getParent(), dirForVM.getName() + ".bk");
    if(!backFile.renameTo(dirForVM)) {
      FileUtil.delete(dirForVM);
      FileUtil.copy(backFile, dirForVM);
      FileUtil.delete(backFile);
    }
  }

}
