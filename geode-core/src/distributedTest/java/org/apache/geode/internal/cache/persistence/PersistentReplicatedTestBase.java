/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.persistence;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

public abstract class PersistentReplicatedTestBase extends JUnit4CacheTestCase {

  protected static final int MAX_WAIT = 60 * 1000;
  protected static String REGION_NAME = "region";
  protected File diskDir;
  protected static String SAVED_ACK_WAIT_THRESHOLD;

  @Override
  public final void postSetUp() throws Exception {
    Invoke.invokeInEveryVM(PersistentReplicatedTestBase.class, "setRegionName",
        new Object[] {getUniqueName()});
    setRegionName(getUniqueName());
    diskDir = new File("diskDir-" + getName()).getAbsoluteFile();
    FileUtils.deleteDirectory(diskDir);
    diskDir.mkdir();
    diskDir.deleteOnExit();
  }

  public static void setRegionName(String testName) {
    REGION_NAME = testName + "Region";
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    FileUtils.deleteDirectory(diskDir);
    postTearDownPersistentReplicatedTestBase();
  }

  protected void postTearDownPersistentReplicatedTestBase() throws Exception {}

  protected void waitForBlockedInitialization(VM vm) {
    vm.invoke(new SerializableRunnable() {

      public void run() {
        GeodeAwaitility.await().untilAsserted(new WaitCriterion() {

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

        });

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

  protected void createNonPersistentRegion(VM vm) throws Exception {
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

  protected AsyncInvocation createPersistentRegionWithWait(VM vm) throws Exception {
    return _createPersistentRegion(vm, true);
  }

  protected void createPersistentRegion(VM vm) throws Exception {
    _createPersistentRegion(vm, false);
  }

  private AsyncInvocation _createPersistentRegion(VM vm, boolean wait) throws Exception {
    AsyncInvocation future = createPersistentRegionAsync(vm);
    long waitTime = wait ? 500 : MAX_WAIT;
    future.join(waitTime);
    if (future.isAlive() && !wait) {
      fail("Region not created within" + MAX_WAIT);
    }
    if (!future.isAlive() && wait) {
      fail("Did not expect region creation to complete");
    }
    if (!wait && future.exceptionOccurred()) {
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
    File dir = new File(diskDir, String.valueOf(vm.getId()));
    return dir;
  }

  protected void backupDir(VM vm) throws IOException {
    File dirForVM = getDiskDirForVM(vm);
    File backFile = new File(dirForVM.getParent(), dirForVM.getName() + ".bk");
    FileUtils.copyDirectory(dirForVM, backFile);
  }

  protected void restoreBackup(VM vm) throws IOException {
    File dirForVM = getDiskDirForVM(vm);
    File backFile = new File(dirForVM.getParent(), dirForVM.getName() + ".bk");
    if (!backFile.renameTo(dirForVM)) {
      FileUtils.deleteDirectory(dirForVM);
      FileUtils.copyDirectory(backFile, dirForVM);
      FileUtils.deleteDirectory(backFile);
    }
  }

}
