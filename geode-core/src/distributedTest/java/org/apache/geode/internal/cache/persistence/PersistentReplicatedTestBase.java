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

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

public abstract class PersistentReplicatedTestBase extends JUnit4CacheTestCase {

  static String regionName = "region";

  private File diskDir;

  @Before
  public void setUpPersistentReplicatedTestBase() {
    invokeInEveryVM(() -> regionName = getUniqueName() + "Region");
    regionName = getUniqueName() + "Region";

    diskDir = new File("diskDir-" + getName()).getAbsoluteFile();
    deleteQuietly(diskDir);
    diskDir.mkdir();
    diskDir.deleteOnExit();
  }

  @After
  public void tearDownPersistentReplicatedTestBase() {
    deleteQuietly(diskDir);
  }

  void waitForBlockedInitialization(VM vm) {
    vm.invoke(() -> {
      await().until(() -> {
        PersistentMemberManager persistentMemberManager = getCache().getPersistentMemberManager();
        Map<String, Set<PersistentMemberID>> regions = persistentMemberManager.getWaitingRegions();
        return !regions.isEmpty();
      });
    });
  }

  void createPersistentRegionWithoutCompaction(VM vm) {
    vm.invoke(() -> {
      getCache();

      File dir = getDiskDirForVM(vm);
      dir.mkdirs();

      DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
      diskStoreFactory.setDiskDirs(new File[] {dir});
      diskStoreFactory.setMaxOplogSize(1);
      diskStoreFactory.setAutoCompact(false);
      diskStoreFactory.setAllowForceCompaction(true);
      diskStoreFactory.setCompactionThreshold(20);

      DiskStore diskStore = diskStoreFactory.create(regionName);

      RegionFactory regionFactory = new RegionFactory();
      regionFactory.setDiskStoreName(diskStore.getName());
      regionFactory.setDiskSynchronous(true);
      regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      regionFactory.setScope(Scope.DISTRIBUTED_ACK);

      regionFactory.create(regionName);
    });
  }

  void closeRegion(VM vm) {
    vm.invoke(() -> getCache().getRegion(regionName).close());
  }

  void closeCache(final VM vm) {
    vm.invoke(() -> getCache().close());
  }

  AsyncInvocation<Void> closeCacheAsync(VM vm) {
    return vm.invokeAsync(() -> getCache().close());
  }

  void createNonPersistentRegion(VM vm) {
    vm.invoke(() -> {
      getCache();

      RegionFactory regionFactory = new RegionFactory();
      regionFactory.setDataPolicy(DataPolicy.REPLICATE);
      regionFactory.setScope(Scope.DISTRIBUTED_ACK);

      regionFactory.create(regionName);
    });
  }

  AsyncInvocation<Void> createPersistentRegionWithWait(VM vm)
      throws ExecutionException, InterruptedException {
    return createPersistentRegion(vm, true);
  }

  void createPersistentRegion(VM vm) throws ExecutionException, InterruptedException {
    createPersistentRegion(vm, false);
  }

  private AsyncInvocation<Void> createPersistentRegion(VM vm,
      boolean createPersistentRegionWillWait)
      throws ExecutionException, InterruptedException {
    AsyncInvocation<Void> createPersistentRegionInVM = createPersistentRegionAsync(vm);

    if (createPersistentRegionWillWait) {
      createPersistentRegionInVM.join(500);
      assertThat(createPersistentRegionInVM.isAlive()).isTrue();
    } else {
      createPersistentRegionInVM.await();
    }

    return createPersistentRegionInVM;
  }

  AsyncInvocation<Void> createPersistentRegionAsync(VM vm) {
    return vm.invokeAsync(() -> {
      getCache();

      File dir = getDiskDirForVM(vm);
      dir.mkdirs();

      DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
      diskStoreFactory.setDiskDirs(new File[] {dir});
      diskStoreFactory.setMaxOplogSize(1);

      DiskStore diskStore = diskStoreFactory.create(regionName);

      RegionFactory regionFactory = new RegionFactory();
      regionFactory.setDiskStoreName(diskStore.getName());
      regionFactory.setDiskSynchronous(true);
      regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      regionFactory.setScope(Scope.DISTRIBUTED_ACK);

      regionFactory.create(regionName);
    });
  }

  File getDiskDirForVM(VM vm) {
    return new File(diskDir, String.valueOf(vm.getId()));
  }

  void backupDir(VM vm) throws IOException {
    File dirForVM = getDiskDirForVM(vm);
    File backupFile = new File(dirForVM.getParent(), dirForVM.getName() + ".bk");
    FileUtils.copyDirectory(dirForVM, backupFile);
  }

  void restoreBackup(VM vm) throws IOException {
    File dirForVM = getDiskDirForVM(vm);
    File backupFile = new File(dirForVM.getParent(), dirForVM.getName() + ".bk");
    if (!backupFile.renameTo(dirForVM)) {
      deleteDirectory(dirForVM);
      FileUtils.copyDirectory(backupFile, dirForVM);
      deleteDirectory(backupFile);
    }
  }
}
