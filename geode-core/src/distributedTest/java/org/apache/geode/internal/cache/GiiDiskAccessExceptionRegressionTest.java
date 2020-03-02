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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Tests that if a node doing GII experiences DiskAccessException, it should also not try to recover
 * from the disk
 *
 * <p>
 * Bug: Regions with persistence remain in use after IOException have occurred
 */
public class GiiDiskAccessExceptionRegressionTest extends CacheTestCase {

  private String uniqueName;

  private File[] vm0DiskDirs;
  private File[] vm1DiskDirs;
  private File[] controllerDiskDirs;

  private VM vm0;
  private VM vm1;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();

    vm0DiskDirs = new File[] {temporaryFolder.newFolder(uniqueName + "_vm0_disk")};
    vm1DiskDirs = new File[] {temporaryFolder.newFolder(uniqueName + "_vm1_disk")};
    controllerDiskDirs = new File[] {temporaryFolder.newFolder(uniqueName + "_controller_disk")};

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;

    invokeInEveryVM(() -> {
      DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
    });

    addIgnoredException(uniqueName);
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;

    invokeInEveryVM(() -> {
      DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
    });
  }

  /**
   * If the node experiences disk access exception during GII, it should get destroyed & not attempt
   * to recover from the disk
   */
  @Test
  public void diskAccessExceptionDuringGiiShouldShutdown() throws Exception {
    vm0.invoke(() -> createCacheForVM0());
    vm1.invoke(() -> createCacheForVM1());

    // Create DiskRegion locally in controller VM also
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(controllerDiskDirs);

    DiskStore diskStore = diskStoreFactory.create(uniqueName);

    InternalRegionFactory factory = getCache().createInternalRegionFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(diskStore.getName());

    Region<Integer, Integer> region = factory.create(uniqueName);

    // Now put entries in the disk region
    for (int i = 0; i < 100; ++i) {
      region.put(i, i);
    }

    // Now close the region in the controller VM
    region.close();

    // Now recreate the region but set the factory such that disk region entry object
    // used is customized by us to throw exception while writing to disk

    DistributedRegion distributedRegion =
        new DistributedRegion(uniqueName, factory.getCreateAttributes(), null,
            getCache(),
            new InternalRegionArguments().setDestroyLockFlag(true).setRecreateFlag(false)
                .setSnapshotInputStream(null).setImageTarget(null),
            disabledClock());

    distributedRegion.entries.setEntryFactory(new DiskRegionEntryThrowsFactory());

    factory.setInternalMetaRegion(distributedRegion);
    factory.setDestroyLockFlag(true);
    factory.setSnapshotInputStream(null);
    factory.setImageTarget(null);

    assertThatThrownBy(() -> factory.create(uniqueName))
        .isInstanceOf(DiskAccessException.class);
  }

  /**
   * This method is used to create Cache in VM0
   */
  private void createCacheForVM0() {
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(vm0DiskDirs);

    DiskStore diskStore = diskStoreFactory.create(uniqueName);

    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setDiskStoreName(diskStore.getName());
    factory.setDiskSynchronous(false);
    factory.setScope(Scope.DISTRIBUTED_ACK);

    getCache().createRegion(uniqueName, factory.create());
  }

  /**
   * This method is used to create Cache in VM1
   */
  private void createCacheForVM1() {
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(vm1DiskDirs);

    DiskStore diskStore = diskStoreFactory.create(uniqueName);

    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setDiskStoreName(diskStore.getName());
    factory.setDiskSynchronous(false);
    factory.setScope(Scope.DISTRIBUTED_ACK);

    getCache().createRegion(uniqueName, factory.create());
  }

  private class DiskRegionEntryThrowsFactory implements RegionEntryFactory {

    @Override
    public RegionEntry createEntry(RegionEntryContext context, Object key, Object value) {
      throw new DiskAccessException(new IOException(uniqueName));
    }

    @Override
    public Class getEntryClass() {
      return getClass();
    }

    @Override
    public RegionEntryFactory makeVersioned() {
      return this;
    }

    @Override
    public RegionEntryFactory makeOnHeap() {
      return this;
    }
  }
}
