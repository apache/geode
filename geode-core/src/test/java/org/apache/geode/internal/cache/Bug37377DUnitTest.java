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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.internal.cache.entries.VersionedThinDiskRegionEntryHeapObjectKey;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

/**
 * Bug37377 DUNIT Test: The Clear operation during a GII in progress can leave a Entry in the Oplog
 * due to a race condition wherein the clearFlag getting set after the entry gets written to the
 * disk, The Test verifies the existence of the scenario.
 * 
 */

@Category(DistributedTest.class)
public class Bug37377DUnitTest extends JUnit4CacheTestCase {

  protected static String regionName = "TestRegion";

  static Properties props = new Properties();

  protected static DistributedSystem distributedSystem = null;

  VM vm0, vm1;

  protected static Cache cache = null;

  protected static File[] dirs = null;

  private static final int maxEntries = 10000;

  transient private static CountDownLatch clearLatch = new CountDownLatch(1);

  static Boolean clearOccurred = false;

  public Bug37377DUnitTest() {
    super();
    File file1 = new File(getTestMethodName() + "1");
    file1.mkdir();
    file1.deleteOnExit();
    File file2 = new File(getTestMethodName() + "2");
    file2.mkdir();
    file2.deleteOnExit();
    dirs = new File[2];
    dirs[0] = file1;
    dirs[1] = file2;
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    vm1.invoke(() -> destroyRegion());
    vm0.invoke(() -> destroyRegion());
  }

  /**
   * This method is used to create Cache in VM0
   */

  @SuppressWarnings("deprecation")
  private void createCacheForVM0() {
    try {

      distributedSystem = (new Bug37377DUnitTest()).getSystem(props);
      assertTrue(distributedSystem != null);
      cache = CacheFactory.create(distributedSystem);
      assertTrue(cache != null);
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      factory.setDiskSynchronous(false);
      factory.setDiskStoreName(
          cache.createDiskStoreFactory().setDiskDirs(dirs).create("Bug37377DUnitTest").getName());
      RegionAttributes attr = factory.create();
      cache.createRegion(regionName, attr);
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Error Creating cache / region ");
    }
  }

  /**
   * This method is used to create Cache in VM1
   */
  @SuppressWarnings("deprecation")
  private void createCacheForVM1() {
    try {
      distributedSystem = (new Bug37377DUnitTest()).getSystem(props);
      assertTrue(distributedSystem != null);
      cache = CacheFactory.create(distributedSystem);
      assertTrue("cache found null", cache != null);

      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      factory.setDiskSynchronous(false);
      factory.setDiskStoreName(
          cache.createDiskStoreFactory().setDiskDirs(dirs).create("Bug37377DUnitTest").getName());
      RegionAttributes attr = factory.create();
      DistributedRegion distRegion = new DistributedRegion(regionName, attr, null,
          (GemFireCacheImpl) cache, new InternalRegionArguments().setDestroyLockFlag(true)
              .setRecreateFlag(false).setSnapshotInputStream(null).setImageTarget(null));
      // assertTrue("Distributed Region is null", distRegion != null); (cannot be null)

      TestAbstractDiskRegionEntry.setMembers(vm1, vm0); // vm1 is thisVM, vm0 is otherVM

      ((AbstractRegionMap) distRegion.entries)
          .setEntryFactory(TestAbstractDiskRegionEntry.getEntryFactory());

      LocalRegion region = (LocalRegion) ((GemFireCacheImpl) cache).createVMRegion(regionName, attr,
          new InternalRegionArguments().setInternalMetaRegion(distRegion).setDestroyLockFlag(true)
              .setSnapshotInputStream(null).setImageTarget(null));
      assertTrue("Local Region is null", region != null);

    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Error Creating cache / region " + ex);
    }
  }

  /**
   * This method puts in maxEntries in the Region
   */
  private void putSomeEntries() {
    assertTrue("Cache is found as null ", cache != null);
    Region rgn = cache.getRegion(regionName);
    for (int i = 0; i < maxEntries; i++) {
      rgn.put(new Long(i), new Long(i));
    }
  }

  /**
   * This method clears the region and notifies the other member when complete
   * 
   * @throws InterruptedException
   */
  private static void invokeRemoteClearAndWait(VM remoteVM, VM thisVM) {
    remoteVM.invoke(() -> clearRegionAndNotify(thisVM));
    try {
      clearLatch.await();
    } catch (InterruptedException e) {
      fail("wait for remote clear to complete failed");
    }
  }

  /**
   * This method clears the region and notifies the other member when complete
   */
  private static void clearRegionAndNotify(VM otherVM) {
    assertTrue("Cache is found as null ", cache != null);
    Region rgn = cache.getRegion(regionName);
    rgn.clear();
    otherVM.invoke(() -> notifyClearComplete());
  }

  /**
   * Decrement countdown latch to notify clear complete
   */
  private static void notifyClearComplete() {
    clearLatch.countDown();
  }

  /**
   * This method destroys the Region
   */
  private void destroyRegion() {
    try {
      assertTrue("Cache is found as null ", cache != null);
      Region rgn = cache.getRegion(regionName);
      rgn.localDestroyRegion();
      cache.close();
    } catch (Exception ex) {
    }
  }

  /**
   * This method closes the cache on the specified VM
   */
  private void closeCacheForVM(final int vmNo) {
    if (vmNo == 0) {
      cache.getRegion(regionName).localDestroyRegion();
    }
    assertTrue("Cache is found as null ", cache != null);
    cache.close();
  }

  /**
   * This method verifies that the reintialized region size is zero
   */
  private void verifyExtraEntryFromOpLogs() {
    assertTrue("Cache is found as null ", cache != null);
    Region rgn = cache.getRegion(regionName);
    // should be zero after clear
    assertEquals(0, rgn.size());
  }

  /**
   * The Clear operation during a GII in progress can leave a Entry in the Oplog due to a race
   * condition wherein the clearFlag getting set after the entry gets written to the disk, The Test
   * verifies the existence of the scenario.
   * 
   */

  @Test
  public void testGIIputWithClear() {
    vm0.invoke(() -> createCacheForVM0());
    vm0.invoke(() -> putSomeEntries());

    vm1.invoke(() -> createCacheForVM1());

    vm0.invoke(() -> closeCacheForVM(0));
    vm1.invoke(() -> closeCacheForVM(1));

    vm1.invoke(() -> createCacheForVM1());
    vm1.invoke(() -> verifyExtraEntryFromOpLogs());
  }

  static class TestAbstractDiskRegionEntry extends VersionedThinDiskRegionEntryHeapObjectKey {
    static private VM thisVM, otherVM;

    static void setMembers(VM localVM, VM remoteVM) {
      thisVM = localVM;
      otherVM = remoteVM;
    }

    protected TestAbstractDiskRegionEntry(RegionEntryContext r, Object key, Object value) {
      super(r, key, value);
    }

    private static RegionEntryFactory factory = new RegionEntryFactory() {

      public RegionEntry createEntry(RegionEntryContext r, Object key, Object value) {
        return new TestAbstractDiskRegionEntry(r, key, value);
      }

      public Class getEntryClass() {
        return TestAbstractDiskRegionEntry.class;
      }

      public RegionEntryFactory makeVersioned() {
        return this;
      }

      public RegionEntryFactory makeOnHeap() {
        return this;
      }
    };

    /**
     * Overridden setValue method to call clear Region before actually writing the entry
     */
    @Override
    public boolean initialImageInit(final LocalRegion r, final long lastModifiedTime,
        final Object newValue, final boolean create, final boolean wasRecovered,
        final boolean versionTagAccepted) throws RegionClearedException {
      synchronized (clearOccurred) {
        if (!clearOccurred) {
          // Force other member to perform a clear during our GII
          invokeRemoteClearAndWait(otherVM, thisVM);
          clearOccurred = true;
        }
      }

      // Continue GII processing, which should throw RegionClearedException after the clear
      try {
        boolean result = super.initialImageInit(r, lastModifiedTime, newValue, create, wasRecovered,
            versionTagAccepted);
      } catch (RegionClearedException rce) {
        throw rce;
      } catch (Exception ex) {
        fail("Caught exception during initialImageInit: " + ex);
      }

      return true;
    }

    public static RegionEntryFactory getEntryFactory() {
      return factory;
    }
  }
}

