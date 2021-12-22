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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;

/**
 * Disk LRU region recovery tests.
 */
public class DiskLruRegRecoveryJUnitTest extends DiskRegionTestingBase {

  @Override
  protected final void postTearDown() throws Exception {
    DiskStoreObserver.setInstance(null);
  }

  private int getValuesInVM(Region region, int size) {
    int valuesInVm = 0;
    for (int i = 0; i < size; i++) {
      try {
        Object value = ((LocalRegion) region).getValueInVM(i);
        if (value != null) {
          valuesInVm++;
        }
      } catch (EntryNotFoundException e) {
        fail("Entry not found not expected but occurred ");
      }
    }
    return valuesInVm;
  }

  private Region createNonLruRegion() {
    DiskRegionProperties nonLruDiskProps = new DiskRegionProperties();
    nonLruDiskProps.setDiskDirs(dirs);
    nonLruDiskProps.setPersistBackup(true);
    nonLruDiskProps.setRegionName("RecoveryTestNonLruRegion");
    return DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, nonLruDiskProps, Scope.LOCAL);
  }

  private void createRegionAndIntiateRecovery(boolean lruEntryEviction,
      boolean recoveryByCacheClose, boolean addNonLruRegion, int load, int expectedValues) {

    DiskRegionProperties diskProps = new DiskRegionProperties();
    Region nonLruRegion = null;
    int numRegions = 1;

    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("RecoveryTestRegion");

    if (lruEntryEviction) {
      int overflowCapacity = 5;
      diskProps.setOverFlowCapacity(overflowCapacity);
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    } else {
      region = DiskRegionHelperFactory.getSyncHeapLruAndPersistRegion(cache, diskProps);
    }

    if (addNonLruRegion) {
      numRegions++;
      nonLruRegion = createNonLruRegion();
    }

    for (int i = 0; i < load; i++) {
      region.put(i, i);
      if (nonLruRegion != null) {
        nonLruRegion.put(i, i);
      }
    }

    if (recoveryByCacheClose) {
      cache.close();
      cache = createCache();
    } else {
      region.close();
      if (nonLruRegion != null) {
        nonLruRegion.close();
      }
    }

    // Regions are created with its own disk store.
    final CountDownLatch recoveryDone = new CountDownLatch(numRegions);
    DiskStoreObserver.setInstance(new DiskStoreObserver() {
      @Override
      public void afterAsyncValueRecovery(DiskStoreImpl store) {
        recoveryDone.countDown();
      }
    });

    if (lruEntryEviction) {
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    } else {
      region = DiskRegionHelperFactory.getSyncHeapLruAndPersistRegion(cache, diskProps);
    }

    if (addNonLruRegion) {
      nonLruRegion = createNonLruRegion();
    }

    // Wait for recovery to finish.
    try {
      recoveryDone.await();
    } catch (InterruptedException ie) {
      fail("Found interrupted exception while waiting for recovery.");
    }

    int valuesInVm = getValuesInVM(region, load);
    assertEquals("Values for lru regions should not be recovered from Disk.", expectedValues,
        valuesInVm);

    if (nonLruRegion != null) {
      valuesInVm = getValuesInVM(nonLruRegion, load);
      // The values should be recovered for non LRU regions.
      assertEquals("Values for non lru regions are not recovered from Disk.", load, valuesInVm);
    }
  }

  @Test
  public void testValuesAreNotRecoveredForLruRegionWithCacheClose() {
    createRegionAndIntiateRecovery(true, true, false, 10, 0);
  }

  @Test
  public void testValuesAreNotRecoveredForLruRegionWithRegionClose() {
    createRegionAndIntiateRecovery(true, false, false, 10, 0);
  }

  @Test
  public void testValuesAreNotRecoveredForLruAndRecoveredForNonLru() {
    createRegionAndIntiateRecovery(true, true, true, 10, 0);
  }

  @Test
  public void testValuesAreNotRecoveredForLruAndRecoveredForNonLruWithRegionClose() {
    createRegionAndIntiateRecovery(true, false, true, 10, 0);
  }

  @Test
  public void testValuesAreNotRecoveredForHeapLruRegion() {
    createRegionAndIntiateRecovery(false, true, true, 10, 0);
  }

  @Test
  public void testValuesAreNotRecoveredForHeapLruRegionWithRegionClose() {
    createRegionAndIntiateRecovery(false, false, true, 10, 0);
  }

  @Test
  public void testValuesAreNotRecoveredForLruRegionWithReocoverValuePropertySet() {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");

    try {
      createRegionAndIntiateRecovery(true, true, true, 10, 0);
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testValuesAreNotRecoveredForLruRegionWithReocoverValuePropertySetWithRegionClose() {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");

    try {
      createRegionAndIntiateRecovery(true, false, true, 10, 0);
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testValuesAreRecoveredForLruRegionWithReocoverValueAndRecoverLruPropertySet() {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");

    String lruOldValue = System.getProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME, "true");

    try {
      createRegionAndIntiateRecovery(false, true, true, 10, 10);
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }

      if (lruOldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME, lruOldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testValuesAreRecoveredForLruRegionWithReocoverValueAndRecoverLruPropertySetWithRegionClose() {
    String oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");

    String lruOldValue = System.getProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME, "true");

    try {
      createRegionAndIntiateRecovery(false, false, true, 10, 10);
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }

      if (lruOldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME, lruOldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testBasicVerifyStats() {
    DiskRegionProperties diskProps = new DiskRegionProperties();
    final Integer key = 1;
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("basicVerifyStats");
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    region.put(key, 1);
    region.put(key, 2);
    region.close();

    final CountDownLatch recoveryDone = new CountDownLatch(1);
    DiskStoreObserver.setInstance(new DiskStoreObserver() {
      @Override
      public void afterAsyncValueRecovery(DiskStoreImpl store) {
        recoveryDone.countDown();
      }
    });

    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    // Wait for recovery to finish.
    try {
      recoveryDone.await();
    } catch (InterruptedException ie) {
      fail("Found interrupted exception while waiting for recovery.");
    }

    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    assertEquals(0, dr.getNumEntriesInVM());
    assertEquals(1, dr.getNumOverflowOnDisk());

    region.get(key);
    assertEquals(1, dr.getNumEntriesInVM());
    assertEquals(0, dr.getNumOverflowOnDisk());

    region.clear();
    assertEquals(0, dr.getNumEntriesInVM());
    assertEquals(0, dr.getNumOverflowOnDisk());
  }

}
