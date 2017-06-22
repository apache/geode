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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.geode.admin.internal.FinishBackupRequest;
import org.apache.geode.admin.internal.PrepareBackupRequest;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.internal.cache.BackupLock;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({DistributedTest.class})
public class BackupPrepareAndFinishMsgDUnitTest extends CacheTestCase {
  // Although this test does not make use of other members, the current member needs to be
  // a distributed member (rather than local) because it sends prepare and finish backup messages
  File[] diskDirs = null;
  private int waitingForBackupLockCount = 0;

  @After
  public void after() throws Exception {
    waitingForBackupLockCount = 0;
    diskDirs = null;
  }

  @Test
  public void testCreateWithParReg() throws Throwable {
    doCreate(RegionShortcut.PARTITION_PERSISTENT, true);
  }

  @Test
  public void testCreateWithReplicate() throws Throwable {
    doCreate(RegionShortcut.REPLICATE_PERSISTENT, true);
  }

  @Test
  public void testPutAsCreateWithParReg() throws Throwable {
    doCreate(RegionShortcut.PARTITION_PERSISTENT, false);
  }

  @Test
  public void testPutAsCreateWithReplicate() throws Throwable {
    doCreate(RegionShortcut.REPLICATE_PERSISTENT, false);
  }

  @Test
  public void testUpdateWithParReg() throws Throwable {
    doUpdate(RegionShortcut.PARTITION_PERSISTENT);
  }

  @Test
  public void testUpdateWithReplicate() throws Throwable {
    doUpdate(RegionShortcut.REPLICATE_PERSISTENT);
  }

  @Test
  public void testInvalidateWithParReg() throws Throwable {
    doInvalidate(RegionShortcut.PARTITION_PERSISTENT);
  }

  @Test
  public void testInvalidateWithReplicate() throws Throwable {
    doInvalidate(RegionShortcut.REPLICATE_PERSISTENT);
  }

  @Test
  public void testDestroyWithParReg() throws Throwable {
    doDestroy(RegionShortcut.PARTITION_PERSISTENT);
  }

  @Test
  public void testDestroyWithReplicate() throws Throwable {
    doDestroy(RegionShortcut.REPLICATE_PERSISTENT);
  }

  @Test
  public void testGetWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "get");
  }

  @Test
  public void testGetWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "get");
  }

  @Test
  public void testContainsKeyWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "containsKey");
  }

  @Test
  public void testContainsKeyWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "containsKey");
  }

  @Test
  public void testContainsValueWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "containsValue");
  }

  @Test
  public void testContainsValueWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "containsValue");
  }

  @Test
  public void testContainsValueForKeyWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "containsValueForKey");
  }

  @Test
  public void testContainsValueForKeyWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "containsValueForKey");
  }

  @Test
  public void testEntrySetWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "entrySet");
  }

  @Test
  public void testEntrySetWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "entrySet");
  }

  @Test
  public void testGetAllWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "getAll");
  }

  @Test
  public void testGetAllWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "getAll");
  }

  @Test
  public void testGetEntryWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "getEntry");
  }

  @Test
  public void testGetEntryWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "getEntry");
  }

  @Test
  public void testIsEmptyWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "isEmpty");
  }

  @Test
  public void testIsEmptyWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "isEmpty");
  }

  @Test
  public void testKeySetWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "keySet");
  }

  @Test
  public void testKeySetWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "keySet");
  }

  @Test
  public void testSizeWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "size");
  }

  @Test
  public void testSizeWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "size");
  }

  @Test
  public void testValuesWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "values");
  }

  @Test
  public void testValuesWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "values");
  }

  @Test
  public void testQueryWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "query");
  }

  @Test
  public void testQueryWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "query");
  }

  @Test
  public void testExistsValueWithParReg() throws Throwable {
    doRead(RegionShortcut.PARTITION_PERSISTENT, "existsValue");
  }

  @Test
  public void testExistsValueWithReplicate() throws Throwable {
    doRead(RegionShortcut.REPLICATE_PERSISTENT, "existsValue");
  }

  @Test
  public void testPutAllWithParReg() throws Throwable {
    doPutAll(RegionShortcut.PARTITION_PERSISTENT);
  }

  @Test
  public void testPutAllWithReplicate() throws Throwable {
    doPutAll(RegionShortcut.REPLICATE_PERSISTENT);
  }

  @Test
  public void testRemoveAllWithParReg() throws Throwable {
    doRemoveAll(RegionShortcut.PARTITION_PERSISTENT);
  }

  @Test
  public void testRemoveAllWithReplicate() throws Throwable {
    doRemoveAll(RegionShortcut.REPLICATE_PERSISTENT);
  }

  /**
   * Test that a create waits for backup
   * 
   * @param shortcut The region shortcut to use to create the region
   * @throws InterruptedException
   */
  private void doCreate(RegionShortcut shortcut, boolean useCreate) throws InterruptedException {
    Region aRegion = createRegion(shortcut);
    Runnable runnable = new Runnable() {
      public void run() {
        if (useCreate) {
          aRegion.create(1, 1);
        } else {
          aRegion.put(1, 1);
        }
      }
    };

    verifyWaitForBackup(runnable);
    assertTrue(aRegion.containsKey(1));
    assertEquals(aRegion.get(1), 1);
  }

  /**
   * Test that an update waits for backup
   * 
   * @param shortcut The region shortcut to use to create the region
   * @throws InterruptedException
   */
  private void doUpdate(RegionShortcut shortcut) throws InterruptedException {
    Region aRegion = createRegion(shortcut);
    aRegion.put(1, 1);

    Runnable runnable = new Runnable() {
      public void run() {
        aRegion.put(1, 2);
      }
    };

    verifyWaitForBackup(runnable);
    assertTrue(aRegion.containsKey(1));
    assertEquals(aRegion.get(1), 2);
  }

  /**
   * Test that an invalidate waits for backup
   * 
   * @param shortcut The region shortcut to use to create the region
   * @throws InterruptedException
   */
  private void doInvalidate(RegionShortcut shortcut) throws InterruptedException {
    Region aRegion = createRegion(shortcut);
    aRegion.put(1, 1);

    Runnable runnable = (new Runnable() {
      public void run() {
        aRegion.invalidate(1);
      }
    });

    verifyWaitForBackup(runnable);
    assertTrue(aRegion.containsKey(1));
    assertNull(aRegion.get(1));
  }

  /**
   * Test that a destroy waits for backup
   * 
   * @param shortcut The region shortcut to use to create the region
   * @throws InterruptedException
   */
  private void doDestroy(RegionShortcut shortcut) throws InterruptedException {
    Region aRegion = createRegion(shortcut);
    aRegion.put(1, 1);

    Runnable runnable = new Runnable() {
      public void run() {
        aRegion.destroy(1);
      }
    };

    verifyWaitForBackup(runnable);
    assertFalse(aRegion.containsKey(1));
  }

  /**
   * Test that a read op does NOT wait for backup
   * 
   * @param shortcut The region shortcut to use to create the region
   * @throws InterruptedException
   */
  private void doRead(RegionShortcut shortcut, String op) throws Exception {
    Region aRegion = createRegion(shortcut);
    aRegion.put(1, 1);

    Runnable runnable = new Runnable() {
      public void run() {
        switch (op) {
          case "get": {
            aRegion.get(1);
            break;
          }
          case "containsKey": {
            aRegion.containsKey(1);
            break;
          }
          case "containsValue": {
            aRegion.containsValue(1);
            break;
          }
          case "containsValueForKey": {
            aRegion.containsValue(1);
            break;
          }
          case "entrySet": {
            aRegion.entrySet();
            break;
          }
          case "existsValue": {
            try {
              aRegion.existsValue("value = 1");
            } catch (FunctionDomainException | TypeMismatchException | NameResolutionException
                | QueryInvocationTargetException e) {
              fail(e.toString());
            }
            break;
          }
          case "getAll": {
            aRegion.getAll(new ArrayList());
            break;
          }
          case "getEntry": {
            aRegion.getEntry(1);
            break;
          }
          case "isEmpty": {
            aRegion.isEmpty();
            break;
          }
          case "keySet": {
            aRegion.keySet();
            break;
          }
          case "query": {
            try {
              aRegion.query("select *");
            } catch (FunctionDomainException | TypeMismatchException | NameResolutionException
                | QueryInvocationTargetException e) {
              fail(e.toString());
            }
            break;
          }
          case "size": {
            aRegion.size();
            break;
          }
          case "values": {
            aRegion.values();
            break;
          }
          default: {
            fail("Unknown operation " + op);
          }
        }
      }
    };

    verifyNoWaitForBackup(runnable);
  }

  /**
   * Test that a putAll waits for backup
   * 
   * @param shortcut The region shortcut to use to create the region
   * @throws InterruptedException
   */
  private void doPutAll(RegionShortcut shortcut) throws InterruptedException {
    Region aRegion = createRegion(shortcut);
    Runnable runnable = new Runnable() {
      public void run() {
        Map<Object, Object> putAllMap = new HashMap<Object, Object>();
        putAllMap.put(1, 1);
        putAllMap.put(2, 2);
        aRegion.putAll(putAllMap);
      }
    };

    verifyWaitForBackup(runnable);
    assertTrue(aRegion.containsKey(1));
    assertEquals(aRegion.get(1), 1);
    assertTrue(aRegion.containsKey(2));
    assertEquals(aRegion.get(2), 2);
  }

  /**
   * Test that a removeAll waits for backup
   * 
   * @param shortcut The region shortcut to use to create the region
   * @throws InterruptedException
   */
  private void doRemoveAll(RegionShortcut shortcut) throws InterruptedException {
    Region aRegion = createRegion(shortcut);
    aRegion.put(1, 2);
    aRegion.put(2, 3);

    Runnable runnable = new Runnable() {
      public void run() {
        List<Object> keys = new ArrayList();
        keys.add(1);
        keys.add(2);
        aRegion.removeAll(keys);
      }
    };

    verifyWaitForBackup(runnable);
    assertEquals(aRegion.size(), 0);
  }

  /**
   * Test that executing the given runnable waits for backup completion to proceed
   * 
   * @param runnable The code that should wait for backup.
   * @throws InterruptedException
   */
  private void verifyWaitForBackup(Runnable runnable) throws InterruptedException {
    DM dm = ((InternalCache) GemFireCacheImpl.getInstance()).getDistributionManager();
    Set recipients = dm.getOtherDistributionManagerIds();
    boolean abort = true;
    Thread aThread = new Thread(runnable);
    try {
      PrepareBackupRequest.send(dm, recipients);
      abort = false;
      waitingForBackupLockCount = 0;
      aThread.start();
      Awaitility.await().atMost(30, TimeUnit.SECONDS)
          .until(() -> assertTrue(waitingForBackupLockCount == 1));
    } finally {
      FinishBackupRequest.send(dm, recipients, diskDirs[0], null, abort);
      aThread.join(30000);
      assertFalse(aThread.isAlive());
    }
  }

  /**
   * Test that executing the given runnable does NOT wait for backup completion to proceed
   * 
   * @param runnable The code that should not wait for backup.
   * @throws InterruptedException
   */
  private void verifyNoWaitForBackup(Runnable runnable) throws InterruptedException {
    DM dm = ((InternalCache) GemFireCacheImpl.getInstance()).getDistributionManager();
    Set recipients = dm.getOtherDistributionManagerIds();
    boolean abort = true;
    Thread aThread = new Thread(runnable);
    try {
      PrepareBackupRequest.send(dm, recipients);
      abort = false;
      waitingForBackupLockCount = 0;
      aThread.start();
      aThread.join(30000);
      assertFalse(aThread.isAlive());
      assertTrue(waitingForBackupLockCount == 0);
    } finally {
      FinishBackupRequest.send(dm, recipients, diskDirs[0], null, abort);
    }
  }

  /**
   * Implementation of test hook
   */
  private class BackupLockHook implements BackupLock.BackupLockTestHook {
    @Override
    public void beforeWaitForBackupCompletion() {
      waitingForBackupLockCount++;
    }
  }

  /**
   * Create a region, installing the test hook in the backup lock
   * 
   * @param shortcut The region shortcut to use to create the region
   * @return The newly created region.
   */
  private Region<?, ?> createRegion(RegionShortcut shortcut) {
    Cache cache = getCache();
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskDirs = getDiskDirs();
    diskStoreFactory.setDiskDirs(diskDirs);
    DiskStore diskStore = diskStoreFactory.create(getUniqueName());
    ((DiskStoreImpl) diskStore).getBackupLock().setBackupLockTestHook(new BackupLockHook());

    RegionFactory<String, String> regionFactory = cache.createRegionFactory(shortcut);
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDiskSynchronous(true);
    if (shortcut.equals(RegionShortcut.PARTITION_PERSISTENT)) {
      PartitionAttributesFactory prFactory = new PartitionAttributesFactory();
      prFactory.setTotalNumBuckets(1);
      regionFactory.setPartitionAttributes(prFactory.create());
    }
    return regionFactory.create("TestRegion");
  }

}
