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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
import org.apache.geode.test.junit.categories.DistributedTest;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({DistributedTest.class})
public abstract class BackupPrepareAndFinishMsgDUnitTest extends CacheTestCase {
  // Although this test does not make use of other members, the current member needs to be
  // a distributed member (rather than local) because it sends prepare and finish backup messages
  private static final String TEST_REGION_NAME = "TestRegion";
  private File[] diskDirs = null;
  private int waitingForBackupLockCount = 0;
  private Region<Integer, Integer> region;

  protected abstract Region<Integer, Integer> createRegion();

  @Before
  public void setup() {
    region = createRegion();
  }

  @Test
  public void createWaitsForBackupTest() throws Throwable {
    doActionAndVerifyWaitForBackup(() -> region.create(1, 1));
    verifyKeyValuePair(1, 1);
  }

  @Test
  public void putThatCreatesWaitsForBackupTest() throws Throwable {
    doActionAndVerifyWaitForBackup(() -> region.put(1, 1));
    verifyKeyValuePair(1, 1);
  }

  @Test
  public void putWaitsForBackupTest() throws Throwable {
    region.put(1, 1);
    doActionAndVerifyWaitForBackup(() -> region.put(1, 2));
    verifyKeyValuePair(1, 2);
  }

  @Test
  public void invalidateWaitsForBackupTest() throws Throwable {
    region.put(1, 1);
    doActionAndVerifyWaitForBackup(() -> region.invalidate(1));
    verifyKeyValuePair(1, null);
  }

  @Test
  public void destroyWaitsForBackupTest() throws Throwable {
    region.put(1, 1);
    doActionAndVerifyWaitForBackup(() -> region.destroy(1));
    assertFalse(region.containsKey(1));
  }

  @Test
  public void putAllWaitsForBackupTest() throws Throwable {
    Map<Integer, Integer> entries = new HashMap<>();
    entries.put(1, 1);
    entries.put(2, 2);

    doActionAndVerifyWaitForBackup(() -> region.putAll(entries));
    verifyKeyValuePair(1, 1);
    verifyKeyValuePair(2, 2);
  }

  @Test
  public void removeAllWaitsForBackupTest() throws Throwable {
    region.put(1, 1);
    region.put(2, 2);

    List<Integer> keys = Arrays.asList(1, 2);
    doActionAndVerifyWaitForBackup(() -> region.removeAll(keys));
    assertTrue(region.isEmpty());
  }

  @Test
  public void readActionsDoNotBlockDuringBackup() {
    region.put(1, 1);
    doReadActionsAndVerifyCompletion();
  }

  private void doActionAndVerifyWaitForBackup(Runnable function)
      throws InterruptedException, TimeoutException, ExecutionException {
    DM dm = GemFireCacheImpl.getInstance().getDistributionManager();
    Set recipients = dm.getOtherDistributionManagerIds();
    Future<Void> future = null;
    PrepareBackupRequest.send(dm, recipients);
    waitingForBackupLockCount = 0;
    future = CompletableFuture.runAsync(function);
    Awaitility.await().atMost(5, TimeUnit.SECONDS)
        .until(() -> assertTrue(waitingForBackupLockCount == 1));
    FinishBackupRequest.send(dm, recipients, diskDirs[0], null, false);
    future.get(5, TimeUnit.SECONDS);
  }

  private void doReadActionsAndVerifyCompletion() {
    DM dm = GemFireCacheImpl.getInstance().getDistributionManager();
    Set recipients = dm.getOtherDistributionManagerIds();
    PrepareBackupRequest.send(dm, recipients);
    waitingForBackupLockCount = 0;
    List<CompletableFuture<?>> futureList = doReadActions();
    CompletableFuture.allOf(futureList.toArray(new CompletableFuture<?>[futureList.size()]));
    assertTrue(waitingForBackupLockCount == 0);
    FinishBackupRequest.send(dm, recipients, diskDirs[0], null, false);
  }

  private void verifyKeyValuePair(Integer key, Integer expectedValue) {
    assertTrue(region.containsKey(key));
    assertEquals(expectedValue, region.get(key));
  }

  private List<CompletableFuture<?>> doReadActions() {
    List<Runnable> actions = new ArrayList<>();
    actions.add(() -> region.get(1));
    actions.add(() -> region.containsKey(1));
    actions.add(() -> region.containsValue(1));
    actions.add(region::entrySet);
    actions.add(this::valueExistsCheck);
    actions.add(() -> region.getAll(Collections.emptyList()));
    actions.add(() -> region.getEntry(1));
    actions.add(region::isEmpty);
    actions.add(region::keySet);
    actions.add(region::size);
    actions.add(region::values);
    actions.add(this::queryCheck);
    return actions.stream().map(runnable -> CompletableFuture.runAsync(runnable))
        .collect(Collectors.toList());
  }

  private void valueExistsCheck() {
    try {
      region.existsValue("value = 1");
    } catch (FunctionDomainException | TypeMismatchException | NameResolutionException
        | QueryInvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private void queryCheck() {
    try {
      region.query("select * from /" + TEST_REGION_NAME);
    } catch (FunctionDomainException | TypeMismatchException | NameResolutionException
        | QueryInvocationTargetException e) {
      throw new RuntimeException(e);
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
  protected Region<Integer, Integer> createRegion(RegionShortcut shortcut) {
    Cache cache = getCache();
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskDirs = getDiskDirs();
    diskStoreFactory.setDiskDirs(diskDirs);
    DiskStore diskStore = diskStoreFactory.create(getUniqueName());
    ((DiskStoreImpl) diskStore).getBackupLock().setBackupLockTestHook(new BackupLockHook());

    RegionFactory<Integer, Integer> regionFactory = cache.createRegionFactory(shortcut);
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDiskSynchronous(true);
    if (shortcut.equals(RegionShortcut.PARTITION_PERSISTENT)) {
      PartitionAttributesFactory prFactory = new PartitionAttributesFactory();
      prFactory.setTotalNumBuckets(1);
      regionFactory.setPartitionAttributes(prFactory.create());
    }
    return regionFactory.create(TEST_REGION_NAME);
  }

}
