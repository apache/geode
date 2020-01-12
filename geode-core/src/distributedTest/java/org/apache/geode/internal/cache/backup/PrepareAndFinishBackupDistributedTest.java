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
package org.apache.geode.internal.cache.backup;

import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getController;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

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
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@SuppressWarnings("serial,unused")
public class PrepareAndFinishBackupDistributedTest {

  private String uniqueName;
  private String regionName;
  private Region<Integer, Integer> region;

  @Parameter
  public RegionShortcut regionShortcut;

  @Parameters
  public static Collection<RegionShortcut> data() {
    return Arrays.asList(PARTITION_PERSISTENT, REPLICATE_PERSISTENT);
  }

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public DistributedDiskDirRule diskDirRule = new DistributedDiskDirRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";

    region = createRegion(regionShortcut);
  }

  @Test
  public void createWaitsForBackupTest() throws Exception {
    doActionAndVerifyWaitForBackup(() -> region.create(1, 1));
    verifyKeyValuePair(1, 1);
  }

  @Test
  public void putThatCreatesWaitsForBackupTest() throws Exception {
    doActionAndVerifyWaitForBackup(() -> region.put(1, 1));
    verifyKeyValuePair(1, 1);
  }

  @Test
  public void putWaitsForBackupTest() throws Exception {
    region.put(1, 1);
    doActionAndVerifyWaitForBackup(() -> region.put(1, 2));
    verifyKeyValuePair(1, 2);
  }

  @Test
  public void invalidateWaitsForBackupTest() throws Exception {
    region.put(1, 1);
    doActionAndVerifyWaitForBackup(() -> region.invalidate(1));
    verifyKeyValuePair(1, null);
  }

  @Test
  public void destroyWaitsForBackupTest() throws Exception {
    region.put(1, 1);
    doActionAndVerifyWaitForBackup(() -> region.destroy(1));
    assertThat(region).doesNotContainKey(1);
  }

  @Test
  public void putAllWaitsForBackupTest() throws Exception {
    Map<Integer, Integer> entries = new HashMap<>();
    entries.put(1, 1);
    entries.put(2, 2);

    doActionAndVerifyWaitForBackup(() -> region.putAll(entries));
    verifyKeyValuePair(1, 1);
    verifyKeyValuePair(2, 2);
  }

  @Test
  public void removeAllWaitsForBackupTest() throws Exception {
    region.put(1, 1);
    region.put(2, 2);

    List<Integer> keys = Arrays.asList(1, 2);
    doActionAndVerifyWaitForBackup(() -> region.removeAll(keys));
    assertThat(region).isEmpty();
  }

  @Test
  public void readActionsDoNotBlockDuringBackup() {
    region.put(1, 1);
    doReadActionsAndVerifyCompletion();
  }

  /**
   * Create a region, installing the test hook in the backup lock
   *
   * @param shortcut The region shortcut to use to create the region
   * @return The newly created region.
   */
  private Region<Integer, Integer> createRegion(RegionShortcut shortcut) {
    Cache cache = cacheRule.getOrCreateCache();

    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] {getDiskDir()});

    DiskStore diskStore = diskStoreFactory.create(getUniqueName());

    RegionFactory<Integer, Integer> regionFactory = cache.createRegionFactory(shortcut);
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDiskSynchronous(true);

    if (shortcut.equals(PARTITION_PERSISTENT)) {
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setTotalNumBuckets(1);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
    }

    return regionFactory.create(regionName);
  }

  private void doActionAndVerifyWaitForBackup(Runnable function)
      throws InterruptedException, TimeoutException, ExecutionException {
    DistributionManager dm = cacheRule.getCache().getDistributionManager();
    Set recipients = dm.getOtherDistributionManagerIds();

    Properties backupProperties = new BackupConfigFactory()
        .withTargetDirPath(getDiskDir().toString()).createBackupProperties();

    new PrepareBackupStep(dm, dm.getId(), dm.getCache(), recipients,
        new PrepareBackupFactory(), backupProperties).send();

    ReentrantLock backupLock = ((LocalRegion) region).getDiskStore().getBackupLock();
    Future<Void> future = CompletableFuture.runAsync(function);
    await()
        .untilAsserted(() -> assertThat(backupLock.getQueueLength()).isGreaterThanOrEqualTo(0));

    new FinishBackupStep(dm, dm.getId(), dm.getCache(), recipients, new FinishBackupFactory())
        .send();

    future.get(5, TimeUnit.SECONDS);
  }

  private void doReadActionsAndVerifyCompletion() {
    DistributionManager dm = cacheRule.getCache().getDistributionManager();
    Set recipients = dm.getOtherDistributionManagerIds();

    Properties backupProperties = new BackupConfigFactory()
        .withTargetDirPath(getDiskDir().toString()).createBackupProperties();

    new PrepareBackupStep(dm, dm.getId(), dm.getCache(), recipients,
        new PrepareBackupFactory(), backupProperties).send();

    ReentrantLock backupLock = ((LocalRegion) region).getDiskStore().getBackupLock();
    List<CompletableFuture<?>> futureList = doReadActions();
    CompletableFuture.allOf(futureList.toArray(new CompletableFuture<?>[futureList.size()]));
    assertThat(backupLock.getQueueLength()).isEqualTo(0);

    new FinishBackupStep(dm, dm.getId(), dm.getCache(), recipients, new FinishBackupFactory())
        .send();
  }

  private void verifyKeyValuePair(Integer key, Integer expectedValue) {
    assertThat(region).containsKey(key);
    assertThat(region.get(key)).isEqualTo(expectedValue);
  }

  private List<CompletableFuture<?>> doReadActions() {
    List<Runnable> actions = new ArrayList<>();
    actions.add(() -> region.get(1));
    actions.add(() -> region.containsKey(1));
    actions.add(() -> region.containsValue(1));
    actions.add(() -> region.entrySet());
    actions.add(() -> valueExistsCheck());
    actions.add(() -> region.getAll(Collections.emptyList()));
    actions.add(() -> region.getEntry(1));
    actions.add(() -> region.isEmpty());
    actions.add(() -> region.keySet());
    actions.add(() -> region.size());
    actions.add(() -> region.values());
    actions.add(() -> queryCheck());
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
      region.query("select * from /" + regionName);
    } catch (FunctionDomainException | TypeMismatchException | NameResolutionException
        | QueryInvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private String getUniqueName() {
    return uniqueName;
  }

  private File getDiskDir() {
    return diskDirRule.getDiskDirFor(getController());
  }
}
