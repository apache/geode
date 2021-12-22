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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.apache.commons.io.filefilter.DirectoryFileFilter.DIRECTORY;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.DEPLOY_WORKING_DIR;
import static org.apache.geode.internal.util.TransformUtils.getFileNameTransformer;
import static org.apache.geode.internal.util.TransformUtils.transform;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.dunit.internal.DUnitLauncher.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileFilter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Distributed tests for incremental backup.
 */
@SuppressWarnings("serial")
public class IncrementalBackupDistributedTest implements Serializable {

  private static final int DATA_INCREMENT = 10_000;
  private static final IOFileFilter OPLOG_FILTER = new RegexFileFilter(".*\\.[kdc]rf$");

  private static volatile BackupMembershipListener backupMembershipListener;

  private int dataStart;
  private int dataEnd = dataStart + DATA_INCREMENT;

  private String diskStoreName1;
  private String diskStoreName2;
  private String regionName1;
  private String regionName2;

  private File baselineDir;
  private File incrementalDir1;

  private File userDirInVM0;
  private File userDirInVM1;

  private VM vm0;
  private VM vm1;

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
  public void setUp() throws Exception {
    vm0 = getVM(0);
    vm1 = getVM(1);

    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();

    diskStoreName1 = uniqueName + "_diskStore-1";
    diskStoreName2 = uniqueName + "_diskStore-2";
    regionName1 = uniqueName + "_region-1";
    regionName2 = uniqueName + "_region-2";

    baselineDir = temporaryFolder.newFolder("baseline");
    incrementalDir1 = temporaryFolder.newFolder("incremental");

    userDirInVM0 = temporaryFolder.newFolder("vm0");
    userDirInVM1 = temporaryFolder.newFolder("vm1");
    File userDirInController = temporaryFolder.newFolder("controller");

    vm0.invoke(() -> createCache(diskDirRule.getDiskDirFor(vm0), userDirInVM0));
    vm1.invoke(() -> createCache(diskDirRule.getDiskDirFor(vm1), userDirInVM1));

    createCache(diskDirRule.getDiskDirFor(getController()), userDirInController);
    performPuts();
  }

  @After
  public void tearDown() throws Exception {
    for (VM vm : toArray(vm0, vm1, getController())) {
      vm.invoke(() -> {
        backupMembershipListener = null;
      });
    }
  }

  /**
   * Successful if a member performs a full backup when its backup data is not present in the
   * performBackupBaseline (for whatever reason). This also tests what happens when a member is
   * offline during the performBackupBaseline backup.
   *
   * <p>
   * The test is regarded as successful when all of the missing members oplog files are backed up
   * during an performBackupIncremental backup. This means that the member performed a full backup
   * because its oplogs were missing in the performBackupBaseline.
   */
  @Test
  public void testMissingMemberInBaseline() {
    vm1.invoke(this::installNewBackupMembershipListener);

    // Simulate the missing member by forcing a persistent member to go offline.
    PersistentID missingMember = vm0.invoke(() -> getPersistentID(diskStoreName1));

    vm0.invoke(() -> getCache().close());

    vm1.invoke(() -> {
      await().atMost(30, SECONDS).untilAsserted(() -> {
        assertThat(getMissingPersistentMembers()).contains(missingMember);
        assertThat(backupMembershipListener.hasMemberDeparted()).isTrue();
      });
    });

    // Perform performBackupBaseline and make sure that list of offline disk stores contains our
    // missing member.
    vm1.invoke(() -> {
      BackupStatus baselineStatus = performBackup(getBaselineDir().getAbsolutePath());
      validateBackupStatus(baselineStatus);
      assertThat(baselineStatus.getOfflineDiskStores()).hasSize(2);
    });

    // Find all of the member's oplogs in the missing member's diskstore directory structure
    // (*.crf,*.krf,*.drf)
    Collection<File> missingMemberOplogFiles =
        listFiles(new File(missingMember.getDirectory()), OPLOG_FILTER, DIRECTORY);
    assertThat(missingMemberOplogFiles).isNotEmpty();

    // Restart our missing member and make sure it is back online and part of the cluster
    vm0.invoke(() -> createCache(diskDirRule.getDiskDirFor(vm0), userDirInVM0));

    // After reconnecting make sure the other members agree that the missing member is back online.
    await().atMost(30, SECONDS).untilAsserted(() -> {
      assertThat(getMissingPersistentMembers()).doesNotContain(missingMember);
    });

    // Perform performBackupIncremental and make sure we have no offline disk stores.
    vm1.invoke(() -> {
      BackupStatus incrementalStatus =
          performBackup(getIncrementalDir().getAbsolutePath(), getBaselineBackupPath());
      validateBackupStatus(incrementalStatus);
      assertThat(incrementalStatus.getOfflineDiskStores()).isNotNull().isEmpty();
    });

    // Get the missing member's member id which is different from the PersistentID
    String memberId = vm0.invoke(this::getModifiedMemberId);

    // Get list of backed up oplog files in the performBackupIncremental backup for the missing
    // member
    File incrementalMemberDir = getBackupDirForMember(getIncrementalDir(), memberId);
    Collection<File> backupOplogFiles = listFiles(incrementalMemberDir, OPLOG_FILTER, DIRECTORY);
    assertThat(backupOplogFiles).isNotEmpty();

    // Transform missing member oplogs to just their file names.
    Collection<String> missingMemberOplogNames = new ArrayList<>();
    transform(missingMemberOplogFiles, missingMemberOplogNames, getFileNameTransformer());

    // Transform missing member's performBackupIncremental backup oplogs to just their file names.
    List<String> backupOplogNames = new ArrayList<>();
    transform(backupOplogFiles, backupOplogNames, getFileNameTransformer());

    // Make sure that the performBackupIncremental backup for the missing member contains all of the
    // operation logs for that member. This proves that a full backup was performed for that member.
    assertThat(backupOplogNames).containsAll(missingMemberOplogNames);
  }

  private void createCache(final File diskDir, final File userDir) {
    CacheFactory cacheFactory = new CacheFactory(getDistributedSystemProperties())
        .set(DEPLOY_WORKING_DIR, userDir.getAbsolutePath());

    cacheRule.getOrCreateCache(cacheFactory);

    createDiskStore(diskStoreName1, diskDir);
    createDiskStore(diskStoreName2, diskDir);

    createRegion(regionName1, diskStoreName1);
    createRegion(regionName2, diskStoreName2);
  }

  private void createDiskStore(final String diskStoreName, final File diskDir) {
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] {diskDir});
    diskStoreFactory.create(diskStoreName);
  }

  private void createRegion(final String regionName, final String diskStoreName) {
    PartitionAttributesFactory<Integer, String> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setTotalNumBuckets(5);

    RegionFactory<Integer, String> regionFactory =
        getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
    regionFactory.create(regionName);
  }

  private InternalCache getCache() {
    return cacheRule.getCache();
  }

  private File getBaselineDir() {
    return baselineDir;
  }

  private File getIncrementalDir() {
    return incrementalDir1;
  }

  private Set<PersistentID> getMissingPersistentMembers() {
    return AdminDistributedSystemImpl
        .getMissingPersistentMembers(getCache().getDistributionManager());
  }

  private BackupStatus performBackup(final String targetDirPath) {
    return performBackup(targetDirPath, null);
  }

  private BackupStatus performBackup(final String targetDirPath, final String baselineDirPath) {
    return new BackupOperation(getCache().getDistributionManager(), getCache())
        .backupAllMembers(targetDirPath, baselineDirPath);
  }

  private String getModifiedMemberId() {
    return getCache().getDistributedSystem().getDistributedMember().toString()
        .replaceAll("[^\\w]+", "_");
  }

  private PersistentID getPersistentID(final String diskStoreName) {
    for (DiskStore diskStore : getCache().listDiskStores()) {
      if (diskStore.getName().equals(diskStoreName)) {
        return ((DiskStoreImpl) diskStore).getPersistentID();
      }
    }
    throw new AssertionError("Failed to find disk store " + diskStoreName);
  }

  private String getBaselineBackupPath() {
    File[] dirs = getBaselineDir().listFiles((FileFilter) DIRECTORY);
    assertThat(dirs).hasSize(1);
    return dirs[0].getAbsolutePath();
  }

  private File getBackupDirForMember(final File rootDir, final CharSequence memberId) {
    File[] dateDirs = rootDir.listFiles((FileFilter) DIRECTORY);
    assertThat(dateDirs).hasSize(1);

    File[] memberDirs =
        dateDirs[0].listFiles(file -> file.isDirectory() && file.getName().contains(memberId));
    assertThat(memberDirs).hasSize(1);

    return memberDirs[0];
  }

  private void performPuts() {
    Region<Integer, String> region = getCache().getRegion(regionName1);

    // Fill our region data
    for (int i = dataStart; i < dataEnd; ++i) {
      region.put(i, Integer.toString(i));
    }

    Region<Integer, String> barRegion = getCache().getRegion(regionName2);

    // Fill our region data
    for (int i = dataStart; i < dataEnd; ++i) {
      barRegion.put(i, Integer.toString(i));
    }

    dataStart += DATA_INCREMENT;
    dataEnd += DATA_INCREMENT;
  }

  private void validateBackupStatus(final BackupStatus backupStatus) {
    Map<DistributedMember, Set<PersistentID>> backupMap = backupStatus.getBackedUpDiskStores();
    assertThat(backupMap).isNotEmpty();

    for (DistributedMember member : backupMap.keySet()) {
      assertThat(backupMap.get(member)).isNotEmpty();
      for (PersistentID id : backupMap.get(member)) {
        assertThat(id.getHost()).isNotNull();
        assertThat(id.getUUID()).isNotNull();
        assertThat(id.getDirectory()).isNotNull();
      }
    }
  }

  private void installNewBackupMembershipListener() {
    if (backupMembershipListener != null) {
      getCache().getDistributionManager().removeMembershipListener(backupMembershipListener);
    }
    backupMembershipListener = new BackupMembershipListener();
    getCache().getDistributionManager().addMembershipListener(backupMembershipListener);
  }

  private static class BackupMembershipListener implements MembershipListener {

    private final AtomicBoolean memberDeparted = new AtomicBoolean();

    @Override
    public void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      memberDeparted.set(true);
    }

    boolean hasMemberDeparted() {
      return memberDeparted.get();
    }
  }
}
