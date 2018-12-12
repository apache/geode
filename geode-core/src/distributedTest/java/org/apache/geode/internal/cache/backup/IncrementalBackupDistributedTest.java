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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.apache.commons.io.filefilter.DirectoryFileFilter.DIRECTORY;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DeployedJar;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.process.ProcessStreamReader;
import org.apache.geode.internal.process.ProcessStreamReader.ReadingMode;
import org.apache.geode.internal.util.TransformUtils;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.test.compiler.ClassBuilder;
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
  private static final Logger logger = LogService.getLogger();

  private static final int DATA_INCREMENT = 10_000;
  private static final RegexFileFilter OPLOG_FILTER = new RegexFileFilter(".*\\.[kdc]rf$");

  private int dataStart;
  private int dataEnd = dataStart + DATA_INCREMENT;

  private String diskStoreName1;
  private String diskStoreName2;
  private String regionName1;
  private String regionName2;

  private VM vm0;
  private VM vm1;

  private transient Process process;
  private transient ProcessStreamReader processReader;

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

    vm0.invoke(() -> createCache(diskDirRule.getDiskDirFor(vm0)));
    vm1.invoke(() -> createCache(diskDirRule.getDiskDirFor(vm1)));

    createCache(diskDirRule.getDiskDirFor(getController()));

    performPuts();
  }

  @After
  public void tearDown() throws Exception {
    if (process != null && process.isAlive()) {
      process.destroyForcibly();
      process.waitFor(2, MINUTES);
    }
    if (processReader != null && processReader.isRunning()) {
      processReader.stop();
    }
  }

  /**
   * This tests the basic features of performBackupIncremental backup. This means that operation
   * logs that are present in both the performBackupBaseline and member's disk store should not be
   * copied during the performBackupIncremental backup. Additionally, the restore script should
   * reference and copy operation logs from the performBackupBaseline backup.
   */
  @Test
  public void testIncrementalBackup() throws Exception {
    String memberId = vm1.invoke(() -> getModifiedMemberId());

    File memberDir = diskDirRule.getDiskDirFor(vm1);

    // Find all of the member's oplogs in the disk directory (*.crf,*.krf,*.drf)
    Collection<File> memberOplogFiles = listFiles(memberDir, OPLOG_FILTER, DIRECTORY);
    assertThat(memberOplogFiles).isNotEmpty();

    // Perform a full backup and wait for it to finish
    validateBackupStatus(vm1.invoke(() -> performBackup(getBaselinePath())));
    vm1.invoke(() -> waitForBackup());

    // Find all of the member's oplogs in the performBackupBaseline (*.crf,*.krf,*.drf)
    Collection<File> memberBaselineOplogs =
        listFiles(getBackupDirForMember(getBaselineDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberBaselineOplogs).isNotEmpty();

    List<String> memberBaselineOplogNames = new LinkedList<>();
    TransformUtils.transform(memberBaselineOplogs, memberBaselineOplogNames,
        TransformUtils.fileNameTransformer);

    // Perform and performBackupIncremental backup and wait for it to finish
    performPuts(); // This preserves the new oplogs created by the performBackupBaseline backup
    validateBackupStatus(
        vm1.invoke(() -> performBackup(getIncrementalPath(), getBaselineBackupPath())));
    vm1.invoke(() -> waitForBackup());

    // Find all of the member's oplogs in the performBackupIncremental (*.crf,*.krf,*.drf)
    Collection<File> memberIncrementalOplogs =
        listFiles(getBackupDirForMember(getIncrementalDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberIncrementalOplogs).isNotEmpty();

    List<String> memberIncrementalOplogNames = new LinkedList<>();
    TransformUtils.transform(memberIncrementalOplogs, memberIncrementalOplogNames,
        TransformUtils.fileNameTransformer);

    // Assert that the performBackupIncremental backup does not contain performBackupBaseline
    // operation logs that the member still has copies of.
    assertThat(memberIncrementalOplogNames).doesNotContainAnyElementsOf(memberBaselineOplogNames);

    // Perform a second performBackupIncremental and wait for it to finish.

    // Doing this preserves the new oplogs created by the performBackupIncremental backup
    performPuts();
    validateBackupStatus(
        vm1.invoke(() -> performBackup(getIncremental2Path(), getIncrementalBackupPath())));
    vm1.invoke(() -> waitForBackup());

    Collection<File> memberIncremental2Oplogs =
        listFiles(getBackupDirForMember(getIncremental2Dir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberIncremental2Oplogs).isNotEmpty();

    List<String> memberIncremental2OplogNames = new LinkedList<>();
    TransformUtils.transform(memberIncremental2Oplogs, memberIncremental2OplogNames,
        TransformUtils.fileNameTransformer);

    // Assert that the second performBackupIncremental backup does not contain operation logs copied
    // into the performBackupBaseline.
    assertThat(memberIncremental2OplogNames).doesNotContainAnyElementsOf(memberBaselineOplogNames);

    // Also assert that the second performBackupIncremental backup does not contain operation logs
    // copied into the member's first performBackupIncremental backup.
    assertThat(memberIncremental2OplogNames)
        .doesNotContainAnyElementsOf(memberIncrementalOplogNames);

    // Shut down our member so we can perform a restore
    PersistentID id = vm1.invoke(() -> getPersistentID(diskStoreName1));
    vm1.invoke(() -> cacheRule.getCache().close());

    // Execute the restore
    performRestore(new File(id.getDirectory()),
        getBackupDirForMember(getIncremental2Dir(), memberId));

    // Collect all of the restored operation logs.
    Collection<File> restoredOplogs =
        listFiles(new File(id.getDirectory()), OPLOG_FILTER, DIRECTORY);
    assertThat(restoredOplogs).isNotEmpty();

    List<String> restoredOplogNames = new LinkedList<>();
    TransformUtils.transform(restoredOplogs, restoredOplogNames,
        TransformUtils.fileNameTransformer);

    // Assert that performBackupBaseline operation logs have been copied over to the member's disk
    // directory.
    assertThat(restoredOplogNames).containsAll(memberBaselineOplogNames);

    // Assert that the performBackupIncremental operation logs have been copied over to the member's
    // disk directory.
    assertThat(restoredOplogNames).containsAll(memberIncrementalOplogNames);

    // Assert that the second performBackupIncremental operation logs have been copied over to the
    // member's disk directory.
    assertThat(restoredOplogNames).containsAll(memberIncremental2OplogNames);

    // Reconnect the member.
    vm1.invoke(() -> createCache(diskDirRule.getDiskDirFor(vm1)));
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
    // Simulate the missing member by forcing a persistent member to go offline.
    PersistentID missingMember = vm0.invoke(() -> getPersistentID(diskStoreName1));
    vm0.invoke(() -> cacheRule.getCache().close());

    await()
        .until(() -> vm1.invoke(() -> getMissingPersistentMembers().contains(missingMember)));

    // Perform performBackupBaseline and make sure that list of offline disk stores contains our
    // missing member.
    BackupStatus baselineStatus = vm1.invoke(() -> performBackup(getBaselinePath()));
    validateBackupStatus(baselineStatus);
    assertThat(baselineStatus.getOfflineDiskStores()).isNotNull().hasSize(2);

    // Find all of the member's oplogs in the missing member's diskstore directory structure
    // (*.crf,*.krf,*.drf)
    Collection<File> missingMemberOplogFiles =
        listFiles(new File(missingMember.getDirectory()), OPLOG_FILTER, DIRECTORY);
    assertThat(missingMemberOplogFiles).isNotEmpty();

    // Restart our missing member and make sure it is back online and part of the cluster
    vm0.invoke(() -> createCache(diskDirRule.getDiskDirFor(vm0)));

    // After reconnecting make sure the other members agree that the missing member is back online.
    await()
        .untilAsserted(
            () -> assertThat(getMissingPersistentMembers()).doesNotContain(missingMember));

    // Perform performBackupIncremental and make sure we have no offline disk stores.
    BackupStatus incrementalStatus =
        vm1.invoke(() -> performBackup(getIncrementalPath(), getBaselineBackupPath()));
    validateBackupStatus(incrementalStatus);
    assertThat(incrementalStatus.getOfflineDiskStores()).isNotNull().isEmpty();

    // Get the missing member's member id which is different from the PersistentID
    String memberId = vm0.invoke(() -> getModifiedMemberId());

    // Get list of backed up oplog files in the performBackupIncremental backup for the missing
    // member
    File incrementalMemberDir = getBackupDirForMember(getIncrementalDir(), memberId);
    Collection<File> backupOplogFiles = listFiles(incrementalMemberDir, OPLOG_FILTER, DIRECTORY);
    assertThat(backupOplogFiles).isNotEmpty();

    // Transform missing member oplogs to just their file names.
    List<String> missingMemberOplogNames = new LinkedList<>();
    TransformUtils.transform(missingMemberOplogFiles, missingMemberOplogNames,
        TransformUtils.fileNameTransformer);

    // Transform missing member's performBackupIncremental backup oplogs to just their file names.
    List<String> backupOplogNames = new LinkedList<>();
    TransformUtils.transform(backupOplogFiles, backupOplogNames,
        TransformUtils.fileNameTransformer);

    // Make sure that the performBackupIncremental backup for the missing member contains all of the
    // operation logs for that member. This proves that a full backup was performed for that member.
    assertThat(backupOplogNames).containsAll(missingMemberOplogNames);
  }

  /**
   * Successful if a member performs a full backup if their backup is marked as incomplete in the
   * performBackupBaseline.
   */
  @Test
  public void testIncompleteInBaseline() {
    // Get the member ID for VM 1 and perform a performBackupBaseline.
    String memberId = vm1.invoke(() -> getModifiedMemberId());
    validateBackupStatus(vm1.invoke(() -> performBackup(getBaselinePath())));

    // Find all of the member's oplogs in the performBackupBaseline (*.crf,*.krf,*.drf)
    Collection<File> memberBaselineOplogs =
        listFiles(getBackupDirForMember(getBaselineDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberBaselineOplogs).isNotEmpty();

    List<String> memberBaselineOplogNames = new LinkedList<>();
    TransformUtils.transform(memberBaselineOplogs, memberBaselineOplogNames,
        TransformUtils.fileNameTransformer);

    vm1.invoke(() -> {
      File backupDir = getBackupDirForMember(getBaselineDir(), getModifiedMemberId());
      assertThat(backupDir).exists();

      // Mark the performBackupBaseline as incomplete (even though it really isn't)
      File incomplete = new File(backupDir, BackupWriter.INCOMPLETE_BACKUP_FILE);
      assertThat(incomplete.createNewFile()).isTrue();
    });

    // Do an performBackupIncremental. It should discover that the performBackupBaseline is
    // incomplete and backup all of the operation logs that are in the performBackupBaseline.
    validateBackupStatus(
        vm1.invoke(() -> performBackup(getIncrementalPath(), getBaselineBackupPath())));

    // Find all of the member's oplogs in the performBackupIncremental (*.crf,*.krf,*.drf)
    Collection<File> memberIncrementalOplogs =
        listFiles(getBackupDirForMember(getIncrementalDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberIncrementalOplogs).isNotEmpty();

    List<String> memberIncrementalOplogNames = new LinkedList<>();
    TransformUtils.transform(memberIncrementalOplogs, memberIncrementalOplogNames,
        TransformUtils.fileNameTransformer);

    // Assert that all of the performBackupBaseline operation logs are in the
    // performBackupIncremental backup. If so, then the incomplete marker was discovered in the
    // performBackupBaseline by the performBackupIncremental backup process.
    assertThat(memberIncrementalOplogNames).containsAll(memberBaselineOplogNames);
  }

  /**
   * Successful if all members perform a full backup when they share the performBackupBaseline
   * directory and it is missing.
   */
  @Test
  public void testMissingBaseline() throws Exception {
    // Get the member ID for VM 1 and perform a performBackupBaseline.
    String memberId = vm1.invoke(() -> getModifiedMemberId());
    validateBackupStatus(vm1.invoke(() -> performBackup(getBaselinePath())));

    // Find all of the member's oplogs in the performBackupBaseline (*.crf,*.krf,*.drf)
    Collection<File> memberBaselineOplogs =
        listFiles(getBackupDirForMember(getBaselineDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberBaselineOplogs).isNotEmpty();

    List<String> memberBaselineOplogNames = new LinkedList<>();
    TransformUtils.transform(memberBaselineOplogs, memberBaselineOplogNames,
        TransformUtils.fileNameTransformer);

    // Do an performBackupIncremental after deleting the performBackupBaseline. It should discover
    // that the performBackupBaseline is gone and backup all of the operation logs that are in the
    // performBackupBaseline.
    FileUtils.deleteDirectory(getBaselineDir());

    // Custom performBackupIncremental backup callable that retrieves the current
    // performBackupBaseline before deletion.
    vm1.invoke(() -> {
      new BackupOperation(cacheRule.getSystem().getDistributionManager(), cacheRule.getCache())
          .backupAllMembers(getIncrementalPath(), getBaselinePath());
    });

    // Find all of the member's oplogs in the performBackupIncremental (*.crf,*.krf,*.drf)
    Collection<File> memberIncrementalOplogs =
        listFiles(getBackupDirForMember(getIncrementalDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberIncrementalOplogs).isNotEmpty();

    List<String> memberIncrementalOplogNames = new LinkedList<>();
    TransformUtils.transform(memberIncrementalOplogs, memberIncrementalOplogNames,
        TransformUtils.fileNameTransformer);

    // Assert that all of the performBackupBaseline operation logs are in the
    // performBackupIncremental backup. If so, then the missing performBackupBaseline was discovered
    // by the performBackupIncremental backup process.
    assertThat(memberIncrementalOplogNames).containsAll(memberBaselineOplogNames);
  }

  /**
   * Verifies that a user deployed jar file is included as part of the backup.
   */
  @Test
  public void testBackupUserDeployedJarFiles() throws Exception {
    String jarName = "BackupJarDeploymentDUnit";
    byte[] classBytes = new ClassBuilder().createJarFromName(jarName);

    File jarFile = temporaryFolder.newFile();
    IOUtils.copyLarge(new ByteArrayInputStream(classBytes), new FileOutputStream(jarFile));

    // Deploy a "dummy" jar to the VM.
    File deployedJarFile = vm0.invoke(() -> {
      DeployedJar deployedJar =
          ClassPathLoader.getLatest().getJarDeployer().deploy(jarName, jarFile);
      return deployedJar.getFile();
    });

    assertThat(deployedJarFile).exists();

    // Perform backup. Make sure it is successful.
    validateBackupStatus(vm0.invoke(() -> performBackup(getBaselinePath())));

    // Make sure the user deployed jar is part of the backup.
    Collection<File> memberDeployedJarFiles =
        listFiles(getBackupDirForMember(getBaselineDir(), vm0.invoke(() -> getModifiedMemberId())),
            new RegexFileFilter(".*" + jarName + ".*"), DIRECTORY);
    assertThat(memberDeployedJarFiles).isNotEmpty();

    // Shut down our member so we can perform a restore
    PersistentID id = vm0.invoke(() -> getPersistentID(diskStoreName1));
    vm0.invoke(() -> cacheRule.getCache().close());

    // Get the VM's user directory.
    String vmDir = vm0.invoke(() -> System.getProperty("user.dir"));

    File backupDir =
        getBackupDirForMember(getBaselineDir(), vm0.invoke(() -> getModifiedMemberId()));

    // Cleanup "dummy" jar from file system.
    deleteMatching(new File("."), Pattern.compile('^' + jarName + ".*#\\d++$"));

    // Execute the restore
    performRestore(new File(id.getDirectory()), backupDir);

    // Make sure the user deployed jar is part of the restore.
    Collection<File> restoredJars =
        listFiles(new File(vmDir), new RegexFileFilter(".*" + jarName + ".*"), DIRECTORY);
    assertThat(restoredJars).isNotEmpty();

    List<String> restoredJarNames = new LinkedList<>();
    TransformUtils.transform(memberDeployedJarFiles, restoredJarNames,
        TransformUtils.fileNameTransformer);
    for (String name : restoredJarNames) {
      assertThat(name).contains(jarName);
    }

    // Restart the member
    vm0.invoke(() -> createCache(diskDirRule.getDiskDirFor(vm0)));

    // Remove the "dummy" jar from the VM.
    vm0.invoke(() -> {
      for (DeployedJar jarClassLoader : ClassPathLoader.getLatest().getJarDeployer()
          .findDeployedJars()) {
        if (jarClassLoader.getJarName().startsWith(jarName)) {
          ClassPathLoader.getLatest().getJarDeployer().undeploy(jarClassLoader.getJarName());
        }
      }
    });

    // Cleanup "dummy" jar from file system.
    deleteMatching(new File(vmDir), Pattern.compile('^' + jarName + ".*#\\d++$"));
  }

  private void createCache(final File diskDir) {
    cacheRule.getOrCreateCache();

    createDiskStore(diskStoreName1, diskDir);
    createDiskStore(diskStoreName2, diskDir);

    createRegion(regionName1, diskStoreName1);
    createRegion(regionName2, diskStoreName2);
  }

  private void createDiskStore(final String diskStoreName, final File diskDir) {
    DiskStoreFactory diskStoreFactory = cacheRule.getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] {diskDir});
    diskStoreFactory.create(diskStoreName);
  }

  private void createRegion(final String regionName, final String diskStoreName) {
    PartitionAttributesFactory<Integer, String> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setTotalNumBuckets(5);

    RegionFactory<Integer, String> regionFactory =
        cacheRule.getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
    regionFactory.create(regionName);
  }

  private File getBaselineDir() {
    File dir = new File(temporaryFolder.getRoot(), "baseline");
    if (!dir.exists()) {
      dir.mkdirs();
    }

    return dir;
  }

  private String getBaselinePath() {
    return getBaselineDir().getAbsolutePath();
  }

  private File getIncrementalDir() {
    File dir = new File(temporaryFolder.getRoot(), "incremental");
    if (!dir.exists()) {
      dir.mkdirs();
    }

    return dir;
  }

  private String getIncrementalPath() {
    return getIncrementalDir().getAbsolutePath();
  }

  private File getIncremental2Dir() {
    File dir = new File(temporaryFolder.getRoot(), "incremental2");
    if (!dir.exists()) {
      dir.mkdirs();
    }

    return dir;
  }

  private String getIncremental2Path() {
    return getIncremental2Dir().getAbsolutePath();
  }

  private Set<PersistentID> getMissingPersistentMembers() {
    return AdminDistributedSystemImpl
        .getMissingPersistentMembers(cacheRule.getCache().getDistributionManager());
  }

  private BackupStatus performBackup(final String targetDirPath) {
    return performBackup(targetDirPath, null);
  }

  private BackupStatus performBackup(final String targetDirPath, final String baselineDirPath) {
    return new BackupOperation(cacheRule.getCache().getDistributionManager(), cacheRule.getCache())
        .backupAllMembers(targetDirPath, baselineDirPath);
  }

  private String getModifiedMemberId() {
    return cacheRule.getCache().getDistributedSystem().getDistributedMember().toString()
        .replaceAll("[^\\w]+", "_");
  }

  private PersistentID getPersistentID(final String diskStoreName) {
    for (DiskStore diskStore : cacheRule.getCache().listDiskStores()) {
      if (diskStore.getName().equals(diskStoreName)) {
        return ((DiskStoreImpl) diskStore).getPersistentID();
      }
    }
    throw new Error("Failed to find disk store " + diskStoreName);
  }

  private void waitForBackup() {
    Collection<DiskStore> backupInProgress = cacheRule.getCache().listDiskStores();
    List<DiskStoreImpl> backupCompleteList = new LinkedList<>();

    while (backupCompleteList.size() < backupInProgress.size()) {
      for (DiskStore diskStore : backupInProgress) {
        if (((DiskStoreImpl) diskStore).getInProgressBackup() == null
            && !backupCompleteList.contains(diskStore)) {
          backupCompleteList.add((DiskStoreImpl) diskStore);
        }
      }
    }
  }

  private String getBaselineBackupPath() {
    File[] dirs = getBaselineDir().listFiles((FileFilter) DIRECTORY);
    assertThat(dirs).hasSize(1);
    return dirs[0].getAbsolutePath();
  }

  private String getIncrementalBackupPath() {
    File[] dirs = getIncrementalDir().listFiles((FileFilter) DIRECTORY);
    assertThat(dirs).hasSize(1);
    return dirs[0].getAbsolutePath();
  }

  private File getBackupDirForMember(final File rootDir, final String memberId) {
    File[] dateDirs = rootDir.listFiles((FileFilter) DIRECTORY);
    assertThat(dateDirs).hasSize(1);

    File[] memberDirs =
        dateDirs[0].listFiles(file -> file.isDirectory() && file.getName().contains(memberId));
    assertThat(memberDirs).hasSize(1);

    return memberDirs[0];
  }

  private ReadingMode getReadingMode() {
    return SystemUtils.isWindows() ? ReadingMode.NON_BLOCKING : ReadingMode.BLOCKING;
  }

  private void execute(final String command) throws IOException, InterruptedException {
    process = new ProcessBuilder(command).redirectErrorStream(true).start();

    processReader = new ProcessStreamReader.Builder(process).inputStream(process.getInputStream())
        .inputListener(line -> logger.info("OUTPUT: {}", line))
        .readingMode(getReadingMode()).continueReadingMillis(2 * 1000).build().start();

    assertThat(process.waitFor(5, MINUTES)).isTrue();
    assertThat(process.exitValue()).isEqualTo(0);
  }

  private void performRestore(final File memberDir, final File backupDir)
      throws IOException, InterruptedException {
    // The restore script will not restore if there is an if file in the copy to directory. Remove
    // these files first.
    Collection<File> ifFiles = listFiles(memberDir, new RegexFileFilter(".*\\.if$"), DIRECTORY);
    for (File file : ifFiles) {
      assertThat(file.delete()).isTrue();
    }

    // Remove all operation logs.
    Collection<File> oplogs = listFiles(memberDir, OPLOG_FILTER, DIRECTORY);
    for (File file : oplogs) {
      assertThat(file.delete()).isTrue();
    }

    // Get a hold of the restore script and make sure it is there.
    File restoreScript = new File(backupDir, "restore.sh");
    if (!restoreScript.exists()) {
      restoreScript = new File(backupDir, "restore.bat");
    }
    assertThat(restoreScript).exists();

    execute(restoreScript.getAbsolutePath());
  }

  private void performPuts() {
    Region<Integer, String> region = cacheRule.getCache().getRegion(regionName1);

    // Fill our region data
    for (int i = dataStart; i < dataEnd; ++i) {
      region.put(i, Integer.toString(i));
    }

    Region<Integer, String> barRegion = cacheRule.getCache().getRegion(regionName2);

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

  private void deleteMatching(final File dir, final Pattern pattern) throws IOException {
    Collection<File> files = listFiles(dir, new RegexFileFilter(pattern), DIRECTORY);
    for (File file : files) {
      Files.delete(file.toPath());
    }
  }
}
