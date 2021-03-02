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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.apache.commons.io.filefilter.DirectoryFileFilter.DIRECTORY;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.DEPLOY_WORKING_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.util.TransformUtils.getFileNameTransformer;
import static org.apache.geode.internal.util.TransformUtils.transform;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DeployedJar;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.process.ProcessStreamReader;
import org.apache.geode.internal.process.ProcessStreamReader.ReadingMode;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.rules.DiskDirRule;

public class IncrementalBackupIntegrationTest {
  private static final Logger logger = LogService.getLogger();

  private static final long TIMEOUT_MILLIS = GeodeAwaitility.getTimeout().toMillis();
  private static final int DATA_INCREMENT = 10_000;
  private static final IOFileFilter OPLOG_FILTER = new RegexFileFilter(".*\\.[kdc]rf$");

  private int dataStart;
  private int dataEnd = dataStart + DATA_INCREMENT;

  private InternalCache cache;

  private String diskStoreName1;
  private String diskStoreName2;
  private String regionName1;
  private String regionName2;

  private File userDir;
  private File diskDir;
  private File baselineDir;
  private File incrementalDir1;
  private File incrementalDir2;

  private Process process;
  private ProcessStreamReader processReader;

  @Rule
  public DiskDirRule diskDirRule = new DiskDirRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();

    diskStoreName1 = uniqueName + "_diskStore-1";
    diskStoreName2 = uniqueName + "_diskStore-2";
    regionName1 = uniqueName + "_region-1";
    regionName2 = uniqueName + "_region-2";

    userDir = temporaryFolder.getRoot();
    diskDir = diskDirRule.getDiskDir();
    baselineDir = temporaryFolder.newFolder("baseline");
    incrementalDir1 = temporaryFolder.newFolder("incremental");
    incrementalDir2 = temporaryFolder.newFolder("incremental2");

    createCache();
    performPuts();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    if (process != null && process.isAlive()) {
      process.destroyForcibly();
      process.waitFor(TIMEOUT_MILLIS, MILLISECONDS);
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
    String memberId = getModifiedMemberId();

    // Find all of the member's oplogs in the disk directory (*.crf,*.krf,*.drf)
    Collection<File> memberOplogFiles = listFiles(diskDir, OPLOG_FILTER, DIRECTORY);
    assertThat(memberOplogFiles).isNotEmpty();

    // Perform a full backup and wait for it to finish
    validateBackupStatus(performBackup(getBaselinePath()));
    waitForBackup();

    // Find all of the member's oplogs in the performBackupBaseline (*.crf,*.krf,*.drf)
    Collection<File> memberBaselineOplogs =
        listFiles(getBackupDirForMember(getBaselineDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberBaselineOplogs).isNotEmpty();

    Collection<String> memberBaselineOplogNames = new ArrayList<>();
    transform(memberBaselineOplogs, memberBaselineOplogNames, getFileNameTransformer());

    // Perform and performBackupIncremental backup and wait for it to finish
    performPuts(); // This preserves the new oplogs created by the performBackupBaseline backup
    validateBackupStatus(performBackup(getIncrementalPath(), getBaselineBackupPath()));
    waitForBackup();

    // Find all of the member's oplogs in the performBackupIncremental (*.crf,*.krf,*.drf)
    Collection<File> memberIncrementalOplogs =
        listFiles(getBackupDirForMember(getIncrementalDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberIncrementalOplogs).isNotEmpty();

    List<String> memberIncrementalOplogNames = new ArrayList<>();
    transform(memberIncrementalOplogs, memberIncrementalOplogNames, getFileNameTransformer());

    // Assert that the performBackupIncremental backup does not contain performBackupBaseline
    // operation logs that the member still has copies of.
    assertThat(memberIncrementalOplogNames).doesNotContainAnyElementsOf(memberBaselineOplogNames);

    // Perform a second performBackupIncremental and wait for it to finish.

    // Doing this preserves the new oplogs created by the performBackupIncremental backup
    performPuts();
    validateBackupStatus(performBackup(getIncremental2Path(), getIncrementalBackupPath()));
    waitForBackup();

    Collection<File> memberIncremental2Oplogs =
        listFiles(getBackupDirForMember(getIncremental2Dir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberIncremental2Oplogs).isNotEmpty();

    List<String> memberIncremental2OplogNames = new ArrayList<>();
    transform(memberIncremental2Oplogs, memberIncremental2OplogNames, getFileNameTransformer());

    // Assert that the second performBackupIncremental backup does not contain operation logs copied
    // into the performBackupBaseline.
    assertThat(memberIncremental2OplogNames)
        .doesNotContainAnyElementsOf(memberBaselineOplogNames);

    // Also assert that the second performBackupIncremental backup does not contain operation logs
    // copied into the member's first performBackupIncremental backup.
    assertThat(memberIncremental2OplogNames)
        .doesNotContainAnyElementsOf(memberIncrementalOplogNames);

    // Shut down our member so we can perform a restore
    PersistentID id = getPersistentID(diskStoreName1);
    getCache().close();

    // Execute the restore
    performRestore(new File(id.getDirectory()),
        getBackupDirForMember(getIncremental2Dir(), memberId));

    // Collect all of the restored operation logs.
    Collection<File> restoredOplogs =
        listFiles(new File(id.getDirectory()), OPLOG_FILTER, DIRECTORY);
    assertThat(restoredOplogs).isNotEmpty();

    List<String> restoredOplogNames = new ArrayList<>();
    transform(restoredOplogs, restoredOplogNames, getFileNameTransformer());

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
    createCache();
  }

  /**
   * Successful if a member performs a full backup if their backup is marked as incomplete in the
   * performBackupBaseline.
   */
  @Test
  public void testIncompleteInBaseline() throws Exception {
    // Get the member ID for VM 1 and perform a performBackupBaseline.
    String memberId = getModifiedMemberId();
    validateBackupStatus(performBackup(getBaselinePath()));

    // Find all of the member's oplogs in the performBackupBaseline (*.crf,*.krf,*.drf)
    Collection<File> memberBaselineOplogs =
        listFiles(getBackupDirForMember(getBaselineDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberBaselineOplogs).isNotEmpty();

    Collection<String> memberBaselineOplogNames = new ArrayList<>();
    transform(memberBaselineOplogs, memberBaselineOplogNames, getFileNameTransformer());

    File backupDir = getBackupDirForMember(getBaselineDir(), getModifiedMemberId());
    assertThat(backupDir).exists();

    // Mark the performBackupBaseline as incomplete (even though it really isn't)
    File incomplete = new File(backupDir, BackupWriter.INCOMPLETE_BACKUP_FILE);
    assertThat(incomplete.createNewFile()).isTrue();

    // Do an performBackupIncremental. It should discover that the performBackupBaseline is
    // incomplete and backup all of the operation logs that are in the performBackupBaseline.
    validateBackupStatus(performBackup(getIncrementalPath(), getBaselineBackupPath()));

    // Find all of the member's oplogs in the performBackupIncremental (*.crf,*.krf,*.drf)
    Collection<File> memberIncrementalOplogs =
        listFiles(getBackupDirForMember(getIncrementalDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberIncrementalOplogs).isNotEmpty();

    List<String> memberIncrementalOplogNames = new ArrayList<>();
    transform(memberIncrementalOplogs, memberIncrementalOplogNames, getFileNameTransformer());

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
    String memberId = getModifiedMemberId();
    validateBackupStatus(performBackup(getBaselinePath()));

    // Find all of the member's oplogs in the performBackupBaseline (*.crf,*.krf,*.drf)
    Collection<File> memberBaselineOplogs =
        listFiles(getBackupDirForMember(getBaselineDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberBaselineOplogs).isNotEmpty();

    Collection<String> memberBaselineOplogNames = new ArrayList<>();
    transform(memberBaselineOplogs, memberBaselineOplogNames, getFileNameTransformer());

    // Do an performBackupIncremental after deleting the performBackupBaseline. It should discover
    // that the performBackupBaseline is gone and backup all of the operation logs that are in the
    // performBackupBaseline.
    FileUtils.deleteDirectory(getBaselineDir());

    // Custom performBackupIncremental backup callable that retrieves the current
    // performBackupBaseline before deletion.
    new BackupOperation(getSystem().getDistributionManager(), getCache())
        .backupAllMembers(getIncrementalPath(), getBaselinePath());

    // Find all of the member's oplogs in the performBackupIncremental (*.crf,*.krf,*.drf)
    Collection<File> memberIncrementalOplogs =
        listFiles(getBackupDirForMember(getIncrementalDir(), memberId), OPLOG_FILTER, DIRECTORY);
    assertThat(memberIncrementalOplogs).isNotEmpty();

    List<String> memberIncrementalOplogNames = new ArrayList<>();
    transform(memberIncrementalOplogs, memberIncrementalOplogNames, getFileNameTransformer());

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
    String jarName = getClass().getSimpleName();
    byte[] classBytes = new ClassBuilder().createJarFromName(jarName);

    File jarFile = temporaryFolder.newFile(jarName + ".jar");
    IOUtils.copyLarge(new ByteArrayInputStream(classBytes), new FileOutputStream(jarFile));

    // Deploy a "dummy" jar to the VM.
    File deployedJarFile =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarFile).getFile();

    assertThat(deployedJarFile).exists();

    // Perform backup. Make sure it is successful.
    validateBackupStatus(performBackup(getBaselinePath()));

    // Make sure the user deployed jar is part of the backup.
    Collection<File> memberDeployedJarFiles =
        listFiles(getBackupDirForMember(getBaselineDir(), getModifiedMemberId()),
            new RegexFileFilter(".*" + jarName + ".*"), DIRECTORY);
    assertThat(memberDeployedJarFiles).isNotEmpty();

    // Shut down our member so we can perform a restore
    PersistentID id = getPersistentID(diskStoreName1);
    getCache().close();

    File backupDir =
        getBackupDirForMember(getBaselineDir(), getModifiedMemberId());

    // Cleanup "dummy" jar from file system.
    deleteMatching(new File("."), Pattern.compile('^' + jarName + ".*#\\d++$"));

    // Execute the restore
    performRestore(new File(id.getDirectory()), backupDir);

    // Make sure the user deployed jar is part of the restore.
    Collection<File> restoredJars =
        listFiles(userDir, new RegexFileFilter(".*" + jarName + ".*"), DIRECTORY);
    assertThat(restoredJars).isNotEmpty();

    Collection<String> restoredJarNames = new ArrayList<>();
    transform(memberDeployedJarFiles, restoredJarNames, getFileNameTransformer());
    for (String name : restoredJarNames) {
      assertThat(name).contains(jarName);
    }

    // Restart the member
    createCache();

    // Remove the "dummy" jar from the VM.
    for (DeployedJar jarClassLoader : ClassPathLoader.getLatest().getJarDeployer()
        .findDeployedJars()) {
      if (jarClassLoader.getArtifactId().startsWith(jarName)) {
        ClassPathLoader.getLatest().getJarDeployer().undeploy(jarClassLoader.getDeployedFileName());
      }
    }

    // Cleanup "dummy" jar from file system.
    deleteMatching(userDir, Pattern.compile('^' + jarName + ".*#\\d++$"));
  }

  private void createCache() {
    cache = (InternalCache) new CacheFactory()
        .set(LOCATORS, "")
        .set(DEPLOY_WORKING_DIR, userDir.getAbsolutePath())
        .create();

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
    return cache;
  }

  private InternalDistributedSystem getSystem() {
    return cache.getInternalDistributedSystem();
  }

  private File getBaselineDir() {
    return baselineDir;
  }

  private String getBaselinePath() {
    return getBaselineDir().getAbsolutePath();
  }

  private File getIncrementalDir() {
    return incrementalDir1;
  }

  private String getIncrementalPath() {
    return getIncrementalDir().getAbsolutePath();
  }

  private File getIncremental2Dir() {
    return incrementalDir2;
  }

  private String getIncremental2Path() {
    return getIncremental2Dir().getAbsolutePath();
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

  private void waitForBackup() {
    Collection<DiskStore> backupInProgress = getCache().listDiskStores();
    Collection<DiskStoreImpl> backupCompleteList = new ArrayList<>();

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

  private File getBackupDirForMember(final File rootDir, final CharSequence memberId) {
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
    process = new ProcessBuilder(command)
        .directory(userDir)
        .redirectErrorStream(true)
        .start();

    processReader = new ProcessStreamReader.Builder(process)
        .inputStream(process.getInputStream())
        .inputListener(line -> logger.info("OUTPUT: {}", line))
        .readingMode(getReadingMode())
        .continueReadingMillis(2 * 1000)
        .build()
        .start();

    assertThat(process.waitFor(TIMEOUT_MILLIS, MILLISECONDS)).isTrue();
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

  private void deleteMatching(final File dir, final Pattern pattern) throws IOException {
    Collection<File> files = listFiles(dir, new RegexFileFilter(pattern), DIRECTORY);
    for (File file : files) {
      Files.delete(file.toPath());
    }
  }
}
