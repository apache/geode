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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminDistributedSystemFactory;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.BackupStatus;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DeployedJar;
import org.apache.geode.internal.cache.persistence.BackupManager;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.internal.util.TransformUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests for the incremental backup feature.
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class IncrementalBackupDUnitTest extends JUnit4CacheTestCase {
  /**
   * Data load increment.
   */
  private static final int DATA_INCREMENT = 10000;

  /**
   * Start value for data load.
   */
  private int dataStart = 0;

  /**
   * End value for data load.
   */
  private int dataEnd = this.dataStart + DATA_INCREMENT;

  /**
   * Regular expression used to search for member operation log files.
   */
  private static final String OPLOG_REGEX = ".*\\.[kdc]rf$";

  /**
   * Creates test regions for a member.
   */
  private final SerializableRunnable createRegions = new SerializableRunnable() {
    @Override
    public void run() {
      Cache cache = getCache(new CacheFactory().set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel()));
      cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("fooStore");
      cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("barStore");
      getRegionFactory(cache).setDiskStoreName("fooStore").create("fooRegion");
      getRegionFactory(cache).setDiskStoreName("barStore").create("barRegion");
    }
  };

  private RegionFactory<Integer, String> getRegionFactory(Cache cache) {
    return cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
  }

  /**
   * A FileFilter that looks for a timestamped gemfire backup directory.
   */
  private static final FileFilter backupDirFilter = file -> {
    // This will break in about 90 years...
    return file.isDirectory() && file.getName().startsWith("20");
  };

  /**
   * Abstracts the logging mechanism.
   * 
   * @param message a message to log.
   */
  private void log(String message) {
    LogWriterUtils.getLogWriter().info("[IncrementalBackupDUnitTest] " + message);
  }

  /**
   * @return the baseline backup directory.
   */
  private static File getBaselineDir() {
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File dir = new File(tmpDir, "baseline");
    if (!dir.exists()) {
      dir.mkdirs();
    }

    return dir;
  }

  /**
   * @return the second incremental backup directory.
   */
  private static File getIncremental2Dir() {
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File dir = new File(tmpDir, "incremental2");
    if (!dir.exists()) {
      dir.mkdirs();
    }

    return dir;
  }

  /**
   * @return the incremental backup directory.
   */
  private static File getIncrementalDir() {
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File dir = new File(tmpDir, "incremental");
    if (!dir.exists()) {
      dir.mkdirs();
    }

    return dir;
  }

  /**
   * Returns the directory for a given member.
   * 
   * @param vm a distributed system member.
   * @return the disk directories for a member.
   */
  private File getVMDir(VM vm) {
    return (File) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(new File(getDiskDirs()[0], "../.."));
      }
    });
  }

  /**
   * Invokes {@link AdminDistributedSystem#getMissingPersistentMembers()} on a member.
   * 
   * @param vm a member of the distributed system.
   * @return a set of missing members for the distributed system.
   */
  @SuppressWarnings("unchecked")
  private Set<PersistentID> getMissingMembers(VM vm) {
    return (Set<PersistentID>) vm.invoke(new SerializableCallable("getMissingMembers") {
      @Override
      public Object call() {
        return AdminDistributedSystemImpl
            .getMissingPersistentMembers(getSystem().getDistributionManager());
      }
    });
  }

  /**
   * Invokes {@link AdminDistributedSystem#backupAllMembers(File)} on a member.
   * 
   * @param vm a member of the distributed system
   * @return the status of the backup.
   */
  private BackupStatus baseline(VM vm) {
    return (BackupStatus) vm.invoke(new SerializableCallable("Backup all members.") {
      @Override
      public Object call() {
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          return adminDS.backupAllMembers(getBaselineDir());

        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

  /**
   * Invokes {@link AdminDistributedSystem#backupAllMembers(File, File)} on a member.
   * 
   * @param vm a member of the distributed system.
   * @return a status of the backup operation.
   */
  private BackupStatus incremental(VM vm) {
    return (BackupStatus) vm.invoke(new SerializableCallable("Backup all members.") {
      @Override
      public Object call() {
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          return adminDS.backupAllMembers(getIncrementalDir(), getBaselineBackupDir());

        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

  /**
   * Invokes {@link AdminDistributedSystem#backupAllMembers(File, File)} on a member.
   * 
   * @param vm a member of the distributed system.
   * @return a status of the backup operation.
   */
  private BackupStatus incremental2(VM vm) {
    return (BackupStatus) vm.invoke(new SerializableCallable("Backup all members.") {
      @Override
      public Object call() {
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          return adminDS.backupAllMembers(getIncremental2Dir(), getIncrementalBackupDir());

        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

  /**
   * Invokes {@link DistributedSystem#getDistributedMember()} on a member.
   * 
   * @param vm a distributed system member.
   * @return the member's id.
   */
  private String getMemberId(VM vm) {
    return (String) vm.invoke(new SerializableCallable("getMemberId") {
      @Override
      public Object call() throws Exception {
        return getCache().getDistributedSystem().getDistributedMember().toString()
            .replaceAll("[^\\w]+", "_");
      }
    });
  }

  /**
   * Invokes {@link Cache#close()} on a member.
   */
  private void closeCache(final VM closeVM) {
    closeVM.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        getCache().close();
      }
    });
  }

  /**
   * Locates the PersistentID for the testStore disk store for a distributed member.
   * 
   * @param vm a distributed member.
   * @return a PersistentID for a member's disk store.
   */
  private PersistentID getPersistentID(final VM vm, final String diskStoreName) {
    return vm.invoke(() -> {
      PersistentID id = null;
      Collection<DiskStore> diskStores = ((InternalCache) getCache()).listDiskStores();
      for (DiskStore diskStore : diskStores) {
        if (diskStore.getName().equals(diskStoreName)) {
          id = ((DiskStoreImpl) diskStore).getPersistentID();
          break;
        }
      }
      return id;
    });
  }

  /**
   * Locates the PersistentID for the testStore disk store for a distributed member.
   * 
   * @param vm a distributed member.
   * @return a PersistentID for a member's disk store.
   */
  private PersistentID getPersistentID(final VM vm) {
    return getPersistentID(vm, "fooStore");
  }

  /**
   * Invokes {@link DistributedSystem#disconnect()} on a member.
   * 
   * @param disconnectVM a member of the distributed system to disconnect.
   * @param testVM a member of the distributed system to test for the missing member (just
   *        disconnected).
   */
  private PersistentID disconnect(final VM disconnectVM, final VM testVM) {
    final PersistentID id = disconnectVM.invoke(() -> {
      PersistentID persistentID = null;
      Collection<DiskStore> diskStores = ((InternalCache) getCache()).listDiskStores();
      for (DiskStore diskStore : diskStores) {
        if (diskStore.getName().equals("fooStore")) {
          persistentID = ((DiskStoreImpl) diskStore).getPersistentID();
          break;
        }
      }

      getSystem().disconnect();

      return persistentID;
    });

    final Set<PersistentID> missingMembers = new HashSet<>();
    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        missingMembers.clear();
        missingMembers.addAll(getMissingMembers(testVM));

        return missingMembers.contains(id);
      }

      @Override
      public String description() {
        return "[IncrementalBackupDUnitTest] Waiting for missing member " + id;
      }
    }, 10000, 500, false);

    return id;
  }

  /**
   * Invokes {@link CacheFactory#create()} on a member.
   * 
   * @param vm a member of the distributed system.
   */
  private void openCache(VM vm) {
    vm.invoke(this.createRegions);
  }

  /**
   * Blocks and waits for a backup operation to finish on a distributed member.
   * 
   * @param vm a member of the distributed system.
   */
  private void waitForBackup(VM vm) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Collection<DiskStore> backupInProgress = ((InternalCache) getCache()).listDiskStores();
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
    });
  }

  /**
   * Performs a full backup.
   * 
   * @return the backup status.
   */
  private BackupStatus performBaseline() {
    return baseline(Host.getHost(0).getVM(1));
  }

  /**
   * Performs an incremental backup.
   * 
   * @return the backup status.
   */
  private BackupStatus performIncremental() {
    return incremental(Host.getHost(0).getVM(1));
  }

  /**
   * Performs a second incremental backup.
   */
  private BackupStatus performIncremental2() {
    return incremental2(Host.getHost(0).getVM(1));
  }

  /**
   * @return the directory for the completed baseline backup.
   */
  private static File getBaselineBackupDir() {
    File[] dirs = getBaselineDir().listFiles(backupDirFilter);
    assertEquals(1, dirs.length);
    return dirs[0];
  }

  /**
   * @return the directory for the completed baseline backup.
   */
  private static File getIncrementalBackupDir() {
    File[] dirs = getIncrementalDir().listFiles(backupDirFilter);
    assertEquals(1, dirs.length);
    return dirs[0];
  }

  /**
   * @return the directory for the completed baseline backup.
   */
  private static File getIncremental2BackupDir() {
    File[] dirs = getIncremental2Dir().listFiles(backupDirFilter);
    assertEquals(1, dirs.length);
    return dirs[0];
  }

  /**
   * Returns an individual member's backup directory.
   * 
   * @param rootDir the directory to begin searching for the member's backup dir from.
   * @param memberId the member's identifier.
   * @return the member's backup directory.
   */
  private File getBackupDirForMember(final File rootDir, final String memberId) {
    File[] dateDirs = rootDir.listFiles(backupDirFilter);
    assertEquals(1, dateDirs.length);

    File[] memberDirs = dateDirs[0].listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.isDirectory() && file.getName().contains(memberId);
      }
    });

    assertEquals(1, memberDirs.length);

    return memberDirs[0];
  }

  /**
   * Adds the data region to every participating VM.
   */
  @SuppressWarnings("serial")
  private void createDataRegions() {
    Host host = Host.getHost(0);
    int numberOfVms = host.getVMCount();

    for (int i = 0; i < numberOfVms; ++i) {
      openCache(host.getVM(i));
    }
  }

  /**
   * Executes a shell command in an external process.
   * 
   * @param command a shell command.
   * @return the exit value of processing the shell command.
   */
  private int execute(String command) throws IOException, InterruptedException {
    final ProcessBuilder builder = new ProcessBuilder(command);
    builder.redirectErrorStream(true);
    final Process process = builder.start();

    /*
     * Consume standard out.
     */
    new Thread(new Runnable() {
      @Override
      public void run() {

        try {
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(process.getInputStream()));
          String line;

          do {
            line = reader.readLine();
            log(line);
          } while (null != line);

          reader.close();
        } catch (IOException e) {
          log("Execute: error while reading standard in: " + e.getMessage());
        }
      }
    }).start();

    /*
     * Consume standard error.
     */
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(process.getErrorStream()));
          String line;

          do {
            line = reader.readLine();
          } while (null != line);

          reader.close();
        } catch (IOException e) {
          log("Execute: error while reading standard error: " + e.getMessage());
        }
      }
    }).start();

    return process.waitFor();
  }

  /**
   * Peforms an operation log restore for a member.
   * 
   * @param backupDir the member's backup directory containing the restore script.
   */
  private void performRestore(File memberDir, File backupDir)
      throws IOException, InterruptedException {
    /*
     * The restore script will not restore if there is an if file in the copy to directory. Remove
     * these files first.
     */
    Collection<File> ifFiles = FileUtils.listFiles(memberDir, new RegexFileFilter(".*\\.if$"),
        DirectoryFileFilter.DIRECTORY);
    for (File file : ifFiles) {
      file.delete();
    }

    /*
     * Remove all operation logs.
     */
    Collection<File> oplogs = FileUtils.listFiles(memberDir, new RegexFileFilter(OPLOG_REGEX),
        DirectoryFileFilter.DIRECTORY);
    for (File file : oplogs) {
      file.delete();
    }

    /*
     * Get a hold of the restore script and make sure it is there.
     */
    File restoreScript = new File(backupDir, "restore.sh");
    if (!restoreScript.exists()) {
      restoreScript = new File(backupDir, "restore.bat");
    }
    assertTrue(restoreScript.exists());

    assertEquals(0, execute(restoreScript.getAbsolutePath()));
  }

  /**
   * Adds an incomplete marker to the baseline backup for a member.
   * 
   * @param vm a distributed system member.
   */
  private void markAsIncomplete(VM vm) throws IOException {
    File backupDir = getBackupDirForMember(getBaselineDir(), getMemberId(vm));
    assertTrue(backupDir.exists());

    File incomplete = new File(backupDir, BackupManager.INCOMPLETE_BACKUP);
    incomplete.createNewFile();
  }

  /**
   * Loads additional data into the test regions.
   */
  private void loadMoreData() {
    Region<Integer, String> fooRegion = getCache().getRegion("fooRegion");

    // Fill our region data
    for (int i = this.dataStart; i < this.dataEnd; ++i) {
      fooRegion.put(i, Integer.toString(i));
    }

    Region<Integer, String> barRegion = getCache().getRegion("barRegion");

    // Fill our region data
    for (int i = this.dataStart; i < this.dataEnd; ++i) {
      barRegion.put(i, Integer.toString(i));
    }

    this.dataStart += DATA_INCREMENT;
    this.dataEnd += DATA_INCREMENT;
  }

  /**
   * Used to confirm valid BackupStatus data. Confirms fix for defect #45657
   * 
   * @param backupStatus contains a list of members that were backed up.
   */
  private void assertBackupStatus(final BackupStatus backupStatus) {
    Map<DistributedMember, Set<PersistentID>> backupMap = backupStatus.getBackedUpDiskStores();
    assertFalse(backupMap.isEmpty());

    for (DistributedMember member : backupMap.keySet()) {
      for (PersistentID id : backupMap.get(member)) {
        assertNotNull(id.getHost());
        assertNotNull(id.getUUID());
        assertNotNull(id.getDirectory());
      }
    }
  }

  /**
   * 1. Add partitioned persistent region to all members. 2. Fills region with data.
   */
  @Override
  public final void postSetUp() throws Exception {
    createDataRegions();
    this.createRegions.run();
    loadMoreData();

    log("Data region created and populated.");
  }

  /**
   * Removes backup directories (and all backup data).
   */
  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    FileUtils.deleteDirectory(getIncremental2Dir());
    FileUtils.deleteDirectory(getIncrementalDir());
    FileUtils.deleteDirectory(getBaselineDir());
  }

  /**
   * This tests the basic features of incremental backup. This means that operation logs that are
   * present in both the baseline and member's disk store should not be copied during the
   * incremental backup. Additionally, the restore script should reference and copy operation logs
   * from the baseline backup.
   */
  @Test
  public void testIncrementalBackup() throws Exception {
    String memberId = getMemberId(Host.getHost(0).getVM(1));

    File memberDir = getVMDir(Host.getHost(0).getVM(1));
    assertNotNull(memberDir);

    // Find all of the member's oplogs in the disk directory (*.crf,*.krf,*.drf)
    Collection<File> memberOplogFiles = FileUtils.listFiles(memberDir,
        new RegexFileFilter(OPLOG_REGEX), DirectoryFileFilter.DIRECTORY);
    assertFalse(memberOplogFiles.isEmpty());

    // Perform a full backup and wait for it to finish
    assertBackupStatus(performBaseline());
    waitForBackup(Host.getHost(0).getVM(1));

    // Find all of the member's oplogs in the baseline (*.crf,*.krf,*.drf)
    Collection<File> memberBaselineOplogs =
        FileUtils.listFiles(getBackupDirForMember(getBaselineDir(), memberId),
            new RegexFileFilter(OPLOG_REGEX), DirectoryFileFilter.DIRECTORY);
    assertFalse(memberBaselineOplogs.isEmpty());

    List<String> memberBaselineOplogNames = new LinkedList<>();
    TransformUtils.transform(memberBaselineOplogs, memberBaselineOplogNames,
        TransformUtils.fileNameTransformer);

    // Peform and incremental backup and wait for it to finish
    loadMoreData(); // Doing this preserves the new oplogs created by the baseline backup
    assertBackupStatus(performIncremental());
    waitForBackup(Host.getHost(0).getVM(1));

    // Find all of the member's oplogs in the incremental (*.crf,*.krf,*.drf)
    Collection<File> memberIncrementalOplogs =
        FileUtils.listFiles(getBackupDirForMember(getIncrementalDir(), memberId),
            new RegexFileFilter(OPLOG_REGEX), DirectoryFileFilter.DIRECTORY);
    assertFalse(memberIncrementalOplogs.isEmpty());

    List<String> memberIncrementalOplogNames = new LinkedList<>();
    TransformUtils.transform(memberIncrementalOplogs, memberIncrementalOplogNames,
        TransformUtils.fileNameTransformer);

    log("BASELINE OPLOGS = " + memberBaselineOplogNames);
    log("INCREMENTAL OPLOGS = " + memberIncrementalOplogNames);

    /*
     * Assert that the incremental backup does not contain baseline operation logs that the member
     * still has copies of.
     */
    for (String oplog : memberBaselineOplogNames) {
      assertFalse(memberIncrementalOplogNames.contains(oplog));
    }

    // Perform a second incremental and wait for it to finish.
    loadMoreData(); // Doing this preserves the new oplogs created by the incremental backup
    assertBackupStatus(performIncremental2());
    waitForBackup(Host.getHost(0).getVM(1));

    Collection<File> memberIncremental2Oplogs =
        FileUtils.listFiles(getBackupDirForMember(getIncremental2Dir(), memberId),
            new RegexFileFilter(OPLOG_REGEX), DirectoryFileFilter.DIRECTORY);
    assertFalse(memberIncremental2Oplogs.isEmpty());

    List<String> memberIncremental2OplogNames = new LinkedList<>();
    TransformUtils.transform(memberIncremental2Oplogs, memberIncremental2OplogNames,
        TransformUtils.fileNameTransformer);

    log("INCREMENTAL 2 OPLOGS = " + memberIncremental2OplogNames);

    /*
     * Assert that the second incremental backup does not contain operation logs copied into the
     * baseline.
     */
    for (String oplog : memberBaselineOplogNames) {
      assertFalse(memberIncremental2OplogNames.contains(oplog));
    }

    /*
     * Also assert that the second incremental backup does not contain operation logs copied into
     * the member's first incremental backup.
     */
    for (String oplog : memberIncrementalOplogNames) {
      assertFalse(memberIncremental2OplogNames.contains(oplog));
    }

    // Shut down our member so we can perform a restore
    PersistentID id = getPersistentID(Host.getHost(0).getVM(1));
    closeCache(Host.getHost(0).getVM(1));

    // Execute the restore
    performRestore(new File(id.getDirectory()),
        getBackupDirForMember(getIncremental2Dir(), memberId));

    /*
     * Collect all of the restored operation logs.
     */
    Collection<File> restoredOplogs = FileUtils.listFiles(new File(id.getDirectory()),
        new RegexFileFilter(OPLOG_REGEX), DirectoryFileFilter.DIRECTORY);
    assertFalse(restoredOplogs.isEmpty());
    List<String> restoredOplogNames = new LinkedList<>();
    TransformUtils.transform(restoredOplogs, restoredOplogNames,
        TransformUtils.fileNameTransformer);

    /*
     * Assert that baseline operation logs have been copied over to the member's disk directory.
     */
    for (String oplog : memberBaselineOplogNames) {
      assertTrue(restoredOplogNames.contains(oplog));
    }

    /*
     * Assert that the incremental operation logs have been copied over to the member's disk
     * directory.
     */
    for (String oplog : memberIncrementalOplogNames) {
      assertTrue(restoredOplogNames.contains(oplog));
    }

    /*
     * Assert that the second incremental operation logs have been copied over to the member's disk
     * directory.
     */
    for (String oplog : memberIncremental2OplogNames) {
      assertTrue(restoredOplogNames.contains(oplog));
    }

    /*
     * Reconnect the member.
     */
    openCache(Host.getHost(0).getVM(1));
  }

  /**
   * Successful if a member performs a full backup when its backup data is not present in the
   * baseline (for whatever reason). This also tests what happens when a member is offline during
   * the baseline backup.
   * 
   * The test is regarded as successful when all of the missing members oplog files are backed up
   * during an incremental backup. This means that the member peformed a full backup because its
   * oplogs were missing in the baseline.
   */
  @Test
  public void testMissingMemberInBaseline() throws Exception {
    // Simulate the missing member by forcing a persistent member
    // to go offline.
    final PersistentID missingMember =
        disconnect(Host.getHost(0).getVM(0), Host.getHost(0).getVM(1));

    /*
     * Perform baseline and make sure that the list of offline disk stores contains our missing
     * member.
     */
    BackupStatus baselineStatus = performBaseline();
    assertBackupStatus(baselineStatus);
    assertNotNull(baselineStatus.getOfflineDiskStores());
    assertEquals(2, baselineStatus.getOfflineDiskStores().size());

    // Find all of the member's oplogs in the missing member's diskstore directory structure
    // (*.crf,*.krf,*.drf)
    Collection<File> missingMemberOplogFiles =
        FileUtils.listFiles(new File(missingMember.getDirectory()),
            new RegexFileFilter(OPLOG_REGEX), DirectoryFileFilter.DIRECTORY);
    assertFalse(missingMemberOplogFiles.isEmpty());

    /*
     * Restart our missing member and make sure it is back online and part of the distributed system
     */
    openCache(Host.getHost(0).getVM(0));

    /*
     * After reconnecting make sure the other members agree that the missing member is back online.
     */
    final Set<PersistentID> missingMembers = new HashSet<>();
    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        missingMembers.clear();
        missingMembers.addAll(getMissingMembers(Host.getHost(0).getVM(1)));
        return !missingMembers.contains(missingMember);
      }

      @Override
      public String description() {
        return "[testMissingMemberInBasline] Wait for missing member.";
      }
    }, 10000, 500, false);

    assertEquals(0, missingMembers.size());

    /*
     * Peform incremental and make sure we have no offline disk stores.
     */
    BackupStatus incrementalStatus = performIncremental();
    assertBackupStatus(incrementalStatus);
    assertNotNull(incrementalStatus.getOfflineDiskStores());
    assertEquals(0, incrementalStatus.getOfflineDiskStores().size());

    // Get the missing member's member id which is different from the PersistentID
    String memberId = getMemberId(Host.getHost(0).getVM(0));
    assertNotNull(memberId);

    // Get list of backed up oplog files in the incremental backup for the missing member
    File incrementalMemberDir = getBackupDirForMember(getIncrementalDir(), memberId);
    Collection<File> backupOplogFiles = FileUtils.listFiles(incrementalMemberDir,
        new RegexFileFilter(OPLOG_REGEX), DirectoryFileFilter.DIRECTORY);
    assertFalse(backupOplogFiles.isEmpty());

    // Transform missing member oplogs to just their file names.
    List<String> missingMemberOplogNames = new LinkedList<>();
    TransformUtils.transform(missingMemberOplogFiles, missingMemberOplogNames,
        TransformUtils.fileNameTransformer);

    // Transform missing member's incremental backup oplogs to just their file names.
    List<String> backupOplogNames = new LinkedList<>();
    TransformUtils.transform(backupOplogFiles, backupOplogNames,
        TransformUtils.fileNameTransformer);

    /*
     * Make sure that the incremental backup for the missing member contains all of the operation
     * logs for that member. This proves that a full backup was performed for that member.
     */
    assertTrue(backupOplogNames.containsAll(missingMemberOplogNames));
  }

  /**
   * Successful if a member performs a full backup if their backup is marked as incomplete in the
   * baseline.
   */
  @Test
  public void testIncompleteInBaseline() throws Exception {
    /*
     * Get the member ID for VM 1 and perform a baseline.
     */
    String memberId = getMemberId(Host.getHost(0).getVM(1));
    assertBackupStatus(performBaseline());

    /*
     * Find all of the member's oplogs in the baseline (*.crf,*.krf,*.drf)
     */
    Collection<File> memberBaselineOplogs =
        FileUtils.listFiles(getBackupDirForMember(getBaselineDir(), memberId),
            new RegexFileFilter(OPLOG_REGEX), DirectoryFileFilter.DIRECTORY);
    assertFalse(memberBaselineOplogs.isEmpty());

    List<String> memberBaselineOplogNames = new LinkedList<>();
    TransformUtils.transform(memberBaselineOplogs, memberBaselineOplogNames,
        TransformUtils.fileNameTransformer);

    // Mark the baseline as incomplete (even though it really isn't)
    markAsIncomplete(Host.getHost(0).getVM(1));

    /*
     * Do an incremental. It should discover that the baseline is incomplete and backup all of the
     * operation logs that are in the baseline.
     */
    assertBackupStatus(performIncremental());

    /*
     * Find all of the member's oplogs in the incremental (*.crf,*.krf,*.drf)
     */
    Collection<File> memberIncrementalOplogs =
        FileUtils.listFiles(getBackupDirForMember(getIncrementalDir(), memberId),
            new RegexFileFilter(OPLOG_REGEX), DirectoryFileFilter.DIRECTORY);
    assertFalse(memberIncrementalOplogs.isEmpty());

    List<String> memberIncrementalOplogNames = new LinkedList<>();
    TransformUtils.transform(memberIncrementalOplogs, memberIncrementalOplogNames,
        TransformUtils.fileNameTransformer);

    /*
     * Assert that all of the baseline operation logs are in the incremental backup. If so, then the
     * incomplete marker was discovered in the baseline by the incremental backup process.
     */
    for (String oplog : memberBaselineOplogNames) {
      assertTrue(memberIncrementalOplogNames.contains(oplog));
    }
  }

  /**
   * Successful if all members perform a full backup when they share the baseline directory and it
   * is missing.
   */
  @Test
  public void testMissingBaseline() throws Exception {
    /*
     * Get the member ID for VM 1 and perform a baseline.
     */
    String memberId = getMemberId(Host.getHost(0).getVM(1));
    assertBackupStatus(performBaseline());

    /*
     * Find all of the member's oplogs in the baseline (*.crf,*.krf,*.drf)
     */
    Collection<File> memberBaselineOplogs =
        FileUtils.listFiles(getBackupDirForMember(getBaselineDir(), memberId),
            new RegexFileFilter(OPLOG_REGEX), DirectoryFileFilter.DIRECTORY);
    assertFalse(memberBaselineOplogs.isEmpty());

    List<String> memberBaselineOplogNames = new LinkedList<>();
    TransformUtils.transform(memberBaselineOplogs, memberBaselineOplogNames,
        TransformUtils.fileNameTransformer);

    /*
     * Custom incremental backup callable that retrieves the current baseline before deletion.
     */
    SerializableCallable callable = new SerializableCallable("Backup all members.") {
      private final File baselineDir = getBaselineBackupDir();

      @Override
      public Object call() {
        AdminDistributedSystem adminDS = null;
        try {
          DistributedSystemConfig config =
              AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          return adminDS.backupAllMembers(getIncrementalDir(), this.baselineDir);

        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    };

    /*
     * Do an incremental after deleting the baseline. It should discover that the baseline is gone
     * and backup all of the operation logs that are in the baseline.
     */
    FileUtils.deleteDirectory(getBaselineDir());
    Host.getHost(0).getVM(1).invoke(callable);

    /*
     * Find all of the member's oplogs in the incremental (*.crf,*.krf,*.drf)
     */
    Collection<File> memberIncrementalOplogs =
        FileUtils.listFiles(getBackupDirForMember(getIncrementalDir(), memberId),
            new RegexFileFilter(OPLOG_REGEX), DirectoryFileFilter.DIRECTORY);
    assertFalse(memberIncrementalOplogs.isEmpty());

    List<String> memberIncrementalOplogNames = new LinkedList<>();
    TransformUtils.transform(memberIncrementalOplogs, memberIncrementalOplogNames,
        TransformUtils.fileNameTransformer);

    /*
     * Assert that all of the baseline operation logs are in the incremental backup. If so, then the
     * missing baseline was discovered by the incremental backup process.
     */
    for (String oplog : memberBaselineOplogNames) {
      assertTrue(memberIncrementalOplogNames.contains(oplog));
    }
  }

  /**
   * Successful if a user deployed jar file is included as part of the backup.
   */
  @Test
  public void testBackupUserDeployedJarFiles() throws Exception {
    final String jarName = "BackupJarDeploymentDUnit";
    final String jarNameRegex = ".*" + jarName + ".*";
    final ClassBuilder classBuilder = new ClassBuilder();
    final byte[] classBytes = classBuilder.createJarFromName(jarName);

    VM vm0 = Host.getHost(0).getVM(0);

    /*
     * Deploy a "dummy" jar to the VM.
     */
    File deployedJarFile = vm0.invoke(() -> {
      DeployedJar deployedJar =
          ClassPathLoader.getLatest().getJarDeployer().deploy(jarName, classBytes);
      return deployedJar.getFile();
    });

    assertTrue(deployedJarFile.exists());
    /*
     * Perform backup. Make sure it is successful.
     */
    assertBackupStatus(baseline(vm0));

    /*
     * Make sure the user deployed jar is part of the backup.
     */
    Collection<File> memberDeployedJarFiles =
        FileUtils.listFiles(getBackupDirForMember(getBaselineDir(), getMemberId(vm0)),
            new RegexFileFilter(jarNameRegex), DirectoryFileFilter.DIRECTORY);
    assertFalse(memberDeployedJarFiles.isEmpty());

    // Shut down our member so we can perform a restore
    PersistentID id = getPersistentID(vm0);
    closeCache(vm0);

    /*
     * Get the VM's user directory.
     */
    final String vmDir = vm0.invoke(() -> System.getProperty("user.dir"));

    File backupDir = getBackupDirForMember(getBaselineDir(), getMemberId(vm0));

    vm0.bounce();

    /*
     * Cleanup "dummy" jar from file system.
     */
    Pattern pattern = Pattern.compile('^' + jarName + ".*#\\d++$");
    deleteMatching(new File("."), pattern);

    // Execute the restore
    performRestore(new File(id.getDirectory()), backupDir);

    /*
     * Make sure the user deployed jar is part of the restore.
     */
    Collection<File> restoredJars = FileUtils.listFiles(new File(vmDir),
        new RegexFileFilter(jarNameRegex), DirectoryFileFilter.DIRECTORY);
    assertFalse(restoredJars.isEmpty());
    List<String> restoredJarNames = new LinkedList<>();
    TransformUtils.transform(memberDeployedJarFiles, restoredJarNames,
        TransformUtils.fileNameTransformer);
    for (String name : restoredJarNames) {
      assertTrue(name.contains(jarName));
    }

    // Restart the member
    openCache(vm0);

    /*
     * Remove the "dummy" jar from the VM.
     */
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        for (DeployedJar jarClassLoader : ClassPathLoader.getLatest().getJarDeployer()
            .findDeployedJars()) {
          if (jarClassLoader.getJarName().startsWith(jarName)) {
            ClassPathLoader.getLatest().getJarDeployer().undeploy(jarClassLoader.getJarName());
          }
        }
        return null;
      }
    });

    /*
     * Cleanup "dummy" jar from file system.
     */
    pattern = Pattern.compile('^' + jarName + ".*#\\d++$");
    deleteMatching(new File(vmDir), pattern);
  }

  private void deleteMatching(File dir, final Pattern pattern) throws IOException {
    Collection<File> files =
        FileUtils.listFiles(dir, new RegexFileFilter(pattern), DirectoryFileFilter.DIRECTORY);
    for (File file : files) {
      Files.delete(file.toPath());
    }
  }
}
