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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DeployedJar;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.cache.persistence.BackupInspector;
import org.apache.geode.internal.cache.persistence.RestoreScript;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;

/**
 * This class manages the state an logic to backup a single cache.
 */
public class BackupManager implements MembershipListener {
  private static final Logger logger = LogService.getLogger(BackupManager.class);

  static final String INCOMPLETE_BACKUP_FILE = "INCOMPLETE_BACKUP_FILE";

  private static final String BACKUP_DIR_PREFIX = "dir";
  private static final String README_FILE = "README_FILE.txt";
  private static final String DATA_STORES_DIRECTORY = "diskstores";
  private static final String USER_FILES = "user";
  private static final String CONFIG_DIRECTORY = "config";

  private final Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStore = new HashMap<>();
  private final RestoreScript restoreScript = new RestoreScript();
  private final InternalDistributedMember sender;
  private final InternalCache cache;
  private final CountDownLatch allowDestroys = new CountDownLatch(1);
  private volatile boolean isCancelled = false;

  public BackupManager(InternalDistributedMember sender, InternalCache gemFireCache) {
    this.sender = sender;
    this.cache = gemFireCache;
  }

  public void validateRequestingAdmin() {
    // We need to watch for pure admin guys that depart. this allMembershipListener set
    // looks like it should receive those events.
    Set allIds = getDistributionManager().addAllMembershipListenerAndGetAllIds(this);
    if (!allIds.contains(sender)) {
      cleanup();
      throw new IllegalStateException("The admin member requesting a backup has already departed");
    }
  }

  public HashSet<PersistentID> prepareForBackup() {
    HashSet<PersistentID> persistentIds = new HashSet<>();
    for (DiskStore store : cache.listDiskStoresIncludingRegionOwned()) {
      DiskStoreImpl storeImpl = (DiskStoreImpl) store;
      storeImpl.lockStoreBeforeBackup();
      if (storeImpl.hasPersistedData()) {
        persistentIds.add(storeImpl.getPersistentID());
        storeImpl.getStats().startBackup();
      }
    }
    return persistentIds;
  }

  public HashSet<PersistentID> doBackup(File targetDir, File baselineDir, boolean abort)
      throws IOException {
    try {
      if (abort) {
        return new HashSet<>();
      }
      HashSet<PersistentID> persistentIds = new HashSet<>();
      File backupDir = getBackupDir(targetDir);

      // Make sure our baseline is okay for this member
      baselineDir = checkBaseline(baselineDir);

      // Create an inspector for the baseline backup
      BackupInspector inspector =
          (baselineDir == null ? null : BackupInspector.createInspector(baselineDir));

      File storesDir = new File(backupDir, DATA_STORES_DIRECTORY);
      Collection<DiskStore> diskStores = cache.listDiskStoresIncludingRegionOwned();
      Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStore = new HashMap<>();

      boolean foundPersistentData = false;
      for (DiskStore store : diskStores) {
        DiskStoreImpl diskStore = (DiskStoreImpl) store;
        if (diskStore.hasPersistedData()) {
          if (!foundPersistentData) {
            createBackupDir(backupDir);
            foundPersistentData = true;
          }
          File diskStoreDir = new File(storesDir, getBackupDirName(diskStore));
          diskStoreDir.mkdir();
          DiskStoreBackup backup = startDiskStoreBackup(diskStore, diskStoreDir, inspector);
          backupByDiskStore.put(diskStore, backup);
        }
        diskStore.releaseBackupLock();
      }

      allowDestroys.countDown();

      for (Map.Entry<DiskStoreImpl, DiskStoreBackup> entry : backupByDiskStore.entrySet()) {
        DiskStoreImpl diskStore = entry.getKey();
        completeBackup(diskStore, entry.getValue());
        diskStore.getStats().endBackup();
        persistentIds.add(diskStore.getPersistentID());
      }

      if (!backupByDiskStore.isEmpty()) {
        completeRestoreScript(backupDir);
      }

      return persistentIds;

    } finally {
      cleanup();
    }
  }

  public void abort() {
    cleanup();
  }

  private DM getDistributionManager() {
    return cache.getInternalDistributedSystem().getDistributionManager();
  }

  private void cleanup() {
    isCancelled = true;
    allowDestroys.countDown();
    releaseBackupLocks();
    getDistributionManager().removeAllMembershipListener(this);
    cache.clearBackupManager();
  }

  private void releaseBackupLocks() {
    for (DiskStore store : cache.listDiskStoresIncludingRegionOwned()) {
      ((DiskStoreImpl) store).releaseBackupLock();
    }
  }

  /**
   * Returns the memberId directory for this member in the baseline. The memberId may have changed
   * if this member has been restarted since the last backup.
   * 
   * @param baselineParentDir parent directory of last backup.
   * @return null if the baseline for this member could not be located.
   */
  private File findBaselineForThisMember(File baselineParentDir) {
    File baselineDir = null;

    /*
     * Find the first matching DiskStoreId directory for this member.
     */
    for (DiskStore diskStore : cache.listDiskStoresIncludingRegionOwned()) {
      File[] matchingFiles = baselineParentDir
          .listFiles((file, name) -> name.endsWith(getBackupDirName((DiskStoreImpl) diskStore)));
      // We found it? Good. Set this member's baseline to the backed up disk store's member dir (two
      // levels up).
      if (null != matchingFiles && matchingFiles.length > 0)
        baselineDir = matchingFiles[0].getParentFile().getParentFile();
    }
    return baselineDir;
  }

  /**
   * Performs a sanity check on the baseline directory for incremental backups. If a baseline
   * directory exists for the member and there is no INCOMPLETE_BACKUP_FILE file then return the
   * data stores directory for this member.
   * 
   * @param baselineParentDir a previous backup directory. This is used with the incremental backup
   *        option. May be null if the user specified a full backup.
   * @return null if the backup is to be a full backup otherwise return the data store directory in
   *         the previous backup for this member (if incremental).
   */
  private File checkBaseline(File baselineParentDir) throws IOException {
    File baselineDir = null;

    if (null != baselineParentDir) {
      // Start by looking for this memberId
      baselineDir = getBackupDir(baselineParentDir);

      if (!baselineDir.exists()) {
        // hmmm, did this member have a restart?
        // Determine which member dir might be a match for us
        baselineDir = findBaselineForThisMember(baselineParentDir);
      }

      if (null != baselineDir) {
        // check for existence of INCOMPLETE_BACKUP_FILE file
        File incompleteBackup = new File(baselineDir, INCOMPLETE_BACKUP_FILE);
        if (incompleteBackup.exists()) {
          baselineDir = null;
        }
      }
    }

    return baselineDir;
  }

  private void completeRestoreScript(File backupDir) throws IOException {
    backupConfigFiles(restoreScript, backupDir);
    backupUserFiles(restoreScript, backupDir);
    backupDeployedJars(restoreScript, backupDir);
    restoreScript.generate(backupDir);
    File incompleteFile = new File(backupDir, INCOMPLETE_BACKUP_FILE);
    if (!incompleteFile.delete()) {
      throw new IOException("Could not delete file " + INCOMPLETE_BACKUP_FILE);
    }
  }

  /**
   * Copy the oplogs to the backup directory. This is the final step of the backup process. The
   * oplogs we copy are defined in the startDiskStoreBackup method.
   */
  private void completeBackup(DiskStoreImpl diskStore, DiskStoreBackup backup) throws IOException {
    if (backup == null) {
      return;
    }
    try {
      // Wait for oplogs to be unpreblown before backing them up.
      diskStore.waitForDelayedWrites();

      // Backup all of the oplogs
      for (Oplog oplog : backup.getPendingBackup()) {
        if (isCancelled()) {
          break;
        }
        // Copy theoplog to the destination directory
        int index = oplog.getDirectoryHolder().getArrayIndex();
        File backupDir = getBackupDir(backup.getTargetDir(), index);
        // TODO prpersist - We could probably optimize this to *move* the files
        // that we know are supposed to be deleted.
        oplog.copyTo(backupDir);

        // Allow the oplog to be deleted, and process any pending delete
        backup.backupFinished(oplog);
      }
    } finally {
      backup.cleanup();
    }
  }

  /**
   * Returns the dir name used to back up this DiskStore's directories under. The name is a
   * concatenation of the disk store name and id.
   */
  private String getBackupDirName(DiskStoreImpl diskStore) {
    String name = diskStore.getName();

    if (name == null) {
      name = GemFireCacheImpl.getDefaultDiskStoreName();
    }

    return (name + "_" + diskStore.getDiskStoreID().toString());
  }

  /**
   * Start the backup process. This is the second step of the backup process. In this method, we
   * define the data we're backing up by copying the init file and rolling to the next file. After
   * this method returns operations can proceed as normal, except that we don't remove oplogs.
   */
  private DiskStoreBackup startDiskStoreBackup(DiskStoreImpl diskStore, File targetDir,
      BackupInspector baselineInspector) throws IOException {
    diskStore.getBackupLock().setBackupThread();
    DiskStoreBackup backup = null;
    boolean done = false;
    try {
      for (;;) {
        Oplog childOplog = diskStore.getPersistentOplogSet().getChild();
        if (childOplog == null) {
          backup = new DiskStoreBackup(new Oplog[0], targetDir);
          backupByDiskStore.put(diskStore, backup);
          break;
        }

        // Get an appropriate lock object for each set of oplogs.
        Object childLock = childOplog.lock;

        // TODO - We really should move this lock into the disk store, but
        // until then we need to do this magic to make sure we're actually
        // locking the latest child for both types of oplogs

        // This ensures that all writing to disk is blocked while we are
        // creating the snapshot
        synchronized (childLock) {
          if (diskStore.getPersistentOplogSet().getChild() != childOplog) {
            continue;
          }

          if (logger.isDebugEnabled()) {
            logger.debug("snapshotting oplogs for disk store {}", diskStore.getName());
          }

          createDiskStoreBackupDirs(diskStore, targetDir);

          restoreScript.addExistenceTest(diskStore.getDiskInitFile().getIFFile());

          // Contains all oplogs that will backed up
          Oplog[] allOplogs = null;

          // Incremental backup so filter out oplogs that have already been
          // backed up
          if (null != baselineInspector) {
            Map<File, File> baselineCopyMap = new HashMap<>();
            allOplogs = filterBaselineOplogs(diskStore, baselineInspector, baselineCopyMap);
            restoreScript.addBaselineFiles(baselineCopyMap);
          } else {
            allOplogs = diskStore.getAllOplogsForBackup();
          }

          // mark all oplogs as being backed up. This will
          // prevent the oplogs from being deleted
          backup = new DiskStoreBackup(allOplogs, targetDir);
          backupByDiskStore.put(diskStore, backup);

          // copy the init file
          File firstDir = getBackupDir(targetDir, diskStore.getInforFileDirIndex());
          diskStore.getDiskInitFile().copyTo(firstDir);
          diskStore.getPersistentOplogSet().forceRoll(null);

          if (logger.isDebugEnabled()) {
            logger.debug("done snaphotting for disk store {}", diskStore.getName());
          }
          break;
        }
      }
      done = true;
    } finally {
      if (!done) {
        if (backup != null) {
          backupByDiskStore.remove(diskStore);
          backup.cleanup();
        }
      }
    }
    return backup;
  }

  private void createDiskStoreBackupDirs(DiskStoreImpl diskStore, File targetDir)
      throws IOException {
    // Create the directories for this disk store
    DirectoryHolder[] directories = diskStore.getDirectoryHolders();
    for (int i = 0; i < directories.length; i++) {
      File dir = getBackupDir(targetDir, i);
      if (!dir.mkdirs()) {
        throw new IOException("Could not create directory " + dir);
      }
      restoreScript.addFile(directories[i].getDir(), dir);
    }
  }

  /**
   * Filters and returns the current set of oplogs that aren't already in the baseline for
   * incremental backup
   *
   * @param baselineInspector the inspector for the previous backup.
   * @param baselineCopyMap this will be populated with baseline oplogs Files that will be used in
   *        the restore script.
   * @return an array of Oplogs to be copied for an incremental backup.
   */
  private Oplog[] filterBaselineOplogs(DiskStoreImpl diskStore, BackupInspector baselineInspector,
      Map<File, File> baselineCopyMap) throws IOException {
    File baselineDir =
        new File(baselineInspector.getBackupDir(), BackupManager.DATA_STORES_DIRECTORY);
    baselineDir = new File(baselineDir, getBackupDirName(diskStore));

    // Find all of the member's diskstore oplogs in the member's baseline
    // diskstore directory structure (*.crf,*.krf,*.drf)
    Collection<File> baselineOplogFiles =
        FileUtils.listFiles(baselineDir, new String[] {"krf", "drf", "crf"}, true);
    // Our list of oplogs to copy (those not already in the baseline)
    List<Oplog> oplogList = new LinkedList<>();

    // Total list of member oplogs
    Oplog[] allOplogs = diskStore.getAllOplogsForBackup();

    /*
     * Loop through operation logs and see if they are already part of the baseline backup.
     */
    for (Oplog log : allOplogs) {
      // See if they are backed up in the current baseline
      Map<File, File> oplogMap = log.mapBaseline(baselineOplogFiles);

      // No? Then see if they were backed up in previous baselines
      if (oplogMap.isEmpty() && baselineInspector.isIncremental()) {
        Set<String> matchingOplogs =
            log.gatherMatchingOplogFiles(baselineInspector.getIncrementalOplogFileNames());
        if (!matchingOplogs.isEmpty()) {
          for (String matchingOplog : matchingOplogs) {
            oplogMap.put(new File(baselineInspector.getCopyFromForOplogFile(matchingOplog)),
                new File(baselineInspector.getCopyToForOplogFile(matchingOplog)));
          }
        }
      }

      if (oplogMap.isEmpty()) {
        /*
         * These are fresh operation log files so lets back them up.
         */
        oplogList.add(log);
      } else {
        /*
         * These have been backed up before so lets just add their entries from the previous backup
         * or restore script into the current one.
         */
        baselineCopyMap.putAll(oplogMap);
      }
    }

    // Convert the filtered oplog list to an array
    return oplogList.toArray(new Oplog[oplogList.size()]);
  }

  private File getBackupDir(File targetDir, int index) {
    return new File(targetDir, BACKUP_DIR_PREFIX + index);
  }

  private void backupConfigFiles(RestoreScript restoreScript, File backupDir) throws IOException {
    File configBackupDir = new File(backupDir, CONFIG_DIRECTORY);
    configBackupDir.mkdirs();
    URL url = cache.getCacheXmlURL();
    if (url != null) {
      File cacheXMLBackup =
          new File(configBackupDir, DistributionConfig.DEFAULT_CACHE_XML_FILE.getName());
      FileUtils.copyFile(new File(cache.getCacheXmlURL().getFile()), cacheXMLBackup);
    }

    URL propertyURL = DistributedSystem.getPropertiesFileURL();
    if (propertyURL != null) {
      File propertyBackup =
          new File(configBackupDir, DistributionConfig.GEMFIRE_PREFIX + "properties");
      FileUtils.copyFile(new File(DistributedSystem.getPropertiesFile()), propertyBackup);
    }

    // TODO: should the gfsecurity.properties file be backed up?
  }

  private void backupUserFiles(RestoreScript restoreScript, File backupDir) throws IOException {
    List<File> backupFiles = cache.getBackupFiles();
    File userBackupDir = new File(backupDir, USER_FILES);
    if (!userBackupDir.exists()) {
      userBackupDir.mkdir();
    }
    for (File original : backupFiles) {
      if (original.exists()) {
        original = original.getAbsoluteFile();
        File dest = new File(userBackupDir, original.getName());
        if (original.isDirectory()) {
          FileUtils.copyDirectory(original, dest);
        } else {
          FileUtils.copyFile(original, dest);
        }
        restoreScript.addExistenceTest(original);
        restoreScript.addFile(original, dest);
      }
    }
  }

  /**
   * Copies user deployed jars to the backup directory.
   * 
   * @param restoreScript Used to restore from this backup.
   * @param backupDir The backup directory for this member.
   * @throws IOException one or more of the jars did not successfully copy.
   */
  private void backupDeployedJars(RestoreScript restoreScript, File backupDir) throws IOException {
    JarDeployer deployer = null;

    try {
      /*
       * Suspend any user deployed jar file updates during this backup.
       */
      deployer = ClassPathLoader.getLatest().getJarDeployer();
      deployer.suspendAll();

      List<DeployedJar> jarList = deployer.findDeployedJars();
      if (!jarList.isEmpty()) {
        File userBackupDir = new File(backupDir, USER_FILES);
        if (!userBackupDir.exists()) {
          userBackupDir.mkdir();
        }

        for (DeployedJar loader : jarList) {
          File source = new File(loader.getFileCanonicalPath());
          File dest = new File(userBackupDir, source.getName());
          if (source.isDirectory()) {
            FileUtils.copyDirectory(source, dest);
          } else {
            FileUtils.copyFile(source, dest);
          }
          restoreScript.addFile(source, dest);
        }
      }
    } finally {
      /*
       * Re-enable user deployed jar file updates.
       */
      if (null != deployer) {
        deployer.resumeAll();
      }
    }
  }

  private File getBackupDir(File targetDir) throws IOException {
    InternalDistributedMember memberId =
        cache.getInternalDistributedSystem().getDistributedMember();
    String vmId = memberId.toString();
    vmId = cleanSpecialCharacters(vmId);
    return new File(targetDir, vmId);
  }

  private void createBackupDir(File backupDir) throws IOException {
    if (backupDir.exists()) {
      throw new IOException("Backup directory " + backupDir.getAbsolutePath() + " already exists.");
    }

    if (!backupDir.mkdirs()) {
      throw new IOException("Could not create directory: " + backupDir);
    }

    File incompleteFile = new File(backupDir, INCOMPLETE_BACKUP_FILE);
    if (!incompleteFile.createNewFile()) {
      throw new IOException("Could not create file: " + incompleteFile);
    }

    File readme = new File(backupDir, README_FILE);
    FileOutputStream fos = new FileOutputStream(readme);

    try {
      String text = LocalizedStrings.BackupManager_README.toLocalizedString();
      fos.write(text.getBytes());
    } finally {
      fos.close();
    }
  }

  private String cleanSpecialCharacters(String string) {
    return string.replaceAll("[^\\w]+", "_");
  }

  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    cleanup();
  }

  public void memberJoined(InternalDistributedMember id) {}

  public void quorumLost(Set<InternalDistributedMember> failures,
      List<InternalDistributedMember> remaining) {}

  public void memberSuspect(InternalDistributedMember id, InternalDistributedMember whoSuspected,
      String reason) {}

  public void waitForBackup() {
    try {
      allowDestroys.await();
    } catch (InterruptedException e) {
      throw new InternalGemFireError(e);
    }
  }

  public boolean isCancelled() {
    return isCancelled;
  }

  public DiskStoreBackup getBackupForDiskStore(DiskStoreImpl diskStore) {
    return backupByDiskStore.get(diskStore);
  }
}
