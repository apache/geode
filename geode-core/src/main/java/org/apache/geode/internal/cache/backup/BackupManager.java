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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DeployedJar;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.cache.DirectoryHolder;
import org.apache.geode.internal.cache.DiskStoreBackup;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.Oplog;
import org.apache.geode.internal.logging.LogService;

/**
 * This class manages the state an logic to backup a single cache.
 */
public class BackupManager {
  private static final Logger logger = LogService.getLogger();

  static final String INCOMPLETE_BACKUP_FILE = "INCOMPLETE_BACKUP_FILE";

  private static final String BACKUP_DIR_PREFIX = "dir";
  private static final String DATA_STORES_DIRECTORY = "diskstores";
  public static final String DATA_STORES_TEMPORARY_DIRECTORY = "backupTemp_";
  private static final String USER_FILES = "user";
  private static final String CONFIG_DIRECTORY = "config";

  private final MembershipListener membershipListener = new BackupMembershipListener();
  private final Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStore = new HashMap<>();
  private final RestoreScript restoreScript = new RestoreScript();
  private final InternalDistributedMember sender;
  private final InternalCache cache;
  private final CountDownLatch allowDestroys = new CountDownLatch(1);
  private final BackupDefinition backupDefinition = new BackupDefinition();
  private final String diskStoreDirectoryName;
  private volatile boolean isCancelled = false;
  private Path tempDirectory;
  private final Map<DiskStore, Map<DirectoryHolder, Path>> diskStoreDirTempDirsByDiskStore =
      new HashMap<>();

  public BackupManager(InternalDistributedMember sender, InternalCache gemFireCache) {
    this.sender = sender;
    this.cache = gemFireCache;
    diskStoreDirectoryName = DATA_STORES_TEMPORARY_DIRECTORY + System.currentTimeMillis();
  }

  public void validateRequestingAdmin() {
    // We need to watch for pure admin guys that depart. this allMembershipListener set
    // looks like it should receive those events.
    Set allIds = getDistributionManager().addAllMembershipListenerAndGetAllIds(membershipListener);
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
      tempDirectory = Files.createTempDirectory("backup_" + System.currentTimeMillis());
      File backupDir = getBackupDir(targetDir);

      // Make sure our baseline is okay for this member, then create inspector for baseline backup
      baselineDir = checkBaseline(baselineDir);
      BackupInspector inspector =
          (baselineDir == null ? null : BackupInspector.createInspector(baselineDir));
      File storesDir = new File(backupDir, DATA_STORES_DIRECTORY);
      Collection<DiskStore> diskStores = cache.listDiskStoresIncludingRegionOwned();

      Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStores =
          startDiskStoreBackups(inspector, storesDir, diskStores);
      allowDestroys.countDown();
      HashSet<PersistentID> persistentIds = finishDiskStoreBackups(backupByDiskStores);

      if (!backupByDiskStores.isEmpty()) {
        backupAdditionalFiles(backupDir);
        backupDefinition.setRestoreScript(restoreScript);
      }

      if (!backupByDiskStores.isEmpty()) {
        // TODO: allow different stategies...
        BackupDestination backupDestination = new FileSystemBackupDestination(backupDir.toPath());
        backupDestination.backupFiles(backupDefinition);
      }

      return persistentIds;

    } finally {
      cleanup();
    }
  }

  private HashSet<PersistentID> finishDiskStoreBackups(
      Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStores) throws IOException {
    HashSet<PersistentID> persistentIds = new HashSet<>();
    for (Map.Entry<DiskStoreImpl, DiskStoreBackup> entry : backupByDiskStores.entrySet()) {
      DiskStoreImpl diskStore = entry.getKey();
      completeBackup(diskStore, entry.getValue());
      diskStore.getStats().endBackup();
      persistentIds.add(diskStore.getPersistentID());
    }
    return persistentIds;
  }

  private Map<DiskStoreImpl, DiskStoreBackup> startDiskStoreBackups(BackupInspector inspector,
      File storesDir, Collection<DiskStore> diskStores) throws IOException {
    Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStore = new HashMap<>();

    for (DiskStore store : diskStores) {
      DiskStoreImpl diskStore = (DiskStoreImpl) store;
      if (diskStore.hasPersistedData()) {
        File diskStoreDir = new File(storesDir, getBackupDirName(diskStore));
        DiskStoreBackup backup = startDiskStoreBackup(diskStore, diskStoreDir, inspector);
        backupByDiskStore.put(diskStore, backup);
      }
      diskStore.releaseBackupLock();
    }
    return backupByDiskStore;
  }

  public void abort() {
    cleanup();
  }

  public boolean isCancelled() {
    return isCancelled;
  }

  public void waitForBackup() {
    try {
      allowDestroys.await();
    } catch (InterruptedException e) {
      throw new InternalGemFireError(e);
    }
  }

  private DM getDistributionManager() {
    return cache.getInternalDistributedSystem().getDistributionManager();
  }

  private void cleanup() {
    isCancelled = true;
    allowDestroys.countDown();
    cleanupTemporaryFiles();
    releaseBackupLocks();
    getDistributionManager().removeAllMembershipListener(membershipListener);
    cache.clearBackupManager();
  }

  private void cleanupTemporaryFiles() {
    if (tempDirectory != null) {
      try {
        FileUtils.deleteDirectory(tempDirectory.toFile());
      } catch (IOException e) {
        logger.warn("Unable to delete temporary directory created during backup, " + tempDirectory,
            e);
      }
    }
    for (Map<DirectoryHolder, Path> diskStoreDirToTempDirMap : diskStoreDirTempDirsByDiskStore
        .values()) {
      for (Path tempDir : diskStoreDirToTempDirMap.values()) {
        try {
          FileUtils.deleteDirectory(tempDir.toFile());
        } catch (IOException e) {
          logger.warn("Unable to delete temporary directory created during backup, " + tempDir, e);
        }
      }
    }
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

    // Find the first matching DiskStoreId directory for this member.
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

  private void backupAdditionalFiles(File backupDir) throws IOException {
    backupConfigFiles();
    backupUserFiles(backupDir);
    backupDeployedJars(backupDir);
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
        copyOplog(diskStore, tempDirectory.toFile(), oplog);

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
        Object childLock = childOplog.getLock();

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

          addDiskStoreDirectoriesToRestoreScript(diskStore, targetDir);

          restoreScript.addExistenceTest(diskStore.getDiskInitFile().getIFFile());

          // Contains all oplogs that will backed up

          // Incremental backup so filter out oplogs that have already been
          // backed up
          Oplog[] allOplogs;
          if (null != baselineInspector) {
            allOplogs = filterBaselineOplogs(diskStore, baselineInspector);
          } else {
            allOplogs = diskStore.getAllOplogsForBackup();
          }

          // mark all oplogs as being backed up. This will
          // prevent the oplogs from being deleted
          backup = new DiskStoreBackup(allOplogs, targetDir);
          backupByDiskStore.put(diskStore, backup);


          // TODO cleanup new location definition code
          /*
           * Path diskstoreDir = getBackupDir(tempDir.toFile(),
           * diskStore.getInforFileDirIndex()).toPath(); Files.createDirectories(diskstoreDir);
           */
          backupDiskInitFile(diskStore, tempDirectory);
          diskStore.getPersistentOplogSet().forceRoll(null);

          if (logger.isDebugEnabled()) {
            logger.debug("done backing up disk store {}", diskStore.getName());
          }
          break;
        }
      }
      done = true;
    } finally {
      if (!done && backup != null) {
        backupByDiskStore.remove(diskStore);
        backup.cleanup();
      }
    }
    return backup;
  }

  private void backupDiskInitFile(DiskStoreImpl diskStore, Path tempDir) throws IOException {
    File diskInitFile = diskStore.getDiskInitFile().getIFFile();
    String subDir = Integer.toString(diskStore.getInforFileDirIndex());
    Files.createDirectories(tempDir.resolve(subDir));
    Files.copy(diskInitFile.toPath(), tempDir.resolve(subDir).resolve(diskInitFile.getName()),
        StandardCopyOption.COPY_ATTRIBUTES);
    backupDefinition.addDiskInitFile(diskStore,
        tempDir.resolve(subDir).resolve(diskInitFile.getName()));
  }

  private void addDiskStoreDirectoriesToRestoreScript(DiskStoreImpl diskStore, File targetDir) {
    DirectoryHolder[] directories = diskStore.getDirectoryHolders();
    for (int i = 0; i < directories.length; i++) {
      File backupDir = getBackupDir(targetDir, i);
      restoreScript.addFile(directories[i].getDir(), backupDir);
    }
  }

  /**
   * Filters and returns the current set of oplogs that aren't already in the baseline for
   * incremental backup
   *
   * @param baselineInspector the inspector for the previous backup.
   * @return an array of Oplogs to be copied for an incremental backup.
   */
  private Oplog[] filterBaselineOplogs(DiskStoreImpl diskStore, BackupInspector baselineInspector) {
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

    // Loop through operation logs and see if they are already part of the baseline backup.
    for (Oplog log : allOplogs) {
      // See if they are backed up in the current baseline
      Map<File, File> oplogMap = log.mapBaseline(baselineOplogFiles);

      // No? Then see if they were backed up in previous baselines
      if (oplogMap.isEmpty() && baselineInspector.isIncremental()) {
        oplogMap = addBaselineOplogToRestoreScript(baselineInspector, log);
      }

      if (oplogMap.isEmpty()) {
        // These are fresh operation log files so lets back them up.
        oplogList.add(log);
      } else {
        /*
         * These have been backed up before so lets just add their entries from the previous backup
         * or restore script into the current one.
         */
        restoreScript.addBaselineFiles(oplogMap);
      }
    }

    // Convert the filtered oplog list to an array
    return oplogList.toArray(new Oplog[oplogList.size()]);
  }

  private Map<File, File> addBaselineOplogToRestoreScript(BackupInspector baselineInspector,
      Oplog log) {
    Map<File, File> oplogMap = new HashMap<>();
    Set<String> matchingOplogs =
        log.gatherMatchingOplogFiles(baselineInspector.getIncrementalOplogFileNames());
    for (String matchingOplog : matchingOplogs) {
      oplogMap.put(new File(baselineInspector.getCopyFromForOplogFile(matchingOplog)),
          new File(baselineInspector.getCopyToForOplogFile(matchingOplog)));
    }
    return oplogMap;
  }

  private File getBackupDir(File targetDir, int index) {
    return new File(targetDir, BACKUP_DIR_PREFIX + index);
  }

  private void backupConfigFiles() throws IOException {
    Files.createDirectories(tempDirectory.resolve(CONFIG_DIRECTORY));
    addConfigFileToBackup(cache.getCacheXmlURL());
    addConfigFileToBackup(DistributedSystem.getPropertiesFileURL());
    // TODO: should the gfsecurity.properties file be backed up?
  }

  private void addConfigFileToBackup(URL fileUrl) throws IOException {
    if (fileUrl != null) {
      try {
        Path source = Paths.get(fileUrl.toURI());
        Path destination = tempDirectory.resolve(CONFIG_DIRECTORY).resolve(source.getFileName());
        Files.copy(source, destination, StandardCopyOption.COPY_ATTRIBUTES);
        backupDefinition.addConfigFileToBackup(destination);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }
  }

  private void backupUserFiles(File backupDir) throws IOException {
    Files.createDirectories(tempDirectory.resolve(USER_FILES));
    List<File> backupFiles = cache.getBackupFiles();
    File userBackupDir = new File(backupDir, USER_FILES);
    for (File original : backupFiles) {
      if (original.exists()) {
        original = original.getAbsoluteFile();
        Path destination = tempDirectory.resolve(USER_FILES).resolve(original.getName());
        if (original.isDirectory()) {
          FileUtils.copyDirectory(original, destination.toFile());
        } else {
          Files.copy(original.toPath(), destination, StandardCopyOption.COPY_ATTRIBUTES);
        }
        backupDefinition.addUserFilesToBackup(destination);
        File restoreScriptDestination = new File(userBackupDir, original.getName());
        restoreScript.addUserFile(original, restoreScriptDestination);
      }
    }
  }

  /**
   * Copies user deployed jars to the backup directory.
   *
   * @param backupDir The backup directory for this member.
   * @throws IOException one or more of the jars did not successfully copy.
   */
  private void backupDeployedJars(File backupDir) throws IOException {
    JarDeployer deployer = null;

    try {
      // Suspend any user deployed jar file updates during this backup.
      deployer = ClassPathLoader.getLatest().getJarDeployer();
      deployer.suspendAll();

      List<DeployedJar> jarList = deployer.findDeployedJars();
      if (!jarList.isEmpty()) {
        File userBackupDir = new File(backupDir, USER_FILES);

        for (DeployedJar jar : jarList) {
          File source = new File(jar.getFileCanonicalPath());
          String sourceFileName = source.getName();
          Path destination = tempDirectory.resolve(USER_FILES).resolve(sourceFileName);
          Files.copy(source.toPath(), destination, StandardCopyOption.COPY_ATTRIBUTES);
          backupDefinition.addDeployedJarToBackup(destination);

          File restoreScriptDestination = new File(userBackupDir, sourceFileName);
          restoreScript.addFile(source, restoreScriptDestination);
        }
      }
    } finally {
      // Re-enable user deployed jar file updates.
      if (deployer != null) {
        deployer.resumeAll();
      }
    }
  }

  private File getBackupDir(File targetDir) {
    InternalDistributedMember memberId =
        cache.getInternalDistributedSystem().getDistributedMember();
    String vmId = memberId.toString();
    vmId = cleanSpecialCharacters(vmId);
    return new File(targetDir, vmId);
  }

  private void copyOplog(DiskStore diskStore, File targetDir, Oplog oplog) throws IOException {
    DirectoryHolder dirHolder = oplog.getDirectoryHolder();
    backupFile(diskStore, dirHolder, targetDir, oplog.getCrfFile());
    backupFile(diskStore, dirHolder, targetDir, oplog.getDrfFile());

    oplog.finishKrf();
    backupFile(diskStore, dirHolder, targetDir, oplog.getKrfFile());
  }

  private void backupFile(DiskStore diskStore, DirectoryHolder dirHolder, File targetDir, File file)
      throws IOException {
    if (file != null && file.exists()) {
      try {
        Path tempDiskDir = getTempDirForDiskStore(diskStore, dirHolder);
        Files.createLink(tempDiskDir.resolve(file.getName()), file.toPath());
        backupDefinition.addOplogFileToBackup(diskStore, tempDiskDir.resolve(file.getName()));
      } catch (IOException | UnsupportedOperationException e) {
        logger.warn("Unable to create hard link for + {}. Reverting to file copy", targetDir);
        FileUtils.copyFileToDirectory(file, targetDir);
      }
    }
  }

  private Path getTempDirForDiskStore(DiskStore diskStore, DirectoryHolder dirHolder)
      throws IOException {
    Map<DirectoryHolder, Path> tempDirByDirectoryHolder =
        diskStoreDirTempDirsByDiskStore.get(diskStore);
    if (tempDirByDirectoryHolder == null) {
      tempDirByDirectoryHolder = new HashMap<>();
      diskStoreDirTempDirsByDiskStore.put(diskStore, tempDirByDirectoryHolder);
    }
    Path directory = tempDirByDirectoryHolder.get(dirHolder);
    if (directory != null) {
      return directory;
    }

    File diskStoreDir = dirHolder.getDir();
    directory = diskStoreDir.toPath().resolve(diskStoreDirectoryName);
    Files.createDirectories(directory);
    tempDirByDirectoryHolder.put(dirHolder, directory);
    return directory;
  }

  private String cleanSpecialCharacters(String string) {
    return string.replaceAll("[^\\w]+", "_");
  }

  public DiskStoreBackup getBackupForDiskStore(DiskStoreImpl diskStore) {
    return backupByDiskStore.get(diskStore);
  }

  private class BackupMembershipListener implements MembershipListener {
    @Override
    public void memberDeparted(InternalDistributedMember id, boolean crashed) {
      cleanup();
    }

    @Override
    public void memberJoined(InternalDistributedMember id) {
      // unused
    }

    @Override
    public void quorumLost(Set<InternalDistributedMember> failures,
        List<InternalDistributedMember> remaining) {
      // unused
    }

    @Override
    public void memberSuspect(InternalDistributedMember id, InternalDistributedMember whoSuspected,
        String reason) {
      // unused
    }
  }
}
