/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.persistence;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.JarClassLoader;
import com.gemstone.gemfire.internal.JarDeployer;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * This class manages the state an logic to backup a single
 * cache.
 * 
 *
 */
public class BackupManager implements MembershipListener {

  //TODO prpersist internationalize this.
  public static final String INCOMPLETE_BACKUP = "INCOMPLETE_BACKUP";
  public static final String README = "README.txt";
  public  static final String DATA_STORES = "diskstores";
  public static final String USER_FILES = "user";
  public static final String CONFIG = "config";
  private InternalDistributedMember sender;
  private GemFireCacheImpl cache;
  private CountDownLatch allowDestroys = new CountDownLatch(1);
  private volatile boolean isCancelled = false;

  public BackupManager(InternalDistributedMember sender,
      GemFireCacheImpl gemFireCache) {
    this.sender = sender;
    this.cache =gemFireCache;
  }
  
  public void start() {
    final DM distributionManager = cache.getDistributedSystem().getDistributionManager();
    //We need to watch for pure admin guys that depart. this allMembershipListener set
    //looks like it should receive those events.
    Set allIds = distributionManager.addAllMembershipListenerAndGetAllIds(this);
    if(!allIds.contains(sender)) {
      cleanup();
      throw new IllegalStateException("The admin member requesting a backup has already departed");
    }
  }

  private void cleanup() {
    isCancelled = true;
    allowDestroys.countDown();
    Collection<DiskStoreImpl> diskStores = cache.listDiskStoresIncludingRegionOwned();
    for(DiskStoreImpl store : diskStores) {
      store.releaseBackupLock();
    }
    final DM distributionManager = cache.getDistributedSystem().getDistributionManager();
    distributionManager.removeAllMembershipListener(this);
    cache.clearBackupManager();
  }
  
  public HashSet<PersistentID> prepareBackup() {
    HashSet<PersistentID> persistentIds = new HashSet<PersistentID>();
    Collection<DiskStoreImpl> diskStores = cache.listDiskStoresIncludingRegionOwned();
    for(DiskStoreImpl store : diskStores) {
      store.lockStoreBeforeBackup();
      if(store.hasPersistedData()) {
        persistentIds.add(store.getPersistentID());
        store.getStats().startBackup();
      }
    }
    return persistentIds;
  }

  /**
   * Returns the memberId directory for this member in the baseline.  The memberId may have changed if this
   * member has been restarted since the last backup.
   * @param baselineParentDir parent directory of last backup.
   * @return null if the baseline for this member could not be located.
   */
  private File findBaselineForThisMember(File baselineParentDir) {
    File baselineDir = null;

    /*
     * Find the first matching DiskStoreId directory for this member.
     */
    for(DiskStoreImpl diskStore : cache.listDiskStoresIncludingRegionOwned()) {
      baselineDir = FileUtil.find(baselineParentDir, ".*" + diskStore.getBackupDirName() + "$");
      if(null != baselineDir) {
        break;
      }
    }
    
    /*
     * We found it? Good. Set this member's baseline to the backed up disk store's member dir (two levels up).
     */
    if(null != baselineDir) {
      baselineDir = baselineDir.getParentFile().getParentFile();
    }
    
    return baselineDir;
  }

  /**
   * Performs a sanity check on the baseline directory for incremental backups. If a baseline directory
   * exists for the member and there is no INCOMPLETE_BACKUP file then return the data stores directory
   * for this member.
   * @param baselineParentDir a previous backup directory.  This is used with the incremental backup option.  May be null if the user specified a full backup.
   * @return null if the backup is to be a full backup otherwise return the data store directory in the previous backup for this member (if incremental).
   * @throws IOException
   */
  private File checkBaseline(File baselineParentDir) throws IOException{
    File baselineDir = null;
    
    if(null != baselineParentDir) {
      // Start by looking for this memberId
      baselineDir = getBackupDir(baselineParentDir);
      
      if(!baselineDir.exists()) {
        // hmmm, did this member have a restart?
        // Determine which member dir might be a match for us
        baselineDir = findBaselineForThisMember(baselineParentDir);
      } 
      
      if(null != baselineDir) {
        // check for existence of INCOMPLETE_BACKUP file
        File incompleteBackup = new File(baselineDir,INCOMPLETE_BACKUP);
        if(incompleteBackup.exists()) {
           baselineDir = null;
        }
      }        
    }
    
    return baselineDir;
  }
  
  public HashSet<PersistentID> finishBackup(File targetDir, File baselineDir, boolean abort) throws IOException {
    try {
      if(abort) {
        return new HashSet<PersistentID>();
      }
      
      File backupDir = getBackupDir(targetDir);
      
      // Make sure our baseline is okay for this member
      baselineDir = checkBaseline(baselineDir);

      // Create an inspector for the baseline backup
      BackupInspector inspector = (baselineDir == null ? null : BackupInspector.createInspector(baselineDir));
            
      File storesDir = new File(backupDir, DATA_STORES);
      RestoreScript restoreScript = new RestoreScript();
      HashSet<PersistentID> persistentIds = new HashSet<PersistentID>();
      Collection<DiskStoreImpl> diskStores = new ArrayList<DiskStoreImpl>(cache.listDiskStoresIncludingRegionOwned());

      boolean foundPersistentData = false;
      for(Iterator<DiskStoreImpl> itr = diskStores.iterator(); itr.hasNext();) {
        DiskStoreImpl store = itr.next();
        if(store.hasPersistedData()) {
          if(!foundPersistentData) {
            createBackupDir(backupDir);
            foundPersistentData = true;
          }
          File diskStoreDir = new File(storesDir, store.getBackupDirName());
          diskStoreDir.mkdir();
          store.startBackup(diskStoreDir, inspector, restoreScript);
        } else {
          itr.remove();
        }
        store.releaseBackupLock();
      }

      allowDestroys.countDown();

      for(DiskStoreImpl store : diskStores) {
        store.finishBackup(this);
        store.getStats().endBackup();
        persistentIds.add(store.getPersistentID());
      }

      if(foundPersistentData) {
        backupConfigFiles(restoreScript, backupDir);
        backupUserFiles(restoreScript, backupDir);
        backupDeployedJars(restoreScript, backupDir);
        restoreScript.generate(backupDir);
        File incompleteFile = new File(backupDir, INCOMPLETE_BACKUP);
        if(!incompleteFile.delete()) {
          throw new IOException("Could not delete file " + INCOMPLETE_BACKUP);
        }
      }


      return persistentIds;

    } finally {
      cleanup();
    }
  }
  
  public void abort() {
    cleanup();
  }
  
  private void backupConfigFiles(RestoreScript restoreScript, File backupDir) throws IOException {
    File configBackupDir = new File(backupDir, CONFIG);
    FileUtil.mkdirs(configBackupDir);
    URL url = cache.getCacheXmlURL();
    if(url != null) {
      File cacheXMLBackup = new File(configBackupDir, DistributionConfig.DEFAULT_CACHE_XML_FILE.getName());
      FileUtil.copy(url, cacheXMLBackup);
    }
    
    URL propertyURL = DistributedSystem.getPropertyFileURL();
    if(propertyURL != null) {
      File propertyBackup = new File(configBackupDir, DistributionConfig.GEMFIRE_PREFIX + "properties");
      FileUtil.copy(propertyURL, propertyBackup);
    }
    
    // TODO sbawaska: should the gfsecurity.properties file be backed up?
  }

  private void backupUserFiles(RestoreScript restoreScript, File backupDir) throws IOException {
    List<File> backupFiles = cache.getBackupFiles();
    File userBackupDir = new File(backupDir, USER_FILES);
    if (!userBackupDir.exists()) {
      userBackupDir.mkdir();
    }
    for(File original : backupFiles) {
      if(original.exists()) {
        original = original.getAbsoluteFile();
        File dest = new File(userBackupDir, original.getName());
        FileUtil.copy(original, dest);
        restoreScript.addExistenceTest(original);
        restoreScript.addFile(original, dest);
      }
    }
  }

  /**
   * Copies user deployed jars to the backup directory.
   * @param restoreScript Used to restore from this backup.
   * @param backupDir The backup directory for this member.
   * @throws IOException one or more of the jars did not successfully copy. 
   */
  private void backupDeployedJars(RestoreScript restoreScript, File backupDir)  throws IOException {
    JarDeployer deployer = null;
    
    try {
      deployer = new JarDeployer();
      
      /*
       * Suspend any user deployed jar file updates during this
       * backup.
       */
      deployer.suspendAll();
      
      List<JarClassLoader> jarList = deployer.findJarClassLoaders();
      if(!jarList.isEmpty()) {
        File userBackupDir = new File(backupDir, USER_FILES);
        if (!userBackupDir.exists()) {
          userBackupDir.mkdir();
        }
        
        for(JarClassLoader loader : jarList) {
          File source = new File(loader.getFileCanonicalPath());
          File dest = new File(userBackupDir,source.getName());
          FileUtil.copy(source,dest);
          restoreScript.addFile(source, dest);
        }        
      }
    } finally {
      /*
       * Re-enable user deployed jar file updates.
       */
      if(null != deployer) {
        deployer.resumeAll();
      }
    }
  }

  private File getBackupDir(File targetDir) throws IOException {
    InternalDistributedMember memberId = cache.getDistributedSystem().getDistributedMember();
    String vmId = memberId.toString();
    vmId = cleanSpecialCharacters(vmId);
    File backupDir  = new File(targetDir, vmId);
    
    
    return backupDir;
  }
  
  private void createBackupDir(File backupDir) throws IOException {
    if(backupDir.exists()) {
      throw new IOException("Backup directory " + backupDir.getAbsolutePath() + " already exists.");
    }
    
    if(!FileUtil.mkdirs(backupDir)) {
      throw new IOException("Could not create directory: " + backupDir);
    }
    
    File incompleteFile = new File(backupDir, INCOMPLETE_BACKUP);
    if(!incompleteFile.createNewFile()) {
      throw new IOException("Could not create file: " + incompleteFile);
    }
    
    File readme = new File(backupDir, README);
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

  public void memberJoined(InternalDistributedMember id) {
  }

  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
  }

  public void memberSuspect(InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {
  }

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
}
