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
package com.gemstone.gemfire.internal.cache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.BackupStatus;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.ClassBuilder;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.JarClassLoader;
import com.gemstone.gemfire.internal.JarDeployer;
import com.gemstone.gemfire.internal.cache.persistence.BackupManager;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.internal.util.TransformUtils;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests for the incremental backup feature.
 * @author rholmes
 */
@SuppressWarnings("serial")
public class IncrementalBackupDUnitTest extends CacheTestCase {
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
  private int dataEnd = dataStart + DATA_INCREMENT;
  
  /**
   * Regular expression used to search for member operation log files.
   */
  private final static String OPLOG_REGEX = ".*\\.[kdc]rf$";
  
  /**
   * Creates test regions for a member.
   */
  private final SerializableRunnable createRegions = new SerializableRunnable() {
    @Override
    public void run() {
      Cache cache = getCache(new CacheFactory().set("log-level", getDUnitLogLevel()));
      cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("fooStore");
      cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("barStore");
      getRegionFactory(cache).setDiskStoreName("fooStore").create("fooRegion");
      getRegionFactory(cache).setDiskStoreName("barStore").create("barRegion");
    }

  };
  
  protected RegionFactory<Integer, String> getRegionFactory(Cache cache) {
    return cache.<Integer,String>createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
  }            
  
  /**
   * A FileFilter that looks for a timestamped gemfire backup directory.
   */
  private final static FileFilter backupDirFilter = new FileFilter() {
    @Override
    public boolean accept(File file) {
      // This will break in about 90 years...
      return (file.isDirectory() && file.getName().startsWith("20"));
    }          
  };
  
  /**
   * Creates a new IncrementalBackupDUnitTest.
   * @param name test case name.
   */
  public IncrementalBackupDUnitTest(String name) {
    super(name);
  }

  /**
   * Abstracts the logging mechanism.
   * @param message a message to log.
   */
  private void log(String message) {
    getLogWriter().info("[IncrementalBackupDUnitTest] " + message);
  }

  /**
   * @return the baseline backup directory.
   */
  private static File getBaselineDir() {
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File dir = new File(tmpDir, "baseline");
    if(!dir.exists()) {
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
    if(!dir.exists()) {
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
    if(!dir.exists()) {
      dir.mkdirs();      
    }
    
    return dir;
  }

  /**
   * Returns the directory for a given member.
   * @param vm a distributed system member.
   * @return the disk directories for a member.
   */
  private File getVMDir(VM vm) {
    return (File) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(new File(getDiskDirs()[0],"../.."));
      }      
    });
  }
  
  /**
   * Invokes {@link AdminDistributedSystem#getMissingPersistentMembers()} on a member.
   * @param vm a member of the distributed system.
   * @return a set of missing members for the distributed system.
   */
  @SuppressWarnings("unchecked")
  private Set<PersistentID> getMissingMembers(VM vm) { 
    return (Set<PersistentID>) vm.invoke(new SerializableCallable("getMissingMembers") {

      public Object call() {
        return AdminDistributedSystemImpl.getMissingPersistentMembers(getSystem().getDistributionManager());
      }
    });
  }
  
  /**
   * Invokes {@link AdminDistributedSystem#backupAllMembers(File)} on a member.
   * @param vm a member of the distributed system
   * @return the status of the backup.
   */
  private BackupStatus baseline(VM vm) {
    return (BackupStatus) vm.invoke(new SerializableCallable("Backup all members.") {

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
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

  /**
   * Invokes {@link AdminDistributedSystem#backupAllMembers(File, File)} on a member.
   * @param vm a member of the distributed system.
   * @return a status of the backup operation.
   */
  private BackupStatus incremental(VM vm) {
    return (BackupStatus) vm.invoke(new SerializableCallable("Backup all members.") {

      public Object call() {
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null; 
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          return adminDS.backupAllMembers(getIncrementalDir(),getBaselineBackupDir());
          
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

  /**
   * Invokes {@link AdminDistributedSystem#backupAllMembers(File, File)} on a member.
   * @param vm a member of the distributed system.
   * @return a status of the backup operation.
   */
  private BackupStatus incremental2(VM vm) {
    return (BackupStatus) vm.invoke(new SerializableCallable("Backup all members.") {

      public Object call() {
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null; 
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          return adminDS.backupAllMembers(getIncremental2Dir(),getIncrementalBackupDir());
          
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

  /**
   * Invokes {@link DistributedSystem#getDistributedMember()} on a member.
   * @param vm a distributed system member.
   * @return the member's id.
   */
  private String getMemberId(VM vm) {
    return (String) vm.invoke(new SerializableCallable("getMemberId") {      
      @Override
      public Object call() throws Exception {
        return getCache().getDistributedSystem().getDistributedMember().toString().replaceAll("[^\\w]+", "_");
      }
    });
  }
  
  /**
   * Invokes {@link Cache#close()} on a member.
   * @param vm a member of the distributed system.
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
   * @param vm a distributed member.
   * @return a PersistentID for a member's disk store.
   */
  private PersistentID getPersistentID(final VM vm,final String diskStoreName) {
    final PersistentID id = (PersistentID) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        PersistentID id = null;
        Collection<DiskStoreImpl> diskStores =  ((GemFireCacheImpl) getCache()).listDiskStores();
        for(DiskStoreImpl diskStore : diskStores) {
          if(diskStore.getName().equals(diskStoreName)) {
            id = diskStore.getPersistentID();
            break;
          }
        }
        
        return id;
      }      
    });
    
    return id;
  }

  /**
   * Locates the PersistentID for the testStore disk store for a distributed member.
   * @param vm a distributed member.
   * @return a PersistentID for a member's disk store.
   */
  private PersistentID getPersistentID(final VM vm) {
    return getPersistentID(vm,"fooStore");
  }
  
  /**
   * Invokes {@link DistributedSystem#disconnect()} on a member.
   * @param disconnectVM a member of the distributed system to disconnect.
   * @param testVM a member of the distributed system to test for the missing member (just disconnected).
   */
  private PersistentID disconnect(final VM disconnectVM, final VM testVM) {    
    final PersistentID id = (PersistentID) disconnectVM.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        PersistentID id = null;
        Collection<DiskStoreImpl> diskStores =  ((GemFireCacheImpl) getCache()).listDiskStores();
        for(DiskStoreImpl diskStore : diskStores) {
          if(diskStore.getName().equals("fooStore")) {
            id = diskStore.getPersistentID();
            break;
          }
        }
        
        getSystem().disconnect();
        
        return id;
      }      
    });
    
    final Set<PersistentID> missingMembers = new HashSet<PersistentID>();
    DistributedTestCase.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        missingMembers.clear();
        missingMembers.addAll(getMissingMembers(testVM));
        
        return (missingMembers.contains(id));
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
   * @param vm a member of the distributed system.
   */
  private void openCache(VM vm) {
    vm.invoke(createRegions);
  }

  /**
   * Blocks and waits for a backup operation to finish on a distributed member.
   * @param vm a member of the distributed system.
   */
  private void waitForBackup(VM vm) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Collection<DiskStoreImpl> backupInProgress = ((GemFireCacheImpl) getCache()).listDiskStores();
        List<DiskStoreImpl> backupCompleteList = new LinkedList<DiskStoreImpl>();
        
        while(backupCompleteList.size() < backupInProgress.size()) {
          for(DiskStoreImpl diskStore : backupInProgress) {
            if((null == diskStore.getInProgressBackup()) && (!backupCompleteList.contains(diskStore))) {
              backupCompleteList.add(diskStore);
            }
          }
        }
      }      
    });
  }
  
  /**
   * Performs a full backup.
   * @return the backup status.
   */
  private BackupStatus performBaseline() {
    return baseline(Host.getHost(0).getVM(1));
  }
  
  /**
   * Performs an incremental backup.
   * @return the backup status.
   */
  private BackupStatus performIncremental() {
    return incremental(Host.getHost(0).getVM(1)); 
  }

  /**
   * Peforms a second incremental backup.
   * @return
   */
  private BackupStatus performIncremental2() {
    return incremental2(Host.getHost(0).getVM(1));
  }
  
  /**
   * @return the directory for the completed baseline backup.
   */
  private static File getBaselineBackupDir() {
    File[] dirs = getBaselineDir().listFiles(backupDirFilter);    
    assertEquals(1,dirs.length);    
    return dirs[0];
  }
  
  /**
   * @return the directory for the completed baseline backup.
   */
  private static File getIncrementalBackupDir() {
    File[] dirs = getIncrementalDir().listFiles(backupDirFilter);    
    assertEquals(1,dirs.length);    
    return dirs[0];
  }

  /**
   * @return the directory for the completed baseline backup.
   */
  private static File getIncremental2BackupDir() {
    File[] dirs = getIncremental2Dir().listFiles(backupDirFilter);    
    assertEquals(1,dirs.length);    
    return dirs[0];
  }

  /**
   * Returns an individual member's backup directory.
   * @param rootDir the directory to begin searching for the member's backup dir from.
   * @param memberId the member's identifier.
   * @return the member's backup directory.
   */
  private File getBackupDirForMember(final File rootDir, final String memberId) {
    File[] dateDirs = rootDir.listFiles(backupDirFilter);    
    assertEquals(1,dateDirs.length);
    
    File[] memberDirs = dateDirs[0].listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return (file.isDirectory() && (file.getName().indexOf(memberId) != -1));
      }      
    }); 
    
    assertEquals(1,memberDirs.length);

    return memberDirs[0];
  }
  
  /**
   * Adds the data region to every participating VM.
   */
  @SuppressWarnings("serial")
  private void createDataRegions() {
    Host host = Host.getHost(0);
    int numberOfVms = host.getVMCount();
    
    for(int i = 0; i < numberOfVms;++i) {
      openCache(host.getVM(i));
    }
  }

  /**
   * Executes a shell command in an external process.
   * @param command a shell command.
   * @return the exit value of processing the shell command.
   * @throws Exception bad, really bad.
   */
  private int execute(String command) 
  throws Exception {
    final ProcessBuilder builder = new ProcessBuilder(command);
    builder.redirectErrorStream(true);
    final Process process = builder.start();
    
    /*
     * Consume standard out.
     */
    new Thread(new Runnable() {
      @Override
      public void run() {
        BufferedReader reader = null;
        
        try {
          reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
          String line = null;

          do {
            line = reader.readLine();
            log(line);
          } while (null != line);
          
          reader.close();
        } catch (IOException e) {
          log("Excecute: error while reading standard in: " + e.getMessage());
        }
      }
    }).start();
    
    /*
     * Consume standard error.
     */
    new Thread(new Runnable() {
      @Override
      public void run() {
        BufferedReader reader = null;
        
        try {
          reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
          String line = null;

          do {
            line = reader.readLine();
          } while (null != line);
          
          reader.close();
        } catch (IOException e) {
          log("Execute: error while reading standard error: " + e.getMessage());
        }
      }
    }).start();

    int result = process.waitFor();
    
    return result;
  }
  
  /**
   * Peforms an operation log restore for a member.
   * @param backupDir the member's backup directory containing the restore script.
   */
  private void performRestore(File memberDir, File backupDir) throws Exception {
    /*
     * The restore script will not restore if there is an if file in the copy to directory.
     * Remove these files first.
     */
    List<File> ifFiles = FileUtil.findAll(memberDir, ".*\\.if$");
    for(File file : ifFiles) {
      file.delete();
    }

    /*
     * Remove all operation logs.
     */
    List<File> oplogs = FileUtil.findAll(memberDir, OPLOG_REGEX);
    for(File file : oplogs) {
      file.delete();
    }
    
    /*
     * Get a hold of the restore script and make sure it is there.
     */
    File restoreScript = new File(backupDir,"restore.sh");
    if(!restoreScript.exists()) {
      restoreScript = new File(backupDir,"restore.bat");
    }    
    assertTrue(restoreScript.exists());

    assertEquals(0,execute(restoreScript.getAbsolutePath()));
  }
  
  /**
   * Adds an incomplete marker to the baseline backup for a member. 
   * @param vm a distributed system member.
   */
  private void markAsIncomplete(VM vm) throws IOException {
    File backupDir = getBackupDirForMember(getBaselineDir(), getMemberId(vm));
    assertTrue(backupDir.exists());
    
    File incomplete = new File(backupDir,BackupManager.INCOMPLETE_BACKUP);
    incomplete.createNewFile();
  }
  
  /**
   * Loads additional data into the test regions.
   */
  private void loadMoreData() {
    Region<Integer,String> fooRegion = getCache().getRegion("fooRegion");
    
    // Fill our region data
    for(int i = this.dataStart; i < this.dataEnd; ++i) {
      fooRegion.put(i, Integer.toString(i));
    }
    
    Region<Integer,String> barRegion = getCache().getRegion("barRegion");
    
    // Fill our region data
    for(int i = this.dataStart; i < this.dataEnd; ++i) {
      barRegion.put(i, Integer.toString(i));
    }        
    
    this.dataStart += DATA_INCREMENT;
    this.dataEnd += DATA_INCREMENT;
  }

  /**
   * Used to confirm valid BackupStatus data.
   * Confirms fix for defect #45657
   * @param backupStatus contains a list of members that were backed up.
   */
  private void assertBackupStatus(final BackupStatus backupStatus) {
    Map<DistributedMember, Set<PersistentID>> backupMap = backupStatus.getBackedUpDiskStores();
    assertFalse(backupMap.isEmpty());
    
    for(DistributedMember member : backupMap.keySet()) {
      for(PersistentID id : backupMap.get(member)) {
        assertNotNull(id.getHost());
        assertNotNull(id.getUUID());
        assertNotNull(id.getDirectory());
      }
    }
  }
  
  /**
   * 1. Add partitioned persistent region to all members.
   * 2. Fills region with data.
   */
  public void setUp() throws Exception {
    super.setUp();
    createDataRegions();
    createRegions.run();
    loadMoreData();    
    
    log("Data region created and populated.");
  }

  /**
   * Removes backup directories (and all backup data).
   */
  @Override
  public void tearDown2() throws Exception {
    FileUtil.delete(getIncremental2Dir());
    FileUtil.delete(getIncrementalDir());
    FileUtil.delete(getBaselineDir());
    
    super.tearDown2();
  }

  /**
   * This tests the basic features of incremental backup.  This means that operation logs that are present
   * in both the baseline and member's disk store should not be copied during the incremental backup.
   * Additionally, the restore script should reference and copy operation logs from the baseline backup.
   * @throws Exception
   */
  public void testIncrementalBackup() 
  throws Exception {
    String memberId = getMemberId(Host.getHost(0).getVM(1));
    
    File memberDir = getVMDir(Host.getHost(0).getVM(1));
    assertNotNull(memberDir);

    // Find all of the member's oplogs in the disk directory (*.crf,*.krf,*.drf)
    List<File> memberOplogFiles = FileUtil.findAll(memberDir, OPLOG_REGEX);    
    assertFalse(memberOplogFiles.isEmpty());
    
    // Perform a full backup and wait for it to finish
    assertBackupStatus(performBaseline());
    waitForBackup(Host.getHost(0).getVM(1));
    
    // Find all of the member's oplogs in the baseline (*.crf,*.krf,*.drf)
    List<File> memberBaselineOplogs = FileUtil.findAll(getBackupDirForMember(getBaselineDir(), memberId), OPLOG_REGEX);    
    assertFalse(memberBaselineOplogs.isEmpty());
    
    List<String> memberBaselineOplogNames = new LinkedList<String>();
    TransformUtils.transform(memberBaselineOplogs, memberBaselineOplogNames, TransformUtils.fileNameTransformer);

    // Peform and incremental backup and wait for it to finish
    loadMoreData();             // Doing this preserves the new oplogs created by the baseline backup
    assertBackupStatus(performIncremental());
    waitForBackup(Host.getHost(0).getVM(1));

    // Find all of the member's oplogs in the incremental (*.crf,*.krf,*.drf)
    List<File> memberIncrementalOplogs = FileUtil.findAll(getBackupDirForMember(getIncrementalDir(), memberId), OPLOG_REGEX);    
    assertFalse(memberIncrementalOplogs.isEmpty());
    
    List<String> memberIncrementalOplogNames = new LinkedList<String>();
    TransformUtils.transform(memberIncrementalOplogs, memberIncrementalOplogNames, TransformUtils.fileNameTransformer);

    log("BASELINE OPLOGS = " + memberBaselineOplogNames);    
    log("INCREMENTAL OPLOGS = " + memberIncrementalOplogNames);
    
    /*
     * Assert that the incremental backup does not contain baseline operation
     * logs that the member still has copies of.
     */
    for(String oplog : memberBaselineOplogNames) {
      assertFalse(memberIncrementalOplogNames.contains(oplog));
    }

    // Perform a second incremental and wait for it to finish.
    loadMoreData();             // Doing this preserves the new oplogs created by the incremental backup
    assertBackupStatus(performIncremental2());
    waitForBackup(Host.getHost(0).getVM(1));
    
    List<File> memberIncremental2Oplogs = FileUtil.findAll(getBackupDirForMember(getIncremental2Dir(),memberId), OPLOG_REGEX);
    assertFalse(memberIncremental2Oplogs.isEmpty());
    
    List<String> memberIncremental2OplogNames = new LinkedList<String>();
    TransformUtils.transform(memberIncremental2Oplogs, memberIncremental2OplogNames, TransformUtils.fileNameTransformer);
    
    log("INCREMENTAL 2 OPLOGS = " + memberIncremental2OplogNames);

    /*
     * Assert that the second incremental backup does not contain operation logs copied into
     * the baseline.
     */
    for(String oplog : memberBaselineOplogNames) {
      assertFalse(memberIncremental2OplogNames.contains(oplog));
    }
    
    /*
     * Also assert that the second incremental backup does not contain operation logs 
     * copied into the member's first incremental backup.
     */
    for(String oplog : memberIncrementalOplogNames) {
      assertFalse(memberIncremental2OplogNames.contains(oplog));
    }

    // Shut down our member so we can perform a restore
    PersistentID id = getPersistentID(Host.getHost(0).getVM(1));
    closeCache(Host.getHost(0).getVM(1));
    
    // Execute the restore
    performRestore(new File(id.getDirectory()), getBackupDirForMember(getIncremental2Dir(), memberId));
    
    /*
     * Collect all of the restored operation logs.
     */
    List<File> restoredOplogs = FileUtil.findAll(new File(id.getDirectory()), OPLOG_REGEX);
    assertFalse(restoredOplogs.isEmpty());
    List<String> restoredOplogNames = new LinkedList<String>(); 
    TransformUtils.transform(restoredOplogs, restoredOplogNames, TransformUtils.fileNameTransformer);
    
    /*
     * Assert that baseline operation logs have been copied over to the 
     * member's disk directory.
     */
    for(String oplog : memberBaselineOplogNames) {
      assertTrue(restoredOplogNames.contains(oplog));
    }
    
    /*
     * Assert that the incremental operation logs have been copied over
     * to the member's disk directory.
     */
    for(String oplog : memberIncrementalOplogNames) {
      assertTrue(restoredOplogNames.contains(oplog));
    }
    
    /*
     * Assert that the second incremental operation logs have been copied over
     * to the member's disk directory.
     */
    for(String oplog : memberIncremental2OplogNames) {
      assertTrue(restoredOplogNames.contains(oplog));
    }

    /*
     * Reconnect the member.
     */
    openCache(Host.getHost(0).getVM(1));
  }
  
  /**
   * Successful if a member performs a full backup when its backup data is not present
   * in the baseline (for whatever reason).  This also tests what happens when a member is offline during the baseline backup.
   * 
   * The test is regarded as successful when all of the missing members oplog files are backed up during an
   * incremental backup.  This means that the member peformed a full backup because its oplogs were missing
   * in the baseline.
   */
  public void testMissingMemberInBaseline()
  throws Exception {        
    // Simulate the missing member by forcing a persistent member
    // to go offline.
    final PersistentID missingMember = disconnect(Host.getHost(0).getVM(0),Host.getHost(0).getVM(1));
    
    /*
     * Perform baseline and make sure that the list of offline disk stores
     * contains our missing member.
     */
    BackupStatus baselineStatus = performBaseline();
    assertBackupStatus(baselineStatus);
    assertNotNull(baselineStatus.getOfflineDiskStores());
    assertEquals(2, baselineStatus.getOfflineDiskStores().size());

    // Find all of the member's oplogs in the missing member's diskstore directory structure (*.crf,*.krf,*.drf)
    List<File> missingMemberOplogFiles = FileUtil.findAll(new File(missingMember.getDirectory()), OPLOG_REGEX);    
    assertFalse(missingMemberOplogFiles.isEmpty());
    
    /*
     * Restart our missing member and make sure it is back online and 
     * part of the distributed system
     */
    openCache(Host.getHost(0).getVM(0));
    
    /*
     * After reconnecting make sure the other members agree that the missing 
     * member is back online.
     */
    final Set<PersistentID> missingMembers = new HashSet<PersistentID>();
    DistributedTestCase.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        missingMembers.clear();
        missingMembers.addAll(getMissingMembers(Host.getHost(0).getVM(1)));
        return (!missingMembers.contains(missingMember));
      }

      @Override
      public String description() {
        return "[testMissingMemberInBasline] Wait for missing member.";
      }      
    }, 10000, 500, false);

    assertEquals(0,missingMembers.size());
    
    /*
     * Peform incremental and make sure we have no offline disk stores.
     */
    BackupStatus incrementalStatus = performIncremental();
    assertBackupStatus(incrementalStatus);
    assertNotNull(incrementalStatus.getOfflineDiskStores());
    assertEquals(0,incrementalStatus.getOfflineDiskStores().size());
    
    // Get the missing member's member id which is different from the PersistentID
    String memberId = getMemberId(Host.getHost(0).getVM(0));
    assertNotNull(memberId);
    
    // Get list of backed up oplog files in the incremental backup for the missing member
    File incrementalMemberDir = getBackupDirForMember(getIncrementalDir(), memberId);
    List<File> backupOplogFiles = FileUtil.findAll(incrementalMemberDir, OPLOG_REGEX);
    assertFalse(backupOplogFiles.isEmpty());
    
    // Transform missing member oplogs to just their file names.
    List<String> missingMemberOplogNames = new LinkedList<String>();
    TransformUtils.transform(missingMemberOplogFiles, missingMemberOplogNames, TransformUtils.fileNameTransformer);
    
    // Transform missing member's incremental backup oplogs to just their file names.
    List<String> backupOplogNames = new LinkedList<String>();
    TransformUtils.transform(backupOplogFiles, backupOplogNames, TransformUtils.fileNameTransformer);
    
    /*
     * Make sure that the incremental backup for the missing member contains all of the
     * operation logs for that member.  This proves that a full backup was performed for 
     * that member.
     */
    assertTrue(backupOplogNames.containsAll(missingMemberOplogNames));
  }
  
  /**
   * Successful if a member performs a full backup if their backup is marked as 
   * incomplete in the baseline.
   */
  public void testIncompleteInBaseline()
  throws Exception {
    /*
     * Get the member ID for VM 1 and perform a baseline.
     */
    String memberId = getMemberId(Host.getHost(0).getVM(1));    
    assertBackupStatus(performBaseline());
    
    /* 
     * Find all of the member's oplogs in the baseline (*.crf,*.krf,*.drf)
     */
    List<File> memberBaselineOplogs = FileUtil.findAll(getBackupDirForMember(getBaselineDir(), memberId), OPLOG_REGEX);    
    assertFalse(memberBaselineOplogs.isEmpty());
    
    List<String> memberBaselineOplogNames = new LinkedList<String>();
    TransformUtils.transform(memberBaselineOplogs, memberBaselineOplogNames, TransformUtils.fileNameTransformer);

    // Mark the baseline as incomplete (even though it really isn't)
    markAsIncomplete(Host.getHost(0).getVM(1));
    
    /*
     * Do an incremental.  It should discover that the baseline is incomplete
     * and backup all of the operation logs that are in the baseline.
     */
    assertBackupStatus(performIncremental());
    
    /*
     * Find all of the member's oplogs in the incremental (*.crf,*.krf,*.drf)
     */
    List<File> memberIncrementalOplogs = FileUtil.findAll(getBackupDirForMember(getIncrementalDir(), memberId), OPLOG_REGEX);    
    assertFalse(memberIncrementalOplogs.isEmpty());
    
    List<String> memberIncrementalOplogNames = new LinkedList<String>();
    TransformUtils.transform(memberIncrementalOplogs, memberIncrementalOplogNames, TransformUtils.fileNameTransformer);
    
    /*
     * Assert that all of the baseline operation logs are in the incremental backup.
     * If so, then the incomplete marker was discovered in the baseline by the incremental backup process.
     */
    for(String oplog : memberBaselineOplogNames) {
      assertTrue(memberIncrementalOplogNames.contains(oplog));
    }
  }
  
  /**
   * Successful if all members perform a full backup when they share the baseline directory
   * and it is missing.
   */
  public void testMissingBaseline()
  throws Exception {
    /*
     * Get the member ID for VM 1 and perform a baseline.
     */
    String memberId = getMemberId(Host.getHost(0).getVM(1));    
    assertBackupStatus(performBaseline());
    
    /* 
     * Find all of the member's oplogs in the baseline (*.crf,*.krf,*.drf)
     */
    List<File> memberBaselineOplogs = FileUtil.findAll(getBackupDirForMember(getBaselineDir(), memberId), OPLOG_REGEX);    
    assertFalse(memberBaselineOplogs.isEmpty());
    
    List<String> memberBaselineOplogNames = new LinkedList<String>();
    TransformUtils.transform(memberBaselineOplogs, memberBaselineOplogNames, TransformUtils.fileNameTransformer);
    
    /*
     * Custom incremental backup callable that retrieves the current baseline before deletion.
     */
    SerializableCallable callable = new SerializableCallable("Backup all members.") {
      private File baselineDir = getBaselineBackupDir();
      
      public Object call() {
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null; 
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          return adminDS.backupAllMembers(getIncrementalDir(),this.baselineDir);
          
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if(adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    };
    
    /*
     * Do an incremental after deleting the baseline.  It should discover that the baseline is gone
     * and backup all of the operation logs that are in the baseline.
     */
    FileUtil.delete(getBaselineDir());            
    Host.getHost(0).getVM(1).invoke(callable);
    
    /*
     * Find all of the member's oplogs in the incremental (*.crf,*.krf,*.drf)
     */
    List<File> memberIncrementalOplogs = FileUtil.findAll(getBackupDirForMember(getIncrementalDir(), memberId), OPLOG_REGEX);    
    assertFalse(memberIncrementalOplogs.isEmpty());
    
    List<String> memberIncrementalOplogNames = new LinkedList<String>();
    TransformUtils.transform(memberIncrementalOplogs, memberIncrementalOplogNames, TransformUtils.fileNameTransformer);
    
    /*
     * Assert that all of the baseline operation logs are in the incremental backup.
     * If so, then the missing baseline was discovered by the incremental backup process.
     */
    for(String oplog : memberBaselineOplogNames) {
      assertTrue(memberIncrementalOplogNames.contains(oplog));
    }    
  }
  
  /**
   * Successful if a user deployed jar file is included as part of the backup.
   * @throws Exception
   */
  public void testBackupUserDeployedJarFiles() throws Exception {
    final String jarName = "BackupJarDeploymentDUnit";
    final String jarNameRegex = ".*" + jarName + ".*";
    final ClassBuilder classBuilder = new ClassBuilder();
    final byte[] classBytes = classBuilder.createJarFromName(jarName);
    
    VM vm0 = Host.getHost(0).getVM(0);
    
    /*
     * Deploy a "dummy" jar to the VM.
     */
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        JarDeployer deployer = new JarDeployer();
        deployer.deploy(new String[] {jarName}, new byte[][] {classBytes});
        return null;
      }
    });

    /*
     * Perform backup.  Make sure it is successful.
     */
    assertBackupStatus(baseline(vm0));
    
    /*
     * Make sure the user deployed jar is part of the backup.
     */
    List<File> memberDeployedJarFiles = FileUtil.findAll(getBackupDirForMember(getBaselineDir(), getMemberId(vm0)), jarNameRegex);    
    assertFalse(memberDeployedJarFiles.isEmpty());
    
    // Shut down our member so we can perform a restore
    PersistentID id = getPersistentID(vm0);
    closeCache(vm0);

    /*
     * Get the VM's user directory.
     */
    final String vmDir = (String) vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {        
        return System.getProperty("user.dir");
      }      
    });
    
    File backupDir = getBackupDirForMember(getBaselineDir(), getMemberId(vm0));
    
    vm0.bounce();
    
    /*
     * Cleanup "dummy" jar from file system.
     */
    FileUtil.deleteMatching(new File(vmDir), "^" + JarDeployer.JAR_PREFIX + jarName + ".*#\\d++$");
    
    // Execute the restore
    performRestore(new File(id.getDirectory()), backupDir);
    
    /*
     * Make sure the user deployed jar is part of the restore.
     */
    List<File> restoredJars = FileUtil.findAll(new File(vmDir), jarNameRegex);
    assertFalse(restoredJars.isEmpty());
    List<String> restoredJarNames = new LinkedList<String>(); 
    TransformUtils.transform(memberDeployedJarFiles, restoredJarNames, TransformUtils.fileNameTransformer);
    for(String name : restoredJarNames) {
      assertTrue(name.indexOf(jarName) != -1);
    }

    // Restart the member
    openCache(vm0);
    
    /*
     * Remove the "dummy" jar from the VM.
     */
    vm0.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        JarDeployer deployer = new JarDeployer();
        for (JarClassLoader jarClassLoader : deployer.findJarClassLoaders()) {
          if (jarClassLoader.getJarName().startsWith(jarName)) {
            deployer.undeploy(jarClassLoader.getJarName());
          }
        }
        return null;
      }      
    });
    
    /*
     * Cleanup "dummy" jar from file system.
     */
    FileUtil.deleteMatching(new File(vmDir), "^" + JarDeployer.JAR_PREFIX + jarName + ".*#\\d++$");    
  }
}
