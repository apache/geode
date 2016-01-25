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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.gemstone.gemfire.admin.BackupStatus;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.partitioned.PersistentPartitionedRegionTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DUnitEnv;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author dsmith
 *
 */
public class BackupDUnitTest extends PersistentPartitionedRegionTestBase {

  private static final long  MAX_WAIT = 30 * 1000;



  public BackupDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void tearDown2() throws Exception {
    StringBuilder failures = new StringBuilder();
    FileUtil.delete(getBackupDir(), failures);
    if (failures.length() > 0) {
      getLogWriter().error(failures.toString());
    }
    super.tearDown2();
  }
  
  public void testBackupPR() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    
    long lm0 = setBackupFiles(vm0);
    long lm1 = setBackupFiles(vm1);

    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");
    
    BackupStatus status = backup(vm2);
    assertEquals(2, status.getBackedUpDiskStores().size());
    assertEquals(Collections.emptySet(), status.getOfflineDiskStores());
    
    
    List<File> mytexts = FileUtil.findAll(getBackupDir(), ".*my.txt.*");
    assertEquals(2, mytexts.size());
    deleteOldUserUserFile(vm0);
    deleteOldUserUserFile(vm1);
    validateBackupComplete();
    
    createData(vm0, 0, 5, "C", "region1");
    createData(vm0, 0, 5, "C", "region2");
    
    assertEquals(2, status.getBackedUpDiskStores().size());
    assertEquals(Collections.emptySet(), status.getOfflineDiskStores());
    
    closeCache(vm0);
    closeCache(vm1);
    
    //Destroy the current data
    invokeInEveryVM(new SerializableRunnable("Clean disk dirs") {
      public void run() {
        try {
          cleanDiskDirs();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    
    restoreBackup(2);
    
    getLogWriter().info("Creating region in VM0");
    AsyncInvocation async0 = createPersistentRegionAsync(vm0);
    getLogWriter().info("Creating region in VM1");
    AsyncInvocation async1 = createPersistentRegionAsync(vm1);
    
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    
    checkData(vm0, 0, 5, "A", "region1");
    checkData(vm0, 0, 5, "B", "region2");
    verifyUserFileRestored(vm0, lm0);
    verifyUserFileRestored(vm1, lm1);
  }
  
  /**
   * Test of bug 42419
   * @throws Throwable
   */
  public void testBackupFromMemberWithDiskStore() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    
    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");
    
    BackupStatus status = backup(vm1);
    assertEquals(2, status.getBackedUpDiskStores().size());
    for(DistributedMember key : status.getBackedUpDiskStores().keySet()) {
      assertNotNull(key);
    }
    assertEquals(Collections.emptySet(), status.getOfflineDiskStores());
    
    
    validateBackupComplete();
    
    closeCache(vm0);
    closeCache(vm1);
    
    //Destroy the current data
    invokeInEveryVM(new SerializableRunnable("Clean disk dirs") {
      public void run() {
        try {
          cleanDiskDirs();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    
    restoreBackup(2);
    
    getLogWriter().info("Creating region in VM0");
    AsyncInvocation async0 = createPersistentRegionAsync(vm0);
    getLogWriter().info("Creating region in VM1");
    AsyncInvocation async1 = createPersistentRegionAsync(vm1);
    
    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);
    
    checkData(vm0, 0, 5, "A", "region1");
    checkData(vm0, 0, 5, "B", "region2");
  }
  
//  public void testLoop() throws Throwable {
//    for(int i =0 ;i < 100; i++) {
//      testBackupWhileBucketIsCreated();
//      setUp();
//      tearDown();
//    }
//  }
  
  /**
   * Test for bug 42419
   * @throws Throwable
   */
  public void testBackupWhileBucketIsCreated() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    
      
      getLogWriter().info("Creating region in VM0");
      createPersistentRegion(vm0);

      //create a bucket on vm0
      createData(vm0, 0, 1, "A", "region1");

      //create the pr on vm1, which won't have any buckets
      getLogWriter().info("Creating region in VM1");
      createPersistentRegion(vm1);


      final AtomicReference<BackupStatus> statusRef = new AtomicReference<BackupStatus>();
      Thread thread1 = new Thread() {
        public void run() {

          BackupStatus status = backup(vm2);
          statusRef.set(status);
          
        }
      };
      thread1.start();
      Thread thread2 = new Thread() {
        public void run() {
          createData(vm0, 1, 5, "A", "region1");    
        }
      };
      thread2.start();
      thread1.join();
      thread2.join();
      
      
      BackupStatus status = statusRef.get();
      assertEquals(2, status.getBackedUpDiskStores().size());
      assertEquals(Collections.emptySet(), status.getOfflineDiskStores());


      validateBackupComplete();

      createData(vm0, 0, 5, "C", "region1");

      assertEquals(2, status.getBackedUpDiskStores().size());
      assertEquals(Collections.emptySet(), status.getOfflineDiskStores());

      closeCache(vm0);
      closeCache(vm1);

      //Destroy the current data
      invokeInEveryVM(new SerializableRunnable("Clean disk dirs") {
        public void run() {
          try {
            cleanDiskDirs();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });

      restoreBackup(2);

      getLogWriter().info("Creating region in VM0");
      AsyncInvocation async0 = createPersistentRegionAsync(vm0);
      getLogWriter().info("Creating region in VM1");
      AsyncInvocation async1 = createPersistentRegionAsync(vm1);

      async0.getResult(MAX_WAIT);
      async1.getResult(MAX_WAIT);

      checkData(vm0, 0, 1, "A", "region1");
  }
  
  public void testBackupWhileBucketIsMovedBackupBeforeSendDestroy() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm2 = host.getVM(2);
    
    DistributionMessageObserver observer = new SerializableDistributionMessageObserver() {
      private volatile boolean done;
      private AtomicInteger count = new AtomicInteger();
      private volatile int replyId =-0xBAD;
      
      @Override
      public void beforeSendMessage(DistributionManager dm,
          DistributionMessage msg) {
        
        //The bucket move will send a destroy region message.
        if(msg instanceof DestroyRegionOperation.DestroyRegionMessage && !done) {
          backup(vm2);
          done = true;
        }
      }
    };
    
    backupWhileBucketIsMoved(observer);
  }
  
  public void testBackupWhileBucketIsMovedBackupAfterSendDestroy() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    
    DistributionMessageObserver observer = new SerializableDistributionMessageObserver() {
      private volatile boolean done;
      private AtomicInteger count = new AtomicInteger();
      private volatile int replyId =-0xBAD;
      
      @Override
      public void beforeSendMessage(DistributionManager dm,
          DistributionMessage msg) {
        
        //The bucket move will send a destroy region message.
        if(msg instanceof DestroyRegionOperation.DestroyRegionMessage && !done) {
          this.replyId = msg.getProcessorId();
        }
      }

      @Override
      public void beforeProcessMessage(DistributionManager dm,
          DistributionMessage message) {
        if(message instanceof ReplyMessage 
            && replyId != -0xBAD 
            && replyId == message.getProcessorId()
            && !done
            //we need two replies
            && count.incrementAndGet() == 2) {
          backup(vm2);
          done = true;
        }
      }
    };
    
    backupWhileBucketIsMoved(observer);
  }
   
  /**
   * Test for bug 42420. Invoke a backup
   * when a bucket is in the middle of being moved.
   * @param observer - a message observer that triggers
   * at the backup at the correct time.
   * @throws Throwable
   */
  public void backupWhileBucketIsMoved(final DistributionMessageObserver observer) throws Throwable {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    
    vm0.invoke(new SerializableRunnable("Add listener to invoke backup") {
      
      public void run() {
        disconnectFromDS();
        
        //This listener will wait for a response to the 
        //destroy region message, and then trigger a backup.
        //That will backup before this member has finished destroying
        //a bucket, but after the peer has removed the bucket.
        DistributionMessageObserver.setInstance(observer);
      }
    });
    try {
      
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);

    //create twos bucket on vm0
    createData(vm0, 0, 2, "A", "region1");

    //create the pr on vm1, which won't have any buckets
    getLogWriter().info("Creating region in VM1");

    createPersistentRegion(vm1);
    
    //Perform a rebalance. This will trigger the backup in the middle
    //of the bucket move.
    vm0.invoke(new SerializableRunnable("Do rebalance") {
      
      public void run() {
        Cache cache = getCache();
        RebalanceOperation op = cache.getResourceManager().createRebalanceFactory().start();
        RebalanceResults results;
        try {
          results = op.getResults();
          assertEquals(1, results.getTotalBucketTransfersCompleted());
        } catch (Exception e) {
          fail("interupted", e);
        }
      }
    });
    

    validateBackupComplete();

    createData(vm0, 0, 5, "C", "region1");

    closeCache(vm0);
    closeCache(vm1);

    //Destroy the current data
    invokeInEveryVM(new SerializableRunnable("Clean disk dirs") {
      public void run() {
        try {
          cleanDiskDirs();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    restoreBackup(2);

    getLogWriter().info("Creating region in VM0");
    AsyncInvocation async0 = createPersistentRegionAsync(vm0);
    getLogWriter().info("Creating region in VM1");
    AsyncInvocation async1 = createPersistentRegionAsync(vm1);

    async0.getResult(MAX_WAIT);
    async1.getResult(MAX_WAIT);

    checkData(vm0, 0, 2, "A", "region1");
    } finally {
      //cleanup the distribution message observer
      vm0.invoke(new SerializableRunnable() {
        public void run() {
          DistributionMessageObserver.setInstance(null);
          disconnectFromDS();
        }
      });
    }
  }
  
  /**
   * Make sure we don't report members without persistent
   * data as backed up.
   * @throws Throwable
   */
  public void testBackupOverflow() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    getLogWriter().info("Creating region in VM1");
    createOverflowRegion(vm1);
    
    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");
    
    BackupStatus status = backup(vm2);
    assertEquals("Backed up disk stores  " + status, 1, status.getBackedUpDiskStores().size());
    assertEquals(2, status.getBackedUpDiskStores().values().iterator().next().size());
    assertEquals(Collections.emptySet(), status.getOfflineDiskStores());
    
    
    validateBackupComplete();
    
  }
  
  public void testBackupPRWithOfflineMembers() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    getLogWriter().info("Creating region in VM2");
    createPersistentRegion(vm2);
    
    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");
    
    closeCache(vm2);
    
    
    BackupStatus status = backup(vm3);
    assertEquals(2, status.getBackedUpDiskStores().size());
    assertEquals(2, status.getOfflineDiskStores().size());
  }
  
  

  //TODO
  //Test default disk store.
  //Test backing up and recovering while a bucket move is in progress.
  //Test backing up and recovering while ops are in progress?


  private void validateBackupComplete() {
    File backupDir = getBackupDir();
    File incompleteBackup = FileUtil.find(backupDir, ".*INCOMPLETE.*");
    assertNull(incompleteBackup);
  }
  
  protected void createPersistentRegion(VM vm) throws Throwable {
    AsyncInvocation future = createPersistentRegionAsync(vm);
    future.join(MAX_WAIT);
    if(future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    if(future.exceptionOccurred()) {
      throw new RuntimeException(future.getException());
    }
  }

  private void deleteOldUserUserFile(final VM vm) {
    SerializableRunnable validateUserFileBackup = new SerializableRunnable("set user backups") {
      public void run() {
        final int pid = vm.getPid();
        try {
          FileUtil.delete(new File("userbackup_"+pid));
        } catch (IOException e) {
          fail(e.getMessage());
        }
      }
    };
    vm.invoke(validateUserFileBackup);
  }

  protected long setBackupFiles(final VM vm) {
    SerializableCallable setUserBackups = new SerializableCallable("set user backups") {
      public Object call() {
        final int pid = DUnitEnv.get().getPid();
        File vmdir = new File("userbackup_"+pid);
        File test1 = new File(vmdir, "test1");
        File test2 = new File(test1, "test2");
        File mytext = new File(test2, "my.txt");
        final ArrayList<File> backuplist = new ArrayList<File>();
        test2.mkdirs();
        PrintStream ps = null;
        try {
          ps = new PrintStream(mytext);
        } catch (FileNotFoundException e) {
          fail(e.getMessage());
        }
        ps.println(pid);
        ps.close();
        mytext.setExecutable(true, true);
        long lastModified = mytext.lastModified();
        backuplist.add(test2);

        Cache cache = getCache();
        GemFireCacheImpl gfci = (GemFireCacheImpl)cache;
        gfci.setBackupFiles(backuplist);
        
        return lastModified;
      }
    };
    return (long) vm.invoke(setUserBackups);
  }

  protected void verifyUserFileRestored(VM vm, final long lm) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        final int pid = DUnitEnv.get().getPid();
        File vmdir = new File("userbackup_"+pid);
        File mytext = new File(vmdir, "test1/test2/my.txt");
        assertTrue(mytext.exists());
        if (System.getProperty("java.specification.version").equals("1.6")) {
          assertTrue(mytext.canExecute());
        } else {
          System.out.println("java.specification.version is "+System.getProperty("java.specification.version")+", canExecute is"+mytext.canExecute());
        }
        assertEquals(lm, mytext.lastModified());

        try {
          FileReader fr = new FileReader(mytext);
          BufferedReader bin = new BufferedReader(fr);
          String content = bin.readLine();
          assertTrue(content.equals(""+pid));
        } catch (FileNotFoundException e) {
          fail(e.getMessage());
        } catch (IOException e) {
          fail(e.getMessage());
        }
      }
    });
  }

  protected AsyncInvocation createPersistentRegionAsync(final VM vm) {
    SerializableRunnable createRegion = new SerializableRunnable("Create persistent region") {
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(getDiskDirs(getUniqueName()));
        dsf.setMaxOplogSize(1);
        DiskStore ds = dsf.create(getUniqueName());
        
        
        RegionFactory rf = new RegionFactory();
        rf.setDiskStoreName(ds.getName());
        rf.setDiskSynchronous(true);
        rf.setDataPolicy(getDataPolicy());
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);
        rf.setPartitionAttributes(paf.create());
        rf.create("region1");
        
        dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(getDiskDirs(getUniqueName()+2));
        dsf.setMaxOplogSize(1);
        ds = dsf.create(getUniqueName()+2);
        rf.setDiskStoreName(getUniqueName()+2);
        rf.create("region2");
      }
    };
    return vm.invokeAsync(createRegion);
  }
  
  protected void createOverflowRegion(final VM vm) {
    SerializableRunnable createRegion = new SerializableRunnable("Create persistent region") {
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(getDiskDirs(getUniqueName()));
        dsf.setMaxOplogSize(1);
        DiskStore ds = dsf.create(getUniqueName());
        
        
        RegionFactory rf = new RegionFactory();
        rf.setDiskStoreName(ds.getName());
        rf.setDiskSynchronous(true);
        rf.setDataPolicy(DataPolicy.REPLICATE);
        rf.setEvictionAttributes(EvictionAttributes.createLIFOEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
        rf.create("region3");
      }
    };
    vm.invoke(createRegion);
  }

  protected void createData(VM vm, final int startKey, final int endKey,
      final String value) {
    createData(vm, startKey, endKey,value, PR_REGION_NAME);
  }

  protected void createData(VM vm, final int startKey, final int endKey,
      final String value, final String regionName) {
    SerializableRunnable createData = new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        
        for(int i =startKey; i < endKey; i++) {
          region.put(i, value);
        }
      }
    };
    vm.invoke(createData);
  }
  
  protected void checkData(VM vm0, final int startKey, final int endKey,
      final String value) {
    checkData(vm0, startKey, endKey, value, PR_REGION_NAME);
  }

  protected void checkData(VM vm0, final int startKey, final int endKey,
      final String value, final String regionName) {
    SerializableRunnable checkData = new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        
        for(int i =startKey; i < endKey; i++) {
          assertEquals(value, region.get(i));
        }
      }
    };
    
    vm0.invoke(checkData);
  }
  
  protected void closeCache(final VM vm) {
    SerializableRunnable closeCache = new SerializableRunnable("close cache") {
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    };
    vm.invoke(closeCache);
  }
  
  protected Set<Integer> getBucketList(VM vm0) {
    return getBucketList(vm0, PR_REGION_NAME);
  }

  protected Set<Integer> getBucketList(VM vm0, final String regionName) {
    SerializableCallable getBuckets = new SerializableCallable("get buckets") {
      
      public Object call() throws Exception {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        return new TreeSet<Integer>(region.getDataStore().getAllLocalBucketIds());
      }
    };
    
    return (Set<Integer>) vm0.invoke(getBuckets);
  }
  
  public File[] getDiskDirs(String dsName) {
    File[] dirs  =getDiskDirs();
    File[] diskStoreDirs = new File[1];
    diskStoreDirs[0] = new File(dirs[0], dsName);
    diskStoreDirs[0].mkdirs();
    return diskStoreDirs;
  }
  
  protected DataPolicy getDataPolicy() {
    return DataPolicy.PERSISTENT_PARTITION;
  }

  private static class SerializableDistributionMessageObserver extends DistributionMessageObserver implements Serializable {
    
  }

}
