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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.io.FileUtils;
import org.apache.geode.admin.internal.PrepareBackupRequest;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.internal.admin.remote.AdminFailureResponse;
import org.apache.geode.internal.cache.partitioned.PersistentPartitionedRegionTestBase;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DUnitEnv;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@Category(DistributedTest.class)
public class BackupDUnitTest extends PersistentPartitionedRegionTestBase {
  Logger logger = LogManager.getLogger(BackupDUnitTest.class);

  private static final long MAX_WAIT_SECONDS = 30;
  private VM vm0;
  private VM vm1;

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    StringBuilder failures = new StringBuilder();
    delete(getBackupDir(), failures);
    if (failures.length() > 0) {
      logger.error(failures.toString());
    }
  }

  @Test
  public void testBackupPR() throws Throwable {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    logger.info("Creating region in VM0");
    createPersistentRegion(vm0);
    logger.info("Creating region in VM1");
    createPersistentRegion(vm1);

    long lm0 = setBackupFiles(vm0);
    long lm1 = setBackupFiles(vm1);

    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");

    BackupStatus status = backup(vm2);
    assertEquals(2, status.getBackedUpDiskStores().size());
    assertEquals(Collections.emptySet(), status.getOfflineDiskStores());

    Collection<File> files = FileUtils.listFiles(getBackupDir(), new String[] {"txt"}, true);
    assertEquals(4, files.size());
    deleteOldUserUserFile(vm0);
    deleteOldUserUserFile(vm1);
    validateBackupComplete();

    createData(vm0, 0, 5, "C", "region1");
    createData(vm0, 0, 5, "C", "region2");

    assertEquals(2, status.getBackedUpDiskStores().size());
    assertEquals(Collections.emptySet(), status.getOfflineDiskStores());

    closeCache(vm0);
    closeCache(vm1);

    // Destroy the current data
    Invoke.invokeInEveryVM(new SerializableRunnable("Clean disk dirs") {
      public void run() {
        try {
          cleanDiskDirs();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    restoreBackup(2);

    createPersistentRegionsAsync();

    checkData(vm0, 0, 5, "A", "region1");
    checkData(vm0, 0, 5, "B", "region2");
    verifyUserFileRestored(vm0, lm0);
    verifyUserFileRestored(vm1, lm1);
  }

  /**
   * Test of bug 42419
   */
  @Test
  public void testBackupFromMemberWithDiskStore() throws Throwable {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);

    logger.info("Creating region in VM0");
    createPersistentRegion(vm0);
    logger.info("Creating region in VM1");
    createPersistentRegion(vm1);

    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");

    BackupStatus status = backup(vm1);
    assertEquals(2, status.getBackedUpDiskStores().size());
    for (DistributedMember key : status.getBackedUpDiskStores().keySet()) {
      assertNotNull(key);
    }
    assertEquals(Collections.emptySet(), status.getOfflineDiskStores());

    validateBackupComplete();

    closeCache(vm0);
    closeCache(vm1);

    // Destroy the current data
    Invoke.invokeInEveryVM(new SerializableRunnable("Clean disk dirs") {
      public void run() {
        try {
          cleanDiskDirs();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    restoreBackup(2);

    createPersistentRegionsAsync();

    checkData(vm0, 0, 5, "A", "region1");
    checkData(vm0, 0, 5, "B", "region2");
  }

  private void createPersistentRegionsAsync() throws java.util.concurrent.ExecutionException,
      InterruptedException, java.util.concurrent.TimeoutException {
    logger.info("Creating region in VM0");
    AsyncInvocation async0 = createPersistentRegionAsync(vm0);
    logger.info("Creating region in VM1");
    AsyncInvocation async1 = createPersistentRegionAsync(vm1);
    async0.get(MAX_WAIT_SECONDS, TimeUnit.SECONDS);
    async1.get(MAX_WAIT_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Test for bug 42419
   */
  @Test
  public void testBackupWhileBucketIsCreated() throws Throwable {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);

    logger.info("Creating region in VM0");
    createPersistentRegion(vm0);

    // create a bucket on vm0
    createData(vm0, 0, 1, "A", "region1");

    // create the pr on vm1, which won't have any buckets
    logger.info("Creating region in VM1");
    createPersistentRegion(vm1);

    CompletableFuture<BackupStatus> backupStatusFuture =
        CompletableFuture.supplyAsync(() -> backup(vm2));
    CompletableFuture<Void> createDataFuture =
        CompletableFuture.runAsync(() -> createData(vm0, 1, 5, "A", "region1"));
    CompletableFuture.allOf(backupStatusFuture, createDataFuture);

    BackupStatus status = backupStatusFuture.get();
    assertEquals(2, status.getBackedUpDiskStores().size());
    assertEquals(Collections.emptySet(), status.getOfflineDiskStores());

    validateBackupComplete();

    createData(vm0, 0, 5, "C", "region1");

    assertEquals(2, status.getBackedUpDiskStores().size());
    assertEquals(Collections.emptySet(), status.getOfflineDiskStores());

    closeCache(vm0);
    closeCache(vm1);

    // Destroy the current data
    Invoke.invokeInEveryVM(new SerializableRunnable("Clean disk dirs") {
      public void run() {
        try {
          cleanDiskDirs();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    restoreBackup(2);

    createPersistentRegionsAsync();

    checkData(vm0, 0, 1, "A", "region1");
  }

  @Test
  public void testBackupWhileBucketIsMovedBackupBeforeSendDestroy() throws Throwable {
    Host host = Host.getHost(0);
    final VM vm2 = host.getVM(2);

    DistributionMessageObserver observer = new SerializableDistributionMessageObserver() {
      private volatile boolean done;

      @Override
      public void beforeSendMessage(DistributionManager dm, DistributionMessage msg) {

        // The bucket move will send a destroy region message.
        if (msg instanceof DestroyRegionOperation.DestroyRegionMessage && !done) {
          backup(vm2);
          done = true;
        }
      }
    };

    backupWhileBucketIsMoved(observer);
  }

  @Test
  public void testBackupWhileBucketIsMovedBackupAfterSendDestroy() throws Throwable {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);

    DistributionMessageObserver observer = new SerializableDistributionMessageObserver() {
      private volatile boolean done;
      private AtomicInteger count = new AtomicInteger();
      private volatile int replyId = -0xBAD;

      @Override
      public void beforeSendMessage(DistributionManager dm, DistributionMessage msg) {

        // The bucket move will send a destroy region message.
        if (msg instanceof DestroyRegionOperation.DestroyRegionMessage && !done) {
          this.replyId = msg.getProcessorId();
        }
      }

      @Override
      public void beforeProcessMessage(DistributionManager dm, DistributionMessage message) {
        if (message instanceof ReplyMessage && replyId != -0xBAD
            && replyId == message.getProcessorId() && !done
        // we need two replies
            && count.incrementAndGet() == 2) {
          backup(vm2);
          done = true;
        }
      }
    };

    backupWhileBucketIsMoved(observer);
  }

  @Test
  public void testBackupStatusCleanedUpAfterFailureOnOneMember() throws Throwable {
    IgnoredException.addIgnoredException("Uncaught exception");
    IgnoredException.addIgnoredException("Stop processing");
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);

    // Create an observer that will fail a backup
    // When this member receives a prepare
    DistributionMessageObserver observer = new SerializableDistributionMessageObserver() {
      @Override
      public void beforeProcessMessage(DistributionManager dm, DistributionMessage message) {
        if (message instanceof PrepareBackupRequest) {
          DistributionMessageObserver.setInstance(null);
          IOException exception = new IOException("Backup in progess");
          AdminFailureResponse response =
              AdminFailureResponse.create(dm, message.getSender(), exception);
          response.setMsgId(((PrepareBackupRequest) message).getMsgId());
          dm.putOutgoing(response);
          throw new RuntimeException("Stop processing");
        }
      }
    };

    vm0.invoke(() -> {
      disconnectFromDS();
      DistributionMessageObserver.setInstance(observer);
    });

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");

    try {
      backup(vm2);
      fail("Backup should have failed with in progress exception");
    } catch (Exception expected) {
      // that's ok, hte backup should have failed
    }

    // A second backup should succeed because the observer
    // has been cleared and the backup state should be cleared.
    BackupStatus status = backup(vm2);
    assertEquals(2, status.getBackedUpDiskStores().size());
    assertEquals(Collections.emptySet(), status.getOfflineDiskStores());


  }

  /**
   * Test for bug 42420. Invoke a backup when a bucket is in the middle of being moved.
   * 
   * @param observer - a message observer that triggers at the backup at the correct time.
   */
  private void backupWhileBucketIsMoved(final DistributionMessageObserver observer)
      throws Throwable {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);

    vm0.invoke(new SerializableRunnable("Add listener to invoke backup") {

      public void run() {
        disconnectFromDS();

        // This listener will wait for a response to the
        // destroy region message, and then trigger a backup.
        // That will backup before this member has finished destroying
        // a bucket, but after the peer has removed the bucket.
        DistributionMessageObserver.setInstance(observer);
      }
    });
    try {

      logger.info("Creating region in VM0");
      createPersistentRegion(vm0);

      // create twos bucket on vm0
      createData(vm0, 0, 2, "A", "region1");

      // create the pr on vm1, which won't have any buckets
      logger.info("Creating region in VM1");

      createPersistentRegion(vm1);

      // Perform a rebalance. This will trigger the backup in the middle
      // of the bucket move.
      vm0.invoke(new SerializableRunnable("Do rebalance") {

        public void run() {
          Cache cache = getCache();
          RebalanceOperation op = cache.getResourceManager().createRebalanceFactory().start();
          RebalanceResults results;
          try {
            results = op.getResults();
            assertEquals(1, results.getTotalBucketTransfersCompleted());
          } catch (Exception e) {
            Assert.fail("interupted", e);
          }
        }
      });

      validateBackupComplete();

      createData(vm0, 0, 5, "C", "region1");

      closeCache(vm0);
      closeCache(vm1);

      // Destroy the current data
      Invoke.invokeInEveryVM(new SerializableRunnable("Clean disk dirs") {
        public void run() {
          try {
            cleanDiskDirs();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });

      restoreBackup(2);

      createPersistentRegionsAsync();

      checkData(vm0, 0, 2, "A", "region1");
    } finally {
      // cleanup the distribution message observer
      vm0.invoke(new SerializableRunnable() {
        public void run() {
          DistributionMessageObserver.setInstance(null);
          disconnectFromDS();
        }
      });
    }
  }

  /**
   * Make sure we don't report members without persistent data as backed up.
   */
  @Test
  public void testBackupOverflow() throws Throwable {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    logger.info("Creating region in VM0");
    createPersistentRegion(vm0);
    logger.info("Creating region in VM1");
    createOverflowRegion(vm1);

    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");

    BackupStatus status = backup(vm2);
    assertEquals("Backed up disk stores  " + status, 1, status.getBackedUpDiskStores().size());
    assertEquals(2, status.getBackedUpDiskStores().values().iterator().next().size());
    assertEquals(Collections.emptySet(), status.getOfflineDiskStores());

    validateBackupComplete();

  }

  @Test
  public void testBackupPRWithOfflineMembers() throws Throwable {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    logger.info("Creating region in VM0");
    createPersistentRegion(vm0);
    logger.info("Creating region in VM1");
    createPersistentRegion(vm1);
    logger.info("Creating region in VM2");
    createPersistentRegion(vm2);

    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");

    closeCache(vm2);

    BackupStatus status = backup(vm3);
    assertEquals(2, status.getBackedUpDiskStores().size());
    assertEquals(2, status.getOfflineDiskStores().size());
  }

  // TODO
  // Test default disk store.
  // Test backing up and recovering while a bucket move is in progress.
  // Test backing up and recovering while ops are in progress?


  private void validateBackupComplete() {
    File backupDir = getBackupDir();
    Pattern pattern = Pattern.compile(".*INCOMPLETE.*");
    File[] files = backupDir.listFiles((dir1, name) -> pattern.matcher(name).matches());
    assertNotNull(files);
    assertTrue(files.length == 0);
  }

  private void createPersistentRegion(VM vm) throws Throwable {
    AsyncInvocation future = createPersistentRegionAsync(vm);
    future.get(MAX_WAIT_SECONDS, TimeUnit.SECONDS);
    if (future.isAlive()) {
      fail("Region not created within" + MAX_WAIT_SECONDS);
    }
    if (future.exceptionOccurred()) {
      throw new RuntimeException(future.getException());
    }
  }

  private void deleteOldUserUserFile(final VM vm) {
    SerializableRunnable validateUserFileBackup = new SerializableRunnable("set user backups") {
      public void run() {
        try {
          FileUtils.deleteDirectory(new File("userbackup_" + vm.getId()));
        } catch (IOException e) {
          fail(e.getMessage());
        }
      }
    };
    vm.invoke(validateUserFileBackup);
  }

  private long setBackupFiles(final VM vm) {
    SerializableCallable setUserBackups = new SerializableCallable("set user backups") {
      public Object call() {
        final int pid = DUnitEnv.get().getPid();
        File vmdir = new File("userbackup_" + pid);
        File test1 = new File(vmdir, "test1");
        File test2 = new File(test1, "test2");
        File mytext = new File(test2, "my.txt");
        final ArrayList<File> backuplist = new ArrayList<>();
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
        GemFireCacheImpl gfci = (GemFireCacheImpl) cache;
        gfci.setBackupFiles(backuplist);

        return lastModified;
      }
    };
    return (long) vm.invoke(setUserBackups);
  }

  private void verifyUserFileRestored(VM vm, final long lm) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        final int pid = DUnitEnv.get().getPid();
        File vmdir = new File("userbackup_" + pid);
        File mytext = new File(vmdir, "test1/test2/my.txt");
        assertTrue(mytext.exists());
        if (System.getProperty("java.specification.version").equals("1.6")) {
          assertTrue(mytext.canExecute());
        } else {
          System.out.println(
              "java.specification.version is " + System.getProperty("java.specification.version")
                  + ", canExecute is" + mytext.canExecute());
        }
        assertEquals(lm, mytext.lastModified());

        try {
          FileReader fr = new FileReader(mytext);
          BufferedReader bin = new BufferedReader(fr);
          String content = bin.readLine();
          assertTrue(content.equals("" + pid));
        } catch (IOException e) {
          fail(e.getMessage());
        }
      }
    });
  }

  private AsyncInvocation createPersistentRegionAsync(final VM vm) {
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
        dsf.setDiskDirs(getDiskDirs(getUniqueName() + 2));
        dsf.setMaxOplogSize(1);
        dsf.create(getUniqueName() + 2);
        rf.setDiskStoreName(getUniqueName() + 2);
        rf.create("region2");
      }
    };
    return vm.invokeAsync(createRegion);
  }

  private void createOverflowRegion(final VM vm) {
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
        rf.setEvictionAttributes(
            EvictionAttributes.createLIFOEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
        rf.create("region3");
      }
    };
    vm.invoke(createRegion);
  }

  protected void createData(VM vm, final int startKey, final int endKey, final String value) {
    createData(vm, startKey, endKey, value, PR_REGION_NAME);
  }

  protected void createData(VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    SerializableRunnable createData = new SerializableRunnable() {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);

        for (int i = startKey; i < endKey; i++) {
          region.put(i, value);
        }
      }
    };
    vm.invoke(createData);
  }

  protected void checkData(VM vm0, final int startKey, final int endKey, final String value) {
    checkData(vm0, startKey, endKey, value, PR_REGION_NAME);
  }

  protected void checkData(VM vm0, final int startKey, final int endKey, final String value,
      final String regionName) {
    SerializableRunnable checkData = new SerializableRunnable() {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);

        for (int i = startKey; i < endKey; i++) {
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
        return new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
      }
    };

    return (Set<Integer>) vm0.invoke(getBuckets);
  }

  private File[] getDiskDirs(String dsName) {
    File[] dirs = getDiskDirs();
    File[] diskStoreDirs = new File[1];
    diskStoreDirs[0] = new File(dirs[0], dsName);
    diskStoreDirs[0].mkdirs();
    return diskStoreDirs;
  }

  private DataPolicy getDataPolicy() {
    return DataPolicy.PERSISTENT_PARTITION;
  }

  private static class SerializableDistributionMessageObserver extends DistributionMessageObserver
      implements Serializable {

  }

  /**
   * Recursively delete a file or directory. A description of any files or directories that can not
   * be deleted will be added to failures if failures is non-null. This method tries to delete as
   * much as possible.
   */
  public static void delete(File file, StringBuilder failures) {
    if (!file.exists())
      return;

    if (file.isDirectory()) {
      File[] fileList = file.listFiles();
      if (fileList != null) {
        for (File child : fileList) {
          delete(child, failures);
        }
      }
    }

    try {
      Files.delete(file.toPath());
    } catch (IOException e) {
      if (failures != null) {
        failures.append("Could not delete ").append(file).append(" due to ").append(e.getMessage())
            .append('\n');
      }
    }
  }
}
