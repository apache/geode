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

import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
import org.apache.geode.internal.cache.DestroyRegionOperation.DestroyRegionMessage;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.PersistentPartitionedRegionTestBase;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DUnitEnv;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Additional tests to consider adding:
 * <ul>
 * <li>Test default disk store.
 * <li>Test backing up and recovering while a bucket move is in progress.
 * <li>Test backing up and recovering while ops are in progress?
 * </ul>
 */
@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class BackupDistributedTest extends PersistentPartitionedRegionTestBase {

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Before
  public void setUp() throws Exception {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(null);
      disconnectFromDS();
    });

    StringBuilder failures = new StringBuilder();
    delete(getBackupDir(), failures);
    if (failures.length() > 0) {
      // logger.error(failures.toString());
    }
  }

  @Test
  public void testBackupPR() throws Exception {
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    long lm0 = setBackupFiles(vm0);
    long lm1 = setBackupFiles(vm1);

    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");

    BackupStatus status = backup(vm2);
    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();

    Collection<File> files = FileUtils.listFiles(getBackupDir(), new String[] {"txt"}, true);
    assertThat(files).hasSize(4);

    deleteOldUserUserFile(vm0);
    deleteOldUserUserFile(vm1);
    validateBackupComplete();

    createData(vm0, 0, 5, "C", "region1");
    createData(vm0, 0, 5, "C", "region2");

    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();

    closeCache(vm0);
    closeCache(vm1);

    // destroy the current data
    cleanDiskDirsInEveryVM();

    restoreBackup(2);

    createPersistentRegionsAsync();

    checkData(vm0, 0, 5, "A", "region1");
    checkData(vm0, 0, 5, "B", "region2");
    verifyUserFileRestored(vm0, lm0);
    verifyUserFileRestored(vm1, lm1);
  }

  /**
   * Test of bug 42419.
   *
   * <p>
   * TRAC 42419: backed up disk stores map contains null key instead of member; cannot restore
   * backup files
   */
  @Test
  public void testBackupFromMemberWithDiskStore() throws Exception {
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");

    BackupStatus status = backup(vm1);
    assertThat(status.getBackedUpDiskStores()).hasSize(2);

    for (DistributedMember key : status.getBackedUpDiskStores().keySet()) {
      assertThat(key).isNotNull();
    }
    assertThat(status.getOfflineDiskStores()).isEmpty();

    validateBackupComplete();

    closeCache(vm0);
    closeCache(vm1);

    // destroy the current data
    invokeInEveryVM("cleanDiskDirs", () -> cleanDiskDirs());

    restoreBackup(2);

    createPersistentRegionsAsync();

    checkData(vm0, 0, 5, "A", "region1");
    checkData(vm0, 0, 5, "B", "region2");
  }

  /**
   * Test for bug 42419
   *
   * <p>
   * TRAC 42419: backed up disk stores map contains null key instead of member; cannot restore
   * backup files
   */
  @Test
  public void testBackupWhileBucketIsCreated() throws Exception {
    createPersistentRegion(vm0);

    // create a bucket on vm0
    createData(vm0, 0, 1, "A", "region1");

    // create the pr on vm1, which won't have any buckets
    createPersistentRegion(vm1);

    CompletableFuture<BackupStatus> backupStatusFuture =
        CompletableFuture.supplyAsync(() -> backup(vm2));
    CompletableFuture<Void> createDataFuture =
        CompletableFuture.runAsync(() -> createData(vm0, 1, 5, "A", "region1"));
    CompletableFuture.allOf(backupStatusFuture, createDataFuture);

    BackupStatus status = backupStatusFuture.get();
    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();

    validateBackupComplete();

    createData(vm0, 0, 5, "C", "region1");

    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();

    closeCache(vm0);
    closeCache(vm1);

    // destroy the current data
    invokeInEveryVM("cleanDiskDirs", () -> cleanDiskDirs());

    restoreBackup(2);

    createPersistentRegionsAsync();

    checkData(vm0, 0, 1, "A", "region1");
  }

  /**
   * Test for bug 42420. Invoke a backup when a bucket is in the middle of being moved.
   *
   * <p>
   * TRAC 42420: Online backup files sometimes cannot be restored
   */
  @Test
  @Parameters({"BEFORE_SENDING_DESTROYREGIONMESSAGE", "BEFORE_PROCESSING_REPLYMESSAGE"})
  @TestCaseName("{method}({params})")
  public void testWhileBucketIsMovedBackup(final WhenToInvokeBackup whenToInvokeBackup)
      throws Exception {
    vm0.invoke("Add listener to invoke backup", () -> {
      disconnectFromDS();

      // This listener will wait for a response to the
      // destroy region message, and then trigger a backup.
      // That will backup before this member has finished destroying
      // a bucket, but after the peer has removed the bucket.
      DistributionMessageObserver.setInstance(createTestHookToBackup(whenToInvokeBackup));
    });

    createPersistentRegion(vm0);

    // create twos bucket on vm0
    createData(vm0, 0, 2, "A", "region1");

    // create the pr on vm1, which won't have any buckets
    createPersistentRegion(vm1);

    // Perform a rebalance. This will trigger the backup in the middle of the bucket move.
    vm0.invoke("Do rebalance", () -> {
      RebalanceOperation op = getCache().getResourceManager().createRebalanceFactory().start();
      RebalanceResults results;
      try {
        results = op.getResults();
        assertEquals(1, results.getTotalBucketTransfersCompleted());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    validateBackupComplete();

    createData(vm0, 0, 5, "C", "region1");

    closeCache(vm0);
    closeCache(vm1);

    // Destroy the current data
    invokeInEveryVM("Clean disk dirs", () -> {
      cleanDiskDirs();
    });

    restoreBackup(2);

    createPersistentRegionsAsync();

    checkData(vm0, 0, 2, "A", "region1");
  }

  @Test
  public void testBackupStatusCleanedUpAfterFailureOnOneMember() throws Exception {
    addIgnoredException("Uncaught exception");
    addIgnoredException("Stop processing");

    String exceptionMessage = "Backup in progress";

    vm0.invoke(() -> {
      disconnectFromDS();
      // create an observer that will fail a backup when this member receives a prepare
      DistributionMessageObserver.setInstance(
          createTestHookToThrowIOExceptionBeforeProcessingPrepareBackupRequest(exceptionMessage));
    });

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");

    assertThatThrownBy(() -> backup(vm2)).hasRootCauseInstanceOf(IOException.class);

    // second backup should succeed because the observer and backup state has been cleared
    BackupStatus status = backup(vm2);
    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();
  }

  /**
   * Make sure we don't report members without persistent data as backed up.
   */
  @Test
  public void testBackupOverflow() throws Exception {
    createPersistentRegion(vm0);
    createOverflowRegion(vm1);

    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");

    BackupStatus status = backup(vm2);
    assertThat(status.getBackedUpDiskStores()).hasSize(1);
    assertThat(status.getBackedUpDiskStores().values().iterator().next()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();

    validateBackupComplete();
  }

  @Test
  public void testBackupPRWithOfflineMembers() throws Exception {
    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    createPersistentRegion(vm2);

    createData(vm0, 0, 5, "A", "region1");
    createData(vm0, 0, 5, "B", "region2");

    closeCache(vm2);

    BackupStatus status = backup(vm3);
    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).hasSize(2);
  }

  private DistributionMessageObserver createTestHookToBackup(
      WhenToInvokeBackup backupInvocationTestHook) {
    switch (backupInvocationTestHook) {
      case BEFORE_SENDING_DESTROYREGIONMESSAGE:
        return createTestHookToBackupBeforeSendingDestroyRegionMessage(() -> backup(vm2));
      case BEFORE_PROCESSING_REPLYMESSAGE:
        return createTestHookToBackupBeforeProcessingReplyMessage(() -> backup(vm2));
      default:
        throw new AssertionError("Invalid backupInvocationTestHook " + backupInvocationTestHook);
    }
  }

  private DistributionMessageObserver createTestHookToBackupBeforeProcessingReplyMessage(
      Runnable task) {
    return new DistributionMessageObserver() {
      private volatile boolean done;
      private final AtomicInteger count = new AtomicInteger();
      private volatile int replyId = -0xBAD;

      @Override
      public void beforeSendMessage(DistributionManager dm, DistributionMessage message) {
        // the bucket move will send a destroy region message.
        if (message instanceof DestroyRegionMessage && !done) {
          this.replyId = message.getProcessorId();
        }
      }

      @Override
      public void beforeProcessMessage(DistributionManager dm, DistributionMessage message) {
        if (message instanceof ReplyMessage && replyId != -0xBAD
            && replyId == message.getProcessorId() && !done && count.incrementAndGet() == 2) {
          task.run();
          done = true;
        }
      }
    };
  }

  private DistributionMessageObserver createTestHookToBackupBeforeSendingDestroyRegionMessage(
      Runnable task) {
    return new DistributionMessageObserver() {
      private volatile boolean done;

      @Override
      public void beforeSendMessage(DistributionManager dm, DistributionMessage message) {
        // the bucket move will send a destroy region message.
        if (message instanceof DestroyRegionMessage && !done) {
          task.run();
          done = true;
        }
      }
    };
  }

  private void cleanDiskDirsInEveryVM() {
    invokeInEveryVM("cleanDiskDirsInEveryVM", () -> {
      cleanDiskDirs();
    });
  }

  private DistributionMessageObserver createTestHookToThrowIOExceptionBeforeProcessingPrepareBackupRequest(
      final String exceptionMessage) {
    return new DistributionMessageObserver() {
      @Override
      public void beforeProcessMessage(DistributionManager dm, DistributionMessage message) {
        if (message instanceof PrepareBackupRequest) {
          DistributionMessageObserver.setInstance(null);
          IOException exception = new IOException(exceptionMessage);
          AdminFailureResponse response =
              AdminFailureResponse.create(message.getSender(), exception);
          response.setMsgId(((PrepareBackupRequest) message).getMsgId());
          dm.putOutgoing(response);
          throw new RuntimeException("Stop processing");
        }
      }
    };
  }

  private void createPersistentRegionsAsync() throws ExecutionException, InterruptedException {
    AsyncInvocation async0 = createPersistentRegionAsync(vm0);
    AsyncInvocation async1 = createPersistentRegionAsync(vm1);
    async0.await();
    async1.await();
  }

  private void validateBackupComplete() {
    File backupDir = getBackupDir();
    Pattern pattern = Pattern.compile(".*INCOMPLETE.*");
    File[] files = backupDir.listFiles((dir1, name) -> pattern.matcher(name).matches());
    assertNotNull(files);
    assertTrue(files.length == 0);
  }

  private void createPersistentRegion(VM vm) throws Exception {
    createPersistentRegionAsync(vm).await();
  }

  private void deleteOldUserUserFile(final VM vm) {
    SerializableRunnable validateUserFileBackup = new SerializableRunnable("set user backups") {
      @Override
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
      @Override
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
      @Override
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
      @Override
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
      @Override
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

  @Override
  protected void createData(VM vm, final int startKey, final int endKey, final String value) {
    createData(vm, startKey, endKey, value, getPartitionedRegionName());
  }

  @Override
  protected void createData(VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    SerializableRunnable createData = new SerializableRunnable() {

      @Override
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

  @Override
  protected void checkData(VM vm, final int startKey, final int endKey, final String value) {
    checkData(vm, startKey, endKey, value, getPartitionedRegionName());
  }

  @Override
  protected void checkData(VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    SerializableRunnable checkData = new SerializableRunnable() {

      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);

        for (int i = startKey; i < endKey; i++) {
          assertEquals(value, region.get(i));
        }
      }
    };

    vm.invoke(checkData);
  }

  @Override
  protected void closeCache(final VM vm) {
    SerializableRunnable closeCache = new SerializableRunnable("close cache") {
      @Override
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    };
    vm.invoke(closeCache);
  }

  @Override
  protected Set<Integer> getBucketList(VM vm) {
    return getBucketList(vm, getPartitionedRegionName());
  }

  @Override
  protected Set<Integer> getBucketList(VM vm, final String regionName) {
    SerializableCallable getBuckets = new SerializableCallable("get buckets") {

      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        return new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
      }
    };

    return (Set<Integer>) vm.invoke(getBuckets);
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

  void checkRecoveredFromDisk(VM vm, final int bucketId, final boolean recoveredLocally) {
    vm.invoke(new SerializableRunnable("check recovered from disk") {
      @Override
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(getPartitionedRegionName());
        DiskRegion disk = region.getRegionAdvisor().getBucket(bucketId).getDiskRegion();
        if (recoveredLocally) {
          assertEquals(0, disk.getStats().getRemoteInitializations());
          assertEquals(1, disk.getStats().getLocalInitializations());
        } else {
          assertEquals(1, disk.getStats().getRemoteInitializations());
          assertEquals(0, disk.getStats().getLocalInitializations());
        }
      }
    });
  }

  /**
   * Recursively delete a file or directory. A description of any files or directories that can not
   * be deleted will be added to failures if failures is non-null. This method tries to delete as
   * much as possible.
   */
  public static void delete(File file, StringBuilder failures) {
    if (!file.exists()) {
      return;
    }

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

  enum WhenToInvokeBackup {
    BEFORE_SENDING_DESTROYREGIONMESSAGE, BEFORE_PROCESSING_REPLYMESSAGE
  }
}
