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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.apache.commons.io.filefilter.DirectoryFileFilter.DIRECTORY;
import static org.apache.geode.cache.EvictionAction.OVERFLOW_TO_DISK;
import static org.apache.geode.cache.EvictionAttributes.createLIFOEntryAttributes;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.internal.admin.remote.AdminFailureResponse.create;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.locks.DLockRequestProcessor;
import org.apache.geode.internal.admin.remote.AdminFailureResponse;
import org.apache.geode.internal.cache.DestroyRegionOperation.DestroyRegionMessage;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.process.ProcessStreamReader;
import org.apache.geode.internal.process.ProcessStreamReader.ReadingMode;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Additional tests to consider adding:
 * <ul>
 * <li>Test default disk store.
 * <li>Test backing up and recovering while a bucket move is in progress.
 * <li>Test backing up and recovering while ops are in progress?
 * </ul>
 */
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class BackupDistributedTest extends JUnit4DistributedTestCase implements Serializable {
  private static final Logger logger = LogService.getLogger();

  private static final int NUM_BUCKETS = 15;

  private String regionName1;
  private String regionName2;
  private String regionName3;

  private File backupBaseDir;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

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
    vm2 = getVM(2);
    vm3 = getVM(3);

    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName1 = uniqueName + "_region1";
    regionName2 = uniqueName + "_region2";
    regionName3 = uniqueName + "_region3";

    backupBaseDir = temporaryFolder.newFolder("backupDir");
  }

  @After
  public void tearDown() throws Exception {
    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(null);
      disconnectFromDS();
    });

    if (process != null && process.isAlive()) {
      process.destroyForcibly();
      process.waitFor(2, MINUTES);
    }
    if (processReader != null && processReader.isRunning()) {
      processReader.stop();
    }
  }

  @Test
  public void testBackupPR() throws Exception {
    createPersistentRegions(vm0, vm1);

    long lastModified0 = vm0.invoke(() -> setBackupFiles(getDiskDirFor(vm0)));
    long lastModified1 = vm1.invoke(() -> setBackupFiles(getDiskDirFor(vm1)));

    vm0.invoke(() -> {
      createData(0, 5, "A", regionName1);
      createData(0, 5, "B", regionName2);
    });

    vm0.invoke(() -> {
      assertThat(getCache().getDistributionManager().getNormalDistributionManagerIds()).hasSize(2);
    });

    vm2.invoke(() -> {
      assertThat(getCache().getDistributionManager().getNormalDistributionManagerIds()).hasSize(3);
    });

    BackupStatus status = vm2.invoke(() -> backupMember());
    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();

    Collection<File> files = FileUtils.listFiles(backupBaseDir, new String[] {"txt"}, true);
    assertThat(files).hasSize(4);

    vm0.invoke(() -> deleteOldUserFile(getDiskDirFor(vm0)));
    vm1.invoke(() -> deleteOldUserFile(getDiskDirFor(vm1)));

    validateBackupComplete();

    vm0.invoke(() -> {
      createData(0, 5, "C", regionName1);
      createData(0, 5, "C", regionName2);
    });

    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());

    // destroy the current data
    cleanDiskDirsInEveryVM();

    restoreBackup(2);

    createPersistentRegions(vm0, vm1);

    vm0.invoke(() -> {
      validateData(0, 5, "A", regionName1);
      validateData(0, 5, "B", regionName2);
    });

    vm0.invoke(() -> verifyUserFileRestored(getDiskDirFor(vm0), lastModified0));
    vm1.invoke(() -> verifyUserFileRestored(getDiskDirFor(vm1), lastModified1));
  }

  @Test
  public void testBackupDuringGII() throws Exception {
    getBlackboard().initBlackboard();
    String diskStoreName = getUniqueName();

    vm0.invoke(() -> createReplicatedRegion(regionName1, diskStoreName, getDiskStoreFor(vm0), true,
        false));
    vm0.invoke(() -> createReplicatedRegion(regionName2, diskStoreName, getDiskStoreFor(vm0), true,
        false));

    vm1.invoke(
        () -> createReplicatedRegion(regionName1, diskStoreName, getDiskStoreFor(vm1), true, true));

    vm0.invoke(() -> {
      createData(0, 5, "A", regionName1);
      createData(0, 5, "B", regionName2);
    });

    vm2.invoke(() -> {
      DistributionMessageObserver.setInstance(
          createTestHookToCreateRegionBeforeSendingPrepareBackupRequest());
    });

    AsyncInvocation asyncVM1CreateRegion = vm1.invokeAsync(() -> {
      getBlackboard().waitForGate("createRegion", 30, SECONDS);
      createReplicatedRegion(regionName2, diskStoreName, getDiskStoreFor(vm1), false, true);
      getBlackboard().signalGate("createdRegion");
    });


    vm2.invoke(() -> backupMember());
    validateBackupComplete();
    vm2.invoke(() -> getCache().close());

    asyncVM1CreateRegion.join();

    vm0.invoke(() -> {
      validateData(0, 5, "A", regionName1);
      validateData(0, 5, "B", regionName2);
    });

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());

    // destroy the current data
    cleanDiskDirsInEveryVM();

    restoreBackup(2);

    vm1.invoke(() -> {
      getCache().getPartitionedRegionLockService().becomeLockGrantor();
    });

    AsyncInvocation asyncVM1Region1 = vm1.invokeAsync(() -> {
      createReplicatedRegion(regionName1, diskStoreName, getDiskStoreFor(vm1), true, true);
    });

    AsyncInvocation asyncVM1Region2 = vm1.invokeAsync(() -> {
      createReplicatedRegion(regionName2, diskStoreName, getDiskStoreFor(vm1), false, true);
      getBlackboard().signalGate("createdRegionAfterRecovery");
    });

    vm0.invoke(() -> createReplicatedRegion(regionName1, diskStoreName, getDiskStoreFor(vm0), true,
        false));

    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(
          createTestHookToWaitForRegionCreationBeforeSendingDLockRequestMessage());
    });

    vm0.invoke(() -> createReplicatedRegion(regionName2, diskStoreName, getDiskStoreFor(vm0), true,
        false));

    asyncVM1Region1.join();
    asyncVM1Region2.join();

    vm0.invoke(() -> {
      validateData(0, 5, "A", regionName1);
      validateData(0, 5, "B", regionName2);
    });

    vm1.invoke(() -> {
      validateData(0, 5, "A", regionName1);
      validateData(0, 5, "B", regionName2);
    });
  }


  /**
   * Test of bug 42419.
   *
   * <p>
   * TRAC #42419: backed up disk stores map contains null key instead of member; cannot restore
   * backup files
   */
  @Test
  public void testBackupFromMemberWithDiskStore() throws Exception {
    createPersistentRegions(vm0, vm1);

    vm0.invoke(() -> {
      createData(0, 5, "A", regionName1);
      createData(0, 5, "B", regionName2);
    });

    BackupStatus status = vm1.invoke(() -> backupMember());
    assertThat(status.getBackedUpDiskStores()).hasSize(2);

    for (DistributedMember key : status.getBackedUpDiskStores().keySet()) {
      assertThat(key).isNotNull();
    }
    assertThat(status.getOfflineDiskStores()).isEmpty();

    validateBackupComplete();

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());

    // destroy the current data
    cleanDiskDirsInEveryVM();

    restoreBackup(2);

    createPersistentRegions(vm0, vm1);

    vm0.invoke(() -> {
      validateData(0, 5, "A", regionName1);
      validateData(0, 5, "B", regionName2);
    });
  }

  /**
   * Test for bug 42419
   *
   * <p>
   * TRAC #42419: backed up disk stores map contains null key instead of member; cannot restore
   * backup files
   */
  @Test
  public void testBackupWhileBucketIsCreated() throws Exception {
    vm0.invoke(() -> createRegions(vm0));

    // create a bucket on vm0
    vm0.invoke(() -> createData(0, 1, "A", regionName1));

    // create the pr on vm1, which won't have any buckets
    vm1.invoke(() -> createRegions(vm1));

    CompletableFuture<BackupStatus> backupStatusFuture =
        CompletableFuture.supplyAsync(() -> vm2.invoke(() -> backupMember()));

    CompletableFuture<Void> createDataFuture =
        CompletableFuture.runAsync(() -> vm0.invoke(() -> createData(1, 5, "A", regionName1)));

    CompletableFuture.allOf(backupStatusFuture, createDataFuture);

    BackupStatus status = backupStatusFuture.get();
    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();

    validateBackupComplete();

    vm0.invoke(() -> createData(0, 5, "C", regionName1));

    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());

    // destroy the current data
    cleanDiskDirsInEveryVM();

    restoreBackup(2);

    createPersistentRegions(vm0, vm1);

    vm0.invoke(() -> validateData(0, 1, "A", regionName1));

  }

  /**
   * Test for bug 42420. Invoke a backup when a bucket is in the middle of being moved.
   *
   * <p>
   * TRAC #42420: Online backup files sometimes cannot be restored
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

    vm0.invoke(() -> createRegions(vm0));

    // create twos bucket on vm0
    vm0.invoke(() -> createData(0, 2, "A", regionName1));

    // create the pr on vm1, which won't have any buckets
    vm1.invoke(() -> createRegions(vm1));

    // Perform a rebalance. This will trigger the backup in the middle of the bucket move.
    vm0.invoke("Do rebalance", () -> {
      RebalanceOperation op = getCache().getResourceManager().createRebalanceFactory().start();
      RebalanceResults results = op.getResults();
      assertThat(results.getTotalBucketTransfersCompleted()).isEqualTo(1);
    });

    validateBackupComplete();

    vm0.invoke(() -> createData(0, 5, "C", regionName1));

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());

    // Destroy the current data
    cleanDiskDirsInEveryVM();

    restoreBackup(2);

    createPersistentRegions(vm0, vm1);

    vm0.invoke(() -> validateData(0, 2, "A", regionName1));
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

    createPersistentRegions(vm0, vm1);

    vm0.invoke(() -> {
      createData(0, 5, "A", regionName1);
      createData(0, 5, "B", regionName2);
    });

    assertThatThrownBy(() -> vm2.invoke(() -> backupMember()))
        .hasRootCauseInstanceOf(IOException.class);

    // second backup should succeed because the observer and backup state has been cleared
    BackupStatus status = vm2.invoke(() -> backupMember());
    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();
  }

  /**
   * Make sure we don't report members without persistent data as backed up.
   */
  @Test
  public void testBackupOverflow() {
    vm0.invoke(() -> createRegions(vm0));
    vm1.invoke(() -> createRegionWithOverflow(getDiskStoreFor(vm1)));

    vm0.invoke(() -> {
      createData(0, 5, "A", regionName1);
      createData(0, 5, "B", regionName2);
    });

    BackupStatus status = vm2.invoke(() -> backupMember());
    assertThat(status.getBackedUpDiskStores()).hasSize(1);
    assertThat(status.getBackedUpDiskStores().values().iterator().next()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).isEmpty();

    validateBackupComplete();
  }

  @Test
  public void testBackupPRWithOfflineMembers() {
    vm0.invoke(() -> createRegions(vm0));
    vm1.invoke(() -> createRegions(vm1));
    vm2.invoke(() -> createRegions(vm2));

    vm0.invoke(() -> {
      createData(0, 5, "A", regionName1);
      createData(0, 5, "B", regionName2);
    });

    vm1.invoke(() -> getCache().close());

    BackupStatus status = vm3.invoke(() -> backupMember());
    assertThat(status.getBackedUpDiskStores()).hasSize(2);
    assertThat(status.getOfflineDiskStores()).hasSize(2);
  }

  /**
   * Test what happens when we restart persistent members while there is an accessor concurrently
   * performing puts.
   */
  @Test
  public void testRecoverySystemWithConcurrentPutter() throws Exception {
    vm1.invoke(() -> createColocatedRegions(vm1));
    vm2.invoke(() -> createColocatedRegions(vm2));

    vm0.invoke(() -> createAccessor());

    vm0.invoke(() -> {
      createData(0, NUM_BUCKETS, "a", regionName1);
      createData(0, NUM_BUCKETS, "a", regionName2);
    });

    // backup the system. We use this to get a snapshot of vm1 and vm2
    // when they both are online. Recovering from this backup simulates
    // a simultaneous kill and recovery.
    vm3.invoke(() -> backupMember());

    vm1.invoke(() -> getCache().close());
    vm2.invoke(() -> getCache().close());

    cleanDiskDirsInEveryVM();
    restoreBackup(2);

    // in vm0, start doing a bunch of concurrent puts.
    AsyncInvocation putsInVM0 = vm0.invokeAsync(() -> {
      Region region = getCache().getRegion(regionName1);
      try {
        for (int i = 0;; i++) {
          try {
            region.get(i % NUM_BUCKETS);
          } catch (PartitionOfflineException | PartitionedRegionStorageException expected) {
            // do nothing.
          }
        }
      } catch (CacheClosedException expected) {
        // ok, we're done.
      }
    });

    AsyncInvocation createRegionsInVM1 = vm1.invokeAsync(() -> createColocatedRegions(vm1));
    AsyncInvocation createRegionsInVM2 = vm2.invokeAsync(() -> createColocatedRegions(vm2));

    createRegionsInVM1.await();
    createRegionsInVM2.await();

    // close the cache in vm0 to stop the async puts.
    vm0.invoke(() -> getCache().close());

    // make sure we didn't get an exception
    putsInVM0.await();
  }

  private DistributionMessageObserver createTestHookToBackup(
      WhenToInvokeBackup backupInvocationTestHook) {
    switch (backupInvocationTestHook) {
      case BEFORE_SENDING_DESTROYREGIONMESSAGE:
        return createTestHookToBackupBeforeSendingDestroyRegionMessage(
            () -> vm2.invoke(() -> backupMember()));
      case BEFORE_PROCESSING_REPLYMESSAGE:
        return createTestHookToBackupBeforeProcessingReplyMessage(
            () -> vm2.invoke(() -> backupMember()));
      default:
        throw new RuntimeException("Invalid backupInvocationTestHook " + backupInvocationTestHook);
    }
  }

  private DistributionMessageObserver createTestHookToBackupBeforeProcessingReplyMessage(
      final Runnable task) {
    return new DistributionMessageObserver() {

      private final AtomicInteger count = new AtomicInteger();
      private volatile int replyId = -0xBAD;
      private volatile boolean done;

      @Override
      public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
        // the bucket move will send a destroy region message.
        if (message instanceof DestroyRegionMessage && !done) {
          replyId = message.getProcessorId();
        }
      }

      @Override
      public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
        if (message instanceof ReplyMessage && replyId != -0xBAD
            && replyId == message.getProcessorId() && !done && count.incrementAndGet() == 2) {
          task.run();
          done = true;
        }
      }
    };
  }

  private DistributionMessageObserver createTestHookToBackupBeforeSendingDestroyRegionMessage(
      final Runnable task) {
    return new DistributionMessageObserver() {

      private volatile boolean done;

      @Override
      public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
        // the bucket move will send a destroy region message.
        if (message instanceof DestroyRegionMessage && !done) {
          task.run();
          done = true;
        }
      }
    };
  }

  private void cleanDiskDirsInEveryVM() throws IOException {
    FileUtils.deleteDirectory(getDiskDirFor(vm0));
    FileUtils.deleteDirectory(getDiskDirFor(vm1));
    FileUtils.deleteDirectory(getDiskDirFor(vm2));
    FileUtils.deleteDirectory(getDiskDirFor(vm3));
  }

  private DistributionMessageObserver createTestHookToThrowIOExceptionBeforeProcessingPrepareBackupRequest(
      final String exceptionMessage) {
    return new DistributionMessageObserver() {

      @Override
      public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
        if (message instanceof PrepareBackupRequest) {
          DistributionMessageObserver.setInstance(null);
          IOException exception = new IOException(exceptionMessage);
          AdminFailureResponse response = create(message.getSender(), exception);
          response.setMsgId(((PrepareBackupRequest) message).getMsgId());
          dm.putOutgoing(response);
          throw new RuntimeException("Stop processing");
        }
      }
    };
  }

  private DistributionMessageObserver createTestHookToCreateRegionBeforeSendingPrepareBackupRequest() {
    return new DistributionMessageObserver() {

      @Override
      public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
        if (message instanceof PrepareBackupRequest) {
          DistributionMessageObserver.setInstance(null);
          logger.info("#### After getting into observer to create regionName2 in vm1");
          getBlackboard().signalGate("createRegion");
          logger.info("#### After signalling create region");
          try {
            getBlackboard().waitForGate("createdRegion", 30, SECONDS);
          } catch (InterruptedException | TimeoutException ex) {
            throw new RuntimeException("never was notified region was created", ex);
          }
        }
      }
    };
  }

  private DistributionMessageObserver createTestHookToWaitForRegionCreationBeforeSendingDLockRequestMessage() {
    return new DistributionMessageObserver() {

      @Override
      public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
        if (message instanceof DLockRequestProcessor.DLockRequestMessage) {
          DistributionMessageObserver.setInstance(null);
          try {
            logger.info("#### before waiting for createdRegionAfterRecovery");
            getBlackboard().waitForGate("createdRegionAfterRecovery", 30, SECONDS);
            logger.info("#### after waiting for createdRegionAfterRecovery");
          } catch (InterruptedException | TimeoutException ex) {
            throw new RuntimeException("never was notified region was created", ex);
          }
        }
      }
    };
  }

  private void createPersistentRegions(final VM... vms)
      throws ExecutionException, InterruptedException {
    Set<AsyncInvocation> createRegionAsyncs = new HashSet<>();
    for (VM vm : vms) {
      createRegionAsyncs.add(vm.invokeAsync(() -> createRegions(vm)));
    }
    for (AsyncInvocation createRegion : createRegionAsyncs) {
      createRegion.await();
    }
  }

  private void validateBackupComplete() {
    Pattern pattern = Pattern.compile(".*INCOMPLETE.*");
    File[] files = backupBaseDir.listFiles((dir, name) -> pattern.matcher(name).matches());

    assertThat(files).isNotNull().hasSize(0);
  }

  private void deleteOldUserFile(final File dir) throws IOException {
    File userDir = new File(dir, "userbackup-");
    FileUtils.deleteDirectory(userDir);
  }

  private long setBackupFiles(final File dir) throws IOException {
    File dir1 = new File(dir, "test1");
    File dir2 = new File(dir1, "test2");
    dir2.mkdirs();

    File textFile = new File(dir2, "my.txt");
    Files.createFile(textFile.toPath());

    List<File> backupList = new ArrayList<>();
    backupList.add(dir2);
    getCache().setBackupFiles(backupList);

    return textFile.lastModified();
  }

  private void verifyUserFileRestored(final File dir, final long expectedLastModified) {
    File dir1 = new File(dir, "test1");
    File dir2 = new File(dir1, "test2");
    File textFile = new File(dir2, "my.txt");

    assertThat(textFile).exists();
    assertThat(textFile.lastModified()).isEqualTo(expectedLastModified);
  }

  private void createRegions(final VM vm) {
    createRegion(regionName1, getUniqueName() + "-1", getDiskStoreFor(vm, 1));
    createRegion(regionName2, getUniqueName() + "-2", getDiskStoreFor(vm, 2));
  }

  private void createRegion(final String regionName, final String diskStoreName,
      final File diskDir) {
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(toArray(diskDir));
    diskStoreFactory.setMaxOplogSize(1);

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(0);

    RegionFactory regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskStoreName(diskStoreFactory.create(diskStoreName).getName());
    regionFactory.setDiskSynchronous(true);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
    regionFactory.create(regionName);
  }

  private void createReplicatedRegion(final String regionName, final String diskStoreName,
      final File diskDir, boolean sync, boolean delayDiskStoreFlush) {
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(toArray(diskDir));
    diskStoreFactory.setMaxOplogSize(1);
    if (delayDiskStoreFlush) {
      diskStoreFactory.setTimeInterval(Integer.MAX_VALUE);
    }

    RegionFactory regionFactory = getCache().createRegionFactory(REPLICATE_PERSISTENT);
    regionFactory.setDiskStoreName(diskStoreFactory.create(diskStoreName).getName());
    regionFactory.setDiskSynchronous(sync);
    regionFactory.create(regionName);
  }

  private File[] toArray(final File file) {
    return new File[] {file};
  }

  private void createColocatedRegions(final VM vm) {
    DiskStoreFactory diskStoreFactory1 = getCache().createDiskStoreFactory();
    diskStoreFactory1.setDiskDirs(toArray(getDiskStoreFor(vm, 1)));
    diskStoreFactory1.setMaxOplogSize(1);

    DiskStoreFactory diskStoreFactory2 = getCache().createDiskStoreFactory();
    diskStoreFactory2.setDiskDirs(toArray(getDiskStoreFor(vm, 2)));
    diskStoreFactory2.setMaxOplogSize(1);

    PartitionAttributesFactory partitionAttributesFactory1 = new PartitionAttributesFactory();
    partitionAttributesFactory1.setRedundantCopies(0);

    RegionFactory regionFactory1 = getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory1.setPartitionAttributes(partitionAttributesFactory1.create());
    regionFactory1.setDiskStoreName(diskStoreFactory1.create(getUniqueName() + "-1").getName());
    regionFactory1.setDiskSynchronous(true);
    regionFactory1.create(regionName1);

    PartitionAttributesFactory partitionAttributesFactory2 = new PartitionAttributesFactory();
    partitionAttributesFactory2.setColocatedWith(regionName1);
    partitionAttributesFactory2.setRedundantCopies(0);

    RegionFactory regionFactory2 = getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory2.setDiskStoreName(diskStoreFactory2.create(getUniqueName() + "-2").getName());
    regionFactory2.setDiskSynchronous(true);
    regionFactory2.setPartitionAttributes(partitionAttributesFactory2.create());
    regionFactory2.create(regionName2);
  }

  private void createRegionWithOverflow(final File diskDir) {
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(toArray(diskDir));

    EvictionAttributes evictionAttributes = createLIFOEntryAttributes(1, OVERFLOW_TO_DISK);

    RegionFactory regionFactory = getCache().createRegionFactory(REPLICATE);
    regionFactory.setDiskStoreName(diskStoreFactory.create(getUniqueName()).getName());
    regionFactory.setDiskSynchronous(true);
    regionFactory.setEvictionAttributes(evictionAttributes);
    regionFactory.create(regionName3);
  }

  private void createData(final int startKey, final int endKey, final String value,
      final String regionName) {
    Region<Integer, String> region = getCache().getRegion(regionName);

    for (int i = startKey; i < endKey; i++) {
      region.put(i, value);
    }
  }

  private void validateData(final int startKey, final int endKey, final String value,
      final String regionName) {
    Region region = getCache().getRegion(regionName);

    for (int i = startKey; i < endKey; i++) {
      assertThat(region.get(i)).isEqualTo(value);
    }
  }

  private BackupStatus backupMember() {
    return new BackupOperation(getCache().getDistributionManager(), getCache())
        .backupAllMembers(backupBaseDir.toString(), null);
  }

  private void restoreBackup(final int expectedScriptCount)
      throws IOException, InterruptedException {
    Collection<File> restoreScripts =
        listFiles(backupBaseDir, new RegexFileFilter(".*restore.*"), DIRECTORY);

    assertThat(restoreScripts).hasSize(expectedScriptCount);

    for (File script : restoreScripts) {
      executeScript(script);
    }
  }

  private void executeScript(final File script) throws IOException, InterruptedException {
    process = new ProcessBuilder(script.getAbsolutePath()).redirectErrorStream(true).start();

    processReader = new ProcessStreamReader.Builder(process).inputStream(process.getInputStream())
        .inputListener(line -> logger.info("OUTPUT: {}", line))
        .readingMode(getReadingMode()).continueReadingMillis(2 * 1000).build().start();

    assertThat(process.waitFor(5, MINUTES)).isTrue();
    assertThat(process.exitValue()).isEqualTo(0);
  }

  private void createAccessor() {
    PartitionAttributesFactory partitionAttributesFactory1 = new PartitionAttributesFactory();
    partitionAttributesFactory1.setLocalMaxMemory(0);
    partitionAttributesFactory1.setRedundantCopies(0);

    RegionFactory regionFactory1 = getCache().createRegionFactory(PARTITION);
    regionFactory1.setPartitionAttributes(partitionAttributesFactory1.create());
    regionFactory1.create(regionName1);

    PartitionAttributesFactory partitionAttributesFactory2 = new PartitionAttributesFactory();
    partitionAttributesFactory2.setLocalMaxMemory(0);
    partitionAttributesFactory2.setRedundantCopies(0);
    partitionAttributesFactory2.setColocatedWith(regionName1);

    RegionFactory regionFactory2 = getCache().createRegionFactory(PARTITION);
    regionFactory2.setPartitionAttributes(partitionAttributesFactory2.create());
    regionFactory2.create(regionName2);
  }

  private InternalCache getCache() {
    return cacheRule.getOrCreateCache();
  }

  private File getDiskStoreFor(final VM vm) {
    return new File(getDiskDirFor(vm), getUniqueName());
  }

  private File getDiskStoreFor(final VM vm, final int which) {
    return new File(getDiskDirFor(vm), "vm-" + vm.getId() + "-diskstores-" + which);
  }

  private File getDiskDirFor(final VM vm) {
    return diskDirRule.getDiskDirFor(vm);
  }

  private ReadingMode getReadingMode() {
    return SystemUtils.isWindows() ? ReadingMode.NON_BLOCKING : ReadingMode.BLOCKING;
  }

  enum WhenToInvokeBackup {
    BEFORE_SENDING_DESTROYREGIONMESSAGE, BEFORE_PROCESSING_REPLYMESSAGE
  }
}
