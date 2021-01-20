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

import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.IntStream;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.backup.BackupOperation;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class PersistentRegionRecoveryDUnitTest extends JUnit4DistributedTestCase
    implements Serializable {

  private static final Logger logger = LogService.getLogger();

  private String regionName;

  private VM vm0;
  private VM vm1;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    vm0 = getVM(0);
    vm1 = getVM(1);
    regionName = getClass().getSimpleName() + "-" + testName.getMethodName();
    IgnoredException.addIgnoredException("Possible loss of quorum");
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> {
      DistributionMessageObserver.setInstance(null);
    });
  }

  @Test
  public void testRecoveryOfAsyncRegionAfterShutdownAfterGIIAndBeforeCrfWritten() {
    vm0.invoke(() -> createAsyncDiskRegion());

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.put("KEY-1", "VALUE-1");
      region.put("KEY-2", "VALUE-2");
    });

    vm1.invoke(() -> createAsyncDiskRegion(true));

    vm0.invoke(() -> cacheRule.getCache().close());

    logger.info("##### After cache close in vm0");

    logger.info("##### Before vm1 bounce");

    vm1.bounceForcibly();

    logger.info("##### After vm1 bounce");

    vm1.invoke(() -> createAsyncDiskRegion(true));

    logger.info("##### After create region in vm1");

    vm0.invoke(() -> createAsyncDiskRegion());

    logger.info("##### After create region in vm0");

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo("VALUE-1");
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-2");
    });

    vm1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo("VALUE-1");
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-2");
    });
  }

  @Test
  public void testRecoveryOfAsyncRegionAfterShutdownAndBeforeCrfWritten() {
    vm0.invoke(() -> createAsyncDiskRegion());

    vm1.invoke(() -> createAsyncDiskRegion(true));

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.put("KEY-1", "VALUE-1");
      region.put("KEY-2", "VALUE-2");
    });

    vm0.invoke(() -> cacheRule.getCache().close());

    logger.info("##### After cache close in vm0");

    logger.info("##### Before vm1 bounce");

    vm1.bounceForcibly();

    logger.info("##### After vm1 bounce");

    vm1.invoke(() -> createAsyncDiskRegion(true));

    vm0.invoke(() -> createAsyncDiskRegion());

    logger.info("##### After create region in vm1");

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.size()).isEqualTo(0);
    });

    vm1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.size()).isEqualTo(0);
    });
  }

  @Test
  public void testRecoveryOfAsyncRegionAfterShutdownUsingUntrustedRVV() {
    vm0.invoke(() -> createAsyncDiskRegion());

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.put("KEY-1", "VALUE-1");
      region.put("KEY-2", "VALUE-2");
    });

    vm1.invoke(() -> createAsyncDiskRegion(true));

    logger.info("##### Before vm1 bounce");

    vm1.bounceForcibly();

    logger.info("##### After vm1 bounce");

    vm0.invoke(() -> cacheRule.getCache().close());

    logger.info("##### After cache close in vm0");

    vm0.invoke(() -> createAsyncDiskRegion());

    logger.info("##### After create region in vm0");

    vm1.invoke(() -> createAsyncDiskRegion(true));

    logger.info("##### After create region in vm1");

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo("VALUE-1");
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-2");
    });

    vm1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo("VALUE-1");
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-2");
    });
  }

  @Test
  public void testRecoveryOfAsyncRegionAfterGiiFailureAndShutdown() throws Exception {
    getBlackboard().initBlackboard();

    vm1.invoke(() -> cacheRule.createCache());
    vm0.invoke(() -> createSyncDiskRegion()); // Sync region to get the data written to disk.

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.put("KEY-1", "VALUE-1");
      region.put("KEY-2", "VALUE-2");
    });

    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(
          new SignalBounceOnRequestImageMessageObserver(regionName, cacheRule.getCache(),
              getBlackboard()));
    });

    AsyncInvocation asyncVM1 = vm1.invokeAsync(() -> createAsyncDiskRegion());

    logger.info("##### After async create region in vm1");

    SignalBounceOnRequestImageMessageObserver.waitThenBounce(getBlackboard(), vm0);

    logger.info("##### After wait for cache close in vm0");

    vm1.bounceForcibly();

    logger.info("##### After bounce in vm1");

    asyncVM1.join();

    logger.info("##### After asyncvm1 join");

    asyncVM1 = vm1.invokeAsync(() -> createAsyncDiskRegion());

    logger.info("##### After create region in vm1");

    vm0.invoke(() -> createAsyncDiskRegion());

    asyncVM1.join();

    logger.info("##### After create region in vm0");

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo("VALUE-1");
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-2");
    });

    vm1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo("VALUE-1");
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-2");
    });
  }

  @Test
  public void testRecoveryOfSyncRegionAfterGiiFailureAndShutdown() throws Exception {
    getBlackboard().initBlackboard();

    vm1.invoke(() -> cacheRule.createCache());
    vm0.invoke(() -> createSyncDiskRegion());

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.put("KEY-1", "VALUE-1");
      region.put("KEY-2", "VALUE-2");
    });

    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(
          new SignalBounceOnRequestImageMessageObserver(regionName, cacheRule.getCache(),
              getBlackboard()));
    });

    AsyncInvocation asyncVM1 = vm1.invokeAsync(() -> createSyncDiskRegion());

    logger.info("##### After async create region in vm1");

    SignalBounceOnRequestImageMessageObserver.waitThenBounce(getBlackboard(), vm0);

    logger.info("##### After wait for cache close in vm0");

    vm1.bounceForcibly();

    logger.info("##### After bounce in vm1");

    asyncVM1.join();

    logger.info("##### After asyncvm1 join");

    asyncVM1 = vm1.invokeAsync(() -> createSyncDiskRegion());

    logger.info("##### After create region in vm1");

    vm0.invoke(() -> createSyncDiskRegion());

    asyncVM1.join();

    logger.info("##### After create region in vm0");

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo("VALUE-1");
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-2");
    });

    vm1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo("VALUE-1");
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-2");
    });
  }

  @Test
  public void testRecoveryFromBackupOfAsyncRegionAfterShutdownAfterGIIAndBeforeCrfWritten() {
    vm0.invoke(() -> createDiskRegion(false, false, "regionToGetDiskStoreCreated"));
    vm1.invoke(() -> createDiskRegion(false, true, "regionToGetDiskStoreCreated"));

    vm0.invoke(() -> createAsyncDiskRegion());

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.put("KEY-1", "VALUE-1");
      region.put("KEY-2", "VALUE-2");
    });

    vm0.invoke(() -> {
      BackupOperation backupOperation =
          new BackupOperation(cacheRule.getCache().getDistributionManager(), cacheRule.getCache());
      File dir = new File(temporaryFolder.getRoot(), "BackUpDir");
      if (!dir.exists()) {
        dir = temporaryFolder.newFolder("BackUpDir");
      }
      backupOperation.backupAllMembers(dir.getAbsolutePath(), null);
    });

    vm1.invoke(() -> createAsyncDiskRegion(true));

    vm0.invoke(() -> cacheRule.getCache().close());

    logger.info("##### After cache close in vm0");

    logger.info("##### Before vm1 bounce");

    vm1.bounceForcibly();

    logger.info("##### After vm1 bounce");

    vm1.invoke(() -> createAsyncDiskRegion(true));

    logger.info("##### After create region in vm1");

    vm0.invoke(() -> createAsyncDiskRegion());

    logger.info("##### After create region in vm0");

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo("VALUE-1");
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-2");
    });

    vm1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo("VALUE-1");
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-2");
    });
  }

  @Test
  public void testRecoveryFromBackupAndRequestingDeltaGiiDoesFullGiiIfTombstoneGCVersionDiffers()
      throws Exception {
    getBlackboard().initBlackboard();
    vm1.invoke(() -> cacheRule.createCache());

    vm0.invoke(() -> createAsyncDiskRegion(true));
    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.put("KEY-1", "VALUE-1");
      region.put("KEY-2", "VALUE-2");
      flushAsyncDiskRegion();
    });

    vm1.invoke(() -> createAsyncDiskRegion(true));
    vm1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.put("KEY-1", "VALUE-1");
      region.put("KEY-2", "VALUE-2");
      region.put("KEY-1", "VALUE-3");
      region.put("KEY-2", "VALUE-4");
      flushAsyncDiskRegion();
    });

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.destroy("KEY-1");
    });

    vm0.bounceForcibly();

    vm1.invoke(() -> flushAsyncDiskRegion());

    vm1.invoke(() -> {
      DistributionMessageObserver.setInstance(
          new DistributionMessageObserver() {
            @Override
            public void beforeProcessMessage(ClusterDistributionManager dm,
                DistributionMessage message) {
              if (message instanceof InitialImageOperation.RequestImageMessage) {
                InitialImageOperation.RequestImageMessage rim =
                    (InitialImageOperation.RequestImageMessage) message;
                if (rim.regionPath.contains(regionName)) {
                  getBlackboard().signalGate("GotRegionIIRequest");
                  await().until(() -> getBlackboard().isGateSignaled("TombstoneGCDone"));
                }
              }
            }
          });
    });

    AsyncInvocation vm0createRegion = vm0.invokeAsync(() -> createAsyncDiskRegion(true));

    vm1.invoke(() -> {
      await().until(() -> getBlackboard().isGateSignaled("GotRegionIIRequest"));
      cacheRule.getCache().getTombstoneService().forceBatchExpirationForTests(1);
      flushAsyncDiskRegion();
      getBlackboard().signalGate("TombstoneGCDone");
    });

    vm0createRegion.await();

    vm1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo(null);
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-4");
    });

    vm0.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.get("KEY-1")).isEqualTo(null);
      assertThat(region.get("KEY-2")).isEqualTo("VALUE-4");

      CachePerfStats stats = ((LocalRegion) region).getRegionPerfStats();
      assertThat(stats.getDeltaGetInitialImagesCompleted()).isEqualTo(0);
      assertThat(stats.getGetInitialImagesCompleted()).isEqualTo(1);
    });
  }

  @Test
  public void verifyPersistentRecoveryIncrementsNumOverflowBytesOnDisk() {
    // Create cache and persistent region
    vm0.invoke(() -> createSyncDiskRegion());

    // Add entries
    int numEntries = 10;
    int entrySize = 10240;
    vm0.invoke(() -> putEntries(numEntries, entrySize));

    // Close cache
    vm0.invoke(() -> cacheRule.closeAndNullCache());

    // Recreate cache and persistent region without recovering values
    vm0.invoke(() -> {
      System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
      createSyncDiskRegion();
    });

    // Verify numOverflowBytesOnDisk is set after recovery
    vm0.invoke(() -> verifyNumOverflowBytesOnDiskSet(numEntries, entrySize));
  }

  private void putEntries(int numEntries, int entrySize) {
    Region region = cacheRule.getCache().getRegion(regionName);
    IntStream.range(0, numEntries).forEach(i -> region.put(i, new byte[entrySize]));
  }

  private void verifyNumOverflowBytesOnDiskSet(int numEntries, int entrySize) {
    LocalRegion region = (LocalRegion) cacheRule.getCache().getRegion(regionName);
    assertThat(region.getDiskRegion().getStats().getNumOverflowBytesOnDisk())
        .isEqualTo(numEntries * entrySize);
  }

  private void flushAsyncDiskRegion() {
    for (DiskStore store : cacheRule.getCache().listDiskStoresIncludingRegionOwned()) {
      ((DiskStoreImpl) store).forceFlush();
    }
  }

  private void createSyncDiskRegion() throws IOException {
    createDiskRegion(false, false, regionName);
  }

  private void createAsyncDiskRegion() throws IOException {
    createAsyncDiskRegion(false);
  }

  private void createAsyncDiskRegion(boolean delayDiskStoreFlush) throws IOException {
    createDiskRegion(true, delayDiskStoreFlush, regionName);
  }

  private void createDiskRegion(boolean async, boolean delayDiskStoreFlush, String name)
      throws IOException {
    cacheRule.createCache();

    DiskStoreAttributes diskStoreAttributes = new DiskStoreAttributes();
    if (delayDiskStoreFlush) {
      diskStoreAttributes.timeInterval = Integer.MAX_VALUE;
    }

    DiskStoreFactory diskStoreFactory =
        cacheRule.getCache().createDiskStoreFactory(diskStoreAttributes);
    diskStoreFactory.setDiskDirs(
        new File[] {createOrGetDir()});
    diskStoreFactory.create(getDiskStoreName());

    RegionFactory regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE_PERSISTENT);
    regionFactory.setDiskStoreName(getDiskStoreName());
    if (async) {
      regionFactory.setDiskSynchronous(false);
    }
    regionFactory.create(name);
  }

  private File createOrGetDir() throws IOException {
    File dir = new File(temporaryFolder.getRoot(), getDiskStoreName());
    if (!dir.exists()) {
      dir = temporaryFolder.newFolder(getDiskStoreName());
    }
    return dir;
  }

  private String getDiskStoreName() {
    return getClass().getSimpleName() + VM.getCurrentVMNum();
  }

}
