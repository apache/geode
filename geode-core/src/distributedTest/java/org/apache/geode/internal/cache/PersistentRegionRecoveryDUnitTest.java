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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.backup.BackupOperation;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.CacheRule;
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

  @Before
  public void setUp() throws Exception {
    vm0 = getVM(0);
    vm1 = getVM(1);
    regionName = getClass().getSimpleName() + "-" + testName.getMethodName();
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
          new DistributionMessageObserver() {
            @Override
            public void beforeProcessMessage(ClusterDistributionManager dm,
                DistributionMessage message) {
              logger.info("In DistributionMessageObserver message is: " + message);
              if (message instanceof InitialImageOperation.RequestImageMessage) {
                InitialImageOperation.RequestImageMessage rim =
                    (InitialImageOperation.RequestImageMessage) message;
                if (rim.regionPath.contains(regionName)) {
                  logger.info("##### Before vm0 is bounced.");
                  getBlackboard().signalGate("bounce");
                  // vm0.bounceForcibly();
                  await().until(() -> cacheRule.getCache().isClosed());
                  logger.info("##### After vm0 is bounced.");
                } else {
                  logger.info("#### Region path: " + rim.regionPath);
                }
              }
            }
          });
    });

    AsyncInvocation asyncVM1 = vm1.invokeAsync(() -> createAsyncDiskRegion());

    logger.info("##### After async create region in vm1");

    getBlackboard().waitForGate("bounce", 30, SECONDS);
    vm0.bounceForcibly();

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
          new DistributionMessageObserver() {
            @Override
            public void beforeProcessMessage(ClusterDistributionManager dm,
                DistributionMessage message) {
              logger.info("In DistributionMessageObserver message is: " + message);
              if (message instanceof InitialImageOperation.RequestImageMessage) {
                InitialImageOperation.RequestImageMessage rim =
                    (InitialImageOperation.RequestImageMessage) message;
                if (rim.regionPath.contains(regionName)) {
                  logger.info("##### Before vm0 is bounced.");
                  getBlackboard().signalGate("bounce");
                  // vm0.bounceForcibly();
                  await().until(() -> cacheRule.getCache().isClosed());
                  logger.info("##### After vm0 is bounced.");
                } else {
                  logger.info("#### Region path: " + rim.regionPath);
                }
              }
            }
          });
    });

    AsyncInvocation asyncVM1 = vm1.invokeAsync(() -> createSyncDiskRegion());

    logger.info("##### After async create region in vm1");

    getBlackboard().waitForGate("bounce", 30, SECONDS);
    vm0.bounceForcibly();

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
