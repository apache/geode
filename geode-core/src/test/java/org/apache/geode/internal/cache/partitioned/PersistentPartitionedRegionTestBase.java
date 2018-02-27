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
package org.apache.geode.internal.cache.partitioned;

import static org.apache.commons.io.FileUtils.listFiles;
import static org.apache.commons.io.filefilter.DirectoryFileFilter.DIRECTORY;
import static org.apache.geode.admin.AdminDistributedSystemFactory.defineDistributedSystem;
import static org.apache.geode.admin.AdminDistributedSystemFactory.getDistributedSystem;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.logging.log4j.Logger;
import org.junit.Before;

import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.partition.PartitionRegionInfo;
import org.apache.geode.cache.persistence.ConflictingPersistentDataException;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.backup.BackupUtil;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisorImpl;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisorImpl.PersistenceAdvisorObserver;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.management.ManagementException;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

@SuppressWarnings("serial")
public abstract class PersistentPartitionedRegionTestBase extends JUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();

  // This must be bigger than the dunit ack-wait-threshold for the revoke
  // tests. The command line is setting the ack-wait-threshold to be
  // 60 seconds.
  private static final int MAX_WAIT = 70 * 1000;

  private static final int NUM_BUCKETS = 113;
  private static final String CHILD_REGION_NAME = "childRegion";

  private String partitionedRegionName;

  @Before
  public void setUpPersistentPartitionedRegionTestBase() throws Exception {
    disconnectAllFromDS();

    partitionedRegionName = getUniqueName() + "Region";
    invokeInEveryVM(() -> partitionedRegionName = getUniqueName() + "Region");

    postSetUpPersistentPartitionedRegionTestBase();
  }

  protected void postSetUpPersistentPartitionedRegionTestBase() throws Exception {
    // override as needed
  }

  void fakeCleanShutdown(final VM vm, final int bucketId) {
    vm.invoke("fakeCleanShutdown", () -> {
      Cache cache = getCache();
      PartitionedRegion region = (PartitionedRegion) cache.getRegion(getPartitionedRegionName());
      DiskRegion disk = region.getRegionAdvisor().getBucket(bucketId).getDiskRegion();
      for (PersistentMemberID id : disk.getOnlineMembers()) {
        disk.memberOfflineAndEqual(id);
      }
      for (PersistentMemberID id : disk.getOfflineMembers()) {
        disk.memberOfflineAndEqual(id);
      }
      cache.close();
    });
  }

  protected void checkData(VM vm, final int startKey, final int endKey, final String value) {
    checkData(vm, startKey, endKey, value, getPartitionedRegionName());
  }

  protected void checkData(final VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    vm.invoke("checkData", () -> {
      Region region = getCache().getRegion(regionName);
      for (int i = startKey; i < endKey; i++) {
        assertThat(region.get(i)).isEqualTo(value);
      }
    });
  }

  void removeData(final VM vm, final int startKey, final int endKey) {
    vm.invoke("removeData", () -> {
      Region region = getCache().getRegion(getPartitionedRegionName());
      for (int i = startKey; i < endKey; i++) {
        region.destroy(i);
      }
    });
  }

  protected void createData(final VM vm, final int startKey, final int endKey, final String value) {
    createData(vm, startKey, endKey, value, getPartitionedRegionName());
  }

  protected void createData(final VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    vm.invoke("createData", () -> {
      Region region = getCache().getRegion(regionName);
      for (int i = startKey; i < endKey; i++) {
        region.put(i, value);
      }
    });
  }

  protected void closeCache(final VM vm) {
    vm.invoke("closeCache", () -> getCache().close());
  }

  AsyncInvocation closeCacheAsync(final VM vm) {
    return vm.invokeAsync("closeCacheAsync", () -> getCache().close());
  }

  void closePR(final VM vm) {
    closePR(vm, getPartitionedRegionName());
  }

  void closePR(final VM vm, final String regionName) {
    vm.invoke("closePR", () -> getCache().getRegion(regionName).close());
  }

  void destroyPR(final VM vm) {
    destroyPR(vm, getPartitionedRegionName());
  }

  private void destroyPR(final VM vm, String regionName) {
    vm.invoke("destroyPR", () -> getCache().getRegion(regionName).localDestroyRegion());
  }

  void localDestroyPR(final VM vm) {
    vm.invoke("localDestroyPR",
        () -> getCache().getRegion(getPartitionedRegionName()).localDestroyRegion());
  }

  protected void createPR(final VM vm, final int redundancy, final int recoveryDelay,
      final int numBuckets) {
    vm.invoke(getCreatePRRunnable(redundancy, recoveryDelay, numBuckets));
  }

  protected void createPR(final VM vm, final int redundancy, final int recoveryDelay,
      final int numBuckets, final boolean synchronous) {
    vm.invoke(getCreatePRRunnable(redundancy, recoveryDelay, numBuckets, synchronous));
  }

  protected void createPR(final VM vm, final int redundancy, final int recoveryDelay) {
    vm.invoke(getCreatePRRunnable(redundancy, recoveryDelay));
  }

  protected void createPR(final VM vm, final int redundancy) {
    vm.invoke(getCreatePRRunnable(redundancy, -1));
  }

  void createNestedPR(final VM vm) {
    vm.invoke(getNestedPRRunnable());
  }

  AsyncInvocation createNestedPRAsync(final VM vm) {
    return vm.invokeAsync(getNestedPRRunnable());
  }

  private SerializableRunnable getNestedPRRunnable() {
    return new SerializableRunnable("getNestedPRRunnable") {
      @Override
      public void run() {

        // Wait for both nested PRs to be created
        final CountDownLatch recoveryDone = new CountDownLatch(2);

        ResourceObserver observer = new ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(final Region region) {
            recoveryDone.countDown();
          }
        };
        InternalResourceManager.setResourceObserver(observer);

        Cache cache = getCache();
        DiskStore diskStore = cache.findDiskStore("disk");
        if (diskStore == null) {
          diskStore = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }

        AttributesFactory attributesFactory = new AttributesFactory();
        attributesFactory.setDataPolicy(DataPolicy.REPLICATE);

        Region parent1 = cache.createRegion("parent1", attributesFactory.create());
        Region parent2 = cache.createRegion("parent2", attributesFactory.create());

        attributesFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        attributesFactory.setDiskStoreName("disk");

        PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
        partitionAttributesFactory.setRedundantCopies(1);
        attributesFactory.setPartitionAttributes(partitionAttributesFactory.create());

        parent1.createSubregion(getPartitionedRegionName(), attributesFactory.create());
        parent2.createSubregion(getPartitionedRegionName(), attributesFactory.create());

        try {
          recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  void createCoLocatedPR(final VM vm, final int setRedundantCopies,
      final boolean setPersistenceAdvisorObserver) {
    vm.invoke(() -> {
      String dsName = "colacatedpr";

      // Wait for both nested PRs to be created
      final CountDownLatch recoveryDone = new CountDownLatch(2);
      ResourceObserver observer = new ResourceObserverAdapter() {
        @Override
        public void recoveryFinished(final Region region) {
          recoveryDone.countDown();
        }
      };
      InternalResourceManager.setResourceObserver(observer);

      // Wait for parent and child region to be created.
      // And throw exception while region is getting initialized.
      final CountDownLatch childRegionCreated = new CountDownLatch(1);
      if (setPersistenceAdvisorObserver) {
        PersistenceAdvisorImpl.setPersistenceAdvisorObserver(new PersistenceAdvisorObserver() {
          @Override
          public void observe(String regionPath) {
            if (regionPath.contains(getChildRegionName())) {
              try {
                childRegionCreated.await(MAX_WAIT, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              throw new ConflictingPersistentDataException(
                  "Testing Cache Close with ConflictingPersistentDataException for region "
                      + regionPath);
            }
          }
        });
      }

      // Create region.
      try {
        Cache cache = getCache();

        DiskStore diskStore = cache.findDiskStore(dsName);
        if (diskStore == null) {
          diskStore = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create(dsName);
        }

        // Parent Region
        PartitionAttributesFactory partitionAttributesFactory =
            new PartitionAttributesFactory().setRedundantCopies(setRedundantCopies);

        AttributesFactory attributesFactory = new AttributesFactory();
        attributesFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        attributesFactory.setDiskStoreName(dsName);
        attributesFactory.setPartitionAttributes(partitionAttributesFactory.create());

        cache.createRegion(getPartitionedRegionName(), attributesFactory.create());

        // Colocated region
        partitionAttributesFactory = (new PartitionAttributesFactory())
            .setRedundantCopies(setRedundantCopies).setColocatedWith(getPartitionedRegionName());

        attributesFactory = new AttributesFactory();
        attributesFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        attributesFactory.setDiskStoreName(dsName);
        attributesFactory.setPartitionAttributes(partitionAttributesFactory.create());

        cache.createRegion(getChildRegionName(), attributesFactory.create());

        // Count down on region create.
        childRegionCreated.countDown();

        try {
          recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

      } finally {
        PersistenceAdvisorImpl.setPersistenceAdvisorObserver(null);
      }
    });
  }

  private SerializableRunnable getCreatePRRunnable(final int redundancy, final int recoveryDelay) {
    return getCreatePRRunnable(redundancy, recoveryDelay, NUM_BUCKETS);
  }

  private SerializableRunnable getCreatePRRunnable(final int redundancy, final int recoveryDelay,
      final int numBuckets) {
    return getCreatePRRunnable(redundancy, recoveryDelay, numBuckets, true);
  }

  private SerializableRunnable getCreatePRRunnable(final int redundancy, final int recoveryDelay,
      final int numBuckets, final boolean synchronous) {
    return new SerializableRunnable("getCreatePRRunnable") {
      @Override
      public void run() {
        final CountDownLatch recoveryDone = new CountDownLatch(1);
        if (redundancy > 0) {
          ResourceObserver observer = new ResourceObserverAdapter() {
            @Override
            public void recoveryFinished(Region region) {
              recoveryDone.countDown();
            }
          };
          InternalResourceManager.setResourceObserver(observer);
        } else {
          recoveryDone.countDown();
        }

        Cache cache = getCache();

        RegionAttributes regionAttributes =
            getPersistentPRAttributes(redundancy, recoveryDelay, cache, numBuckets, synchronous);
        cache.createRegion(getPartitionedRegionName(), regionAttributes);

        try {
          recoveryDone.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  protected RegionAttributes getPersistentPRAttributes(final int redundancy,
      final int recoveryDelay, final Cache cache, final int numBuckets, final boolean synchronous) {
    DiskStore diskStore = cache.findDiskStore("disk");
    if (diskStore == null) {
      diskStore = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
    }

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundancy);
    partitionAttributesFactory.setRecoveryDelay(recoveryDelay);
    partitionAttributesFactory.setTotalNumBuckets(numBuckets);
    // Make sure all vms end up with the same local max memory
    partitionAttributesFactory.setLocalMaxMemory(500);

    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    attributesFactory.setDiskStoreName("disk");
    attributesFactory.setDiskSynchronous(synchronous);
    attributesFactory.setPartitionAttributes(partitionAttributesFactory.create());

    return attributesFactory.create();
  }

  AsyncInvocation createPRAsync(final VM vm, final int redundancy, int recoveryDelay,
      int numBuckets) {
    return vm.invokeAsync(getCreatePRRunnable(redundancy, recoveryDelay, numBuckets));
  }

  AsyncInvocation createPRAsync(final VM vm, final int redundancy) {
    return vm.invokeAsync(getCreatePRRunnable(redundancy, -1));
  }

  protected Set<Integer> getBucketList(final VM vm) {
    return getBucketList(vm, getPartitionedRegionName());
  }

  protected Set<Integer> getBucketList(final VM vm, final String regionName) {
    return vm.invoke("getBucketList", () -> {
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);
      return new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
    });
  }

  void waitForBuckets(final VM vm, final Set<Integer> expectedBuckets, final String regionName) {
    vm.invoke("waitForBuckets", () -> {
      Cache cache = getCache();
      final PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);

      Wait.waitForCriterion(new WaitCriterion() {
        @Override
        public boolean done() {
          return expectedBuckets.equals(getActualBuckets());
        }

        @Override
        public String description() {
          return "Buckets on vm " + getActualBuckets() + " never became equal to expected "
              + expectedBuckets;
        }

        Set<Integer> getActualBuckets() {
          return new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
        }
      }, 30 * 1000, 100, true);
    });
  }

  Set<Integer> getPrimaryBucketList(final VM vm) {
    return getPrimaryBucketList(vm, getPartitionedRegionName());
  }

  Set<Integer> getPrimaryBucketList(final VM vm, final String regionName) {
    return vm.invoke("getPrimaryBucketList", () -> {
      Cache cache = getCache();
      PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
      return new TreeSet<>(region.getDataStore().getAllLocalPrimaryBucketIds());
    });
  }

  void revokeKnownMissingMembers(final VM vm, final int numExpectedMissing) {
    vm.invoke("revokeKnownMissingMembers", () -> {
      DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
      AdminDistributedSystem adminDS = getDistributedSystem(config);
      adminDS.connect();
      try {
        adminDS.waitToBeConnected(MAX_WAIT);

        final WaitCriterion wc = new WaitCriterion() {
          @Override
          public boolean done() {
            try {
              Set<PersistentID> missingIds = adminDS.getMissingPersistentMembers();
              if (missingIds.size() != numExpectedMissing) {
                return false;
              }
              for (PersistentID missingId : missingIds) {
                adminDS.revokePersistentMember(missingId.getUUID());
              }
              return true;
            } catch (AdminException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public String description() {
            try {
              return "expected " + numExpectedMissing + " missing members for revocation, current: "
                  + adminDS.getMissingPersistentMembers();
            } catch (AdminException e) {
              throw new RuntimeException(e);
            }
          }
        };
        Wait.waitForCriterion(wc, MAX_WAIT, 500, true);

      } finally {
        adminDS.disconnect();
      }
    });
  }

  void revokeAllMembers(final VM vm) {
    vm.invoke("revokeAllMembers", () -> {
      InternalCache cache = getCache(); // TODO:KIRK: delete this line
      DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
      AdminDistributedSystem adminDS = getDistributedSystem(config);
      adminDS.connect();

      try {
        adminDS.waitToBeConnected(MAX_WAIT);
        adminDS.revokePersistentMember(InetAddress.getLocalHost(), null);
      } finally {
        adminDS.disconnect();
      }
    });
  }

  void revokeMember(final VM vm, final File directory) {
    vm.invoke("revokeMember", () -> {
      InternalCache cache = getCache(); // TODO:KIRK: delete this line
      DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
      AdminDistributedSystem adminDS = getDistributedSystem(config);
      adminDS.connect();
      try {
        adminDS.waitToBeConnected(MAX_WAIT);
        adminDS.revokePersistentMember(InetAddress.getLocalHost(), directory.getCanonicalPath());
      } finally {
        adminDS.disconnect();
      }
    });
  }

  protected boolean moveBucket(final int bucketId, final VM source, final VM target) {
    InternalDistributedMember sourceId = getInternalDistributedMember(source);

    return target.invoke("moveBucket", () -> {
      PartitionedRegion region =
          (PartitionedRegion) getCache().getRegion(getPartitionedRegionName());
      return region.getDataStore().moveBucket(bucketId, sourceId, false);
    });
  }

  private InternalDistributedMember getInternalDistributedMember(final VM vm) {
    return (InternalDistributedMember) vm.invoke("getDistributedMember",
        () -> getCache().getDistributedSystem().getDistributedMember());
  }

  Set<PersistentMemberID> getOfflineMembers(final int bucketId, final VM vm) {
    return vm.invoke("getOfflineMembers", () -> {
      PartitionedRegion region =
          (PartitionedRegion) getCache().getRegion(getPartitionedRegionName());
      return region.getRegionAdvisor().getProxyBucketArray()[bucketId].getPersistenceAdvisor()
          .getMembershipView().getOfflineMembers();
    });
  }

  Set<PersistentMemberID> getOnlineMembers(final int bucketId, final VM vm) {
    return vm.invoke("getOnlineMembers", () -> {
      PartitionedRegion region =
          (PartitionedRegion) getCache().getRegion(getPartitionedRegionName());
      return region.getRegionAdvisor().getProxyBucketArray()[bucketId].getPersistenceAdvisor()
          .getPersistedOnlineOrEqualMembers();
    });
  }

  void waitForBucketRecovery(final VM vm, final Set<Integer> lostBuckets) {
    waitForBucketRecovery(vm, lostBuckets, getPartitionedRegionName());
  }

  private void waitForBucketRecovery(final VM vm, final Set<Integer> lostBuckets,
      final String regionName) {
    vm.invoke("waitForBucketRecovery", () -> {
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);
      PartitionedRegionDataStore dataStore = region.getDataStore();

      Wait.waitForCriterion(new WaitCriterion() {
        @Override
        public boolean done() {
          Set<Integer> vm2Buckets = dataStore.getAllLocalBucketIds();
          return lostBuckets.equals(vm2Buckets);
        }

        @Override
        public String description() {
          return "expected to recover " + lostBuckets + " buckets, now have "
              + dataStore.getAllLocalBucketIds();
        }
      }, MAX_WAIT, 100, true);
    });
  }

  void waitForRedundancyRecovery(final VM vm, final int expectedRedundancy,
      final String regionName) {
    vm.invoke("waitForRedundancyRecovery", () -> {
      Region region = getCache().getRegion(regionName);

      Wait.waitForCriterion(new WaitCriterion() {
        @Override
        public boolean done() {
          PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(region);
          return info.getActualRedundantCopies() == expectedRedundancy;
        }

        @Override
        public String description() {
          PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(region);
          return "Did not reach expected redundancy " + expectedRedundancy + " redundancy info = "
              + info.getActualRedundantCopies();
        }
      }, 30 * 1000, 100, true);
    });
  }

  protected BackupStatus backup(final VM vm) {
    return vm.invoke("backup", () -> {
      try {
        return BackupUtil.backupAllMembers(getSystem().getDistributionManager(),
            getBackupDir().toString(), null);
      } catch (ManagementException e) {
        throw new RuntimeException(e);
      }
    });
  }

  protected void restoreBackup(final int expectedNumScripts)
      throws IOException, InterruptedException {
    Collection<File> restoreScripts =
        listFiles(getBackupDir(), new RegexFileFilter(".*restore.*"), DIRECTORY);
    assertThat(restoreScripts).hasSize(expectedNumScripts);
    for (File script : restoreScripts) {
      execute(script);
    }
  }

  private void execute(final File script) throws IOException, InterruptedException {
    ProcessBuilder processBuilder = new ProcessBuilder(script.getAbsolutePath());
    processBuilder.redirectErrorStream(true);
    Process process = processBuilder.start();

    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        logger.info("OUTPUT:" + line);
        // TODO validate output
      }
    }

    assertThat(process.waitFor()).isEqualTo(0);
  }

  public String getPartitionedRegionName() {
    return partitionedRegionName;
  }

  String getChildRegionName() {
    return CHILD_REGION_NAME;
  }

  protected static File getBackupDir() {
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File dir = new File(tmpDir, "backupDir");
    dir.mkdirs();
    return dir;
  }
}
