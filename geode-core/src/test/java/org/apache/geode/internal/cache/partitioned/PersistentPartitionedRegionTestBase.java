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

import static org.apache.geode.test.dunit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminDistributedSystemFactory;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.BackupStatus;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.partition.PartitionRegionInfo;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisor;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

public abstract class PersistentPartitionedRegionTestBase extends JUnit4CacheTestCase {

  public static String PR_REGION_NAME = "region";
  // This must be bigger than the dunit ack-wait-threshold for the revoke
  // tests. The command line is setting the ack-wait-threshold to be
  // 60 seconds.
  private static final int MAX_WAIT = 70 * 1000;

  /*
   * (non-Javadoc) Set the region name for this test so that multiple subclasses of this test base
   * do not conflict with one another during parallel dunit runs
   * 
   * @see dunit.DistributedTestCase#setUp()
   */
  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    Invoke.invokeInEveryVM(PersistentPartitionedRegionTestBase.class, "setRegionName",
        new Object[] {getUniqueName()});
    setRegionName(getUniqueName());
    postSetUpPersistentPartitionedRegionTestBase();
  }

  protected void postSetUpPersistentPartitionedRegionTestBase() throws Exception {}

  public static void setRegionName(String testName) {
    PR_REGION_NAME = testName + "Region";
  }

  protected void checkRecoveredFromDisk(VM vm, final int bucketId, final boolean recoveredLocally) {
    vm.invoke(new SerializableRunnable("check recovered from disk") {
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(PR_REGION_NAME);
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

  protected void fakeCleanShutdown(VM vm, final int bucketId) {
    vm.invoke(new SerializableRunnable("mark clean") {
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(PR_REGION_NAME);
        DiskRegion disk = region.getRegionAdvisor().getBucket(bucketId).getDiskRegion();
        for (PersistentMemberID id : disk.getOnlineMembers()) {
          disk.memberOfflineAndEqual(id);
        }
        for (PersistentMemberID id : disk.getOfflineMembers()) {
          disk.memberOfflineAndEqual(id);
        }
        cache.close();
      }
    });
  }

  private PersistentMemberID getPersistentID(VM vm, final int bucketId) {
    Object id = vm.invoke(new SerializableCallable("get bucket persistent id") {
      public Object call() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(PR_REGION_NAME);
        PersistenceAdvisor advisor =
            region.getRegionAdvisor().getBucket(bucketId).getPersistenceAdvisor();
        return advisor.getPersistentID();
      }
    });

    return (PersistentMemberID) id;
  }

  private void forceRecovery(VM vm) {
    vm.invoke(new SerializableRunnable("force recovery") {
      public void run() {
        Cache cache = getCache();
        RebalanceFactory rf = cache.getResourceManager().createRebalanceFactory();
        try {
          rf.start().getResults();
        } catch (Exception e) {
          Assert.fail("interupted", e);
        }
      }
    });
  }

  protected void checkData(VM vm0, final int startKey, final int endKey, final String value) {
    checkData(vm0, startKey, endKey, value, PR_REGION_NAME);
  }

  protected void checkData(VM vm0, final int startKey, final int endKey, final String value,
      final String regionName) {
    SerializableRunnable checkData = new SerializableRunnable("CheckData") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);

        for (int i = startKey; i < endKey; i++) {
          assertEquals("For key " + i, value, region.get(i));
        }
      }
    };

    vm0.invoke(checkData);
  }

  protected void removeData(VM vm, final int startKey, final int endKey) {
    SerializableRunnable createData = new SerializableRunnable() {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(PR_REGION_NAME);

        for (int i = startKey; i < endKey; i++) {
          region.destroy(i);
        }
      }
    };
    vm.invoke(createData);
  }

  protected void createData(VM vm, final int startKey, final int endKey, final String value) {
    LogWriterUtils.getLogWriter().info("createData invoked.  PR_REGION_NAME is " + PR_REGION_NAME);
    createData(vm, startKey, endKey, value, PR_REGION_NAME);
  }

  protected void createData(VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    SerializableRunnable createData = new SerializableRunnable("createData") {

      public void run() {
        Cache cache = getCache();
        LogWriterUtils.getLogWriter().info("creating data in " + regionName);
        Region region = cache.getRegion(regionName);

        for (int i = startKey; i < endKey; i++) {
          region.put(i, value);
        }
      }
    };
    vm.invoke(createData);
  }

  protected void closeCache(VM vm0) {
    SerializableRunnable close = new SerializableRunnable("Close Cache") {
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    };

    vm0.invoke(close);
  }

  protected AsyncInvocation closeCacheAsync(VM vm0) {
    SerializableRunnable close = new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    };

    return vm0.invokeAsync(close);
  }

  protected void closePR(VM vm0) {
    closePR(vm0, PR_REGION_NAME);
  }

  protected void closePR(VM vm0, String regionName) {
    SerializableRunnable close = new SerializableRunnable("Close PR") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        region.close();
      }
    };

    vm0.invoke(close);
  }

  protected void destroyPR(VM vm0) {
    destroyPR(vm0, PR_REGION_NAME);
  }

  protected void destroyPR(VM vm0, String regionName) {
    SerializableRunnable destroy = new SerializableRunnable("Destroy PR") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        region.localDestroyRegion();
      }
    };

    vm0.invoke(destroy);
  }

  protected void localDestroyPR(VM vm0) {
    SerializableRunnable destroyPR = new SerializableRunnable("destroy pr") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(PR_REGION_NAME);
        region.localDestroyRegion();
      }
    };
    vm0.invoke(destroyPR);
  }

  protected void createPR(VM vm0, final int redundancy, final int recoveryDelay, int numBuckets) {
    SerializableRunnable createPR = getCreatePRRunnable(redundancy, recoveryDelay, numBuckets);

    vm0.invoke(createPR);
  }

  protected void createPR(VM vm0, final int redundancy, final int recoveryDelay, int numBuckets,
      boolean synchronous) {
    SerializableRunnable createPR =
        getCreatePRRunnable(redundancy, recoveryDelay, numBuckets, synchronous);

    vm0.invoke(createPR);
  }

  protected void createPR(VM vm0, final int redundancy, final int recoveryDelay) {
    SerializableRunnable createPR = getCreatePRRunnable(redundancy, recoveryDelay);

    vm0.invoke(createPR);
  }

  protected void createPR(VM vm0, final int redundancy) {
    SerializableRunnable createPR = getCreatePRRunnable(redundancy, -1);

    vm0.invoke(createPR);
  }

  protected void createNestedPR(VM vm) {
    SerializableRunnable createPR = getNestedPRRunnable();
    vm.invoke(createPR);
  }

  protected AsyncInvocation createNestedPRAsync(VM vm) {
    SerializableRunnable createPR = getNestedPRRunnable();
    return vm.invokeAsync(createPR);
  }

  private SerializableRunnable getNestedPRRunnable() {
    SerializableRunnable createPR = new SerializableRunnable("create pr") {

      public void run() {
        Cache cache = getCache();

        // Wait for both nested PRs to be created
        final CountDownLatch recoveryDone = new CountDownLatch(2);

        ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
          @Override
          public void recoveryFinished(Region region) {
            recoveryDone.countDown();
          }
        };
        InternalResourceManager.setResourceObserver(observer);

        DiskStore ds = cache.findDiskStore("disk");
        if (ds == null) {
          ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
        }
        Region parent1;
        {
          AttributesFactory af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.REPLICATE);
          parent1 = cache.createRegion("parent1", af.create());
        }
        Region parent2;
        {
          AttributesFactory af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.REPLICATE);
          parent2 = cache.createRegion("parent2", af.create());
        }
        {
          AttributesFactory af = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(1);
          af.setPartitionAttributes(paf.create());
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          af.setDiskStoreName("disk");
          parent1.createSubregion(PR_REGION_NAME, af.create());
        }
        {
          AttributesFactory af = new AttributesFactory();
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(1);
          af.setPartitionAttributes(paf.create());
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          af.setDiskStoreName("disk");
          parent2.createSubregion(PR_REGION_NAME, af.create());
        }

        try {
          recoveryDone.await(MAX_WAIT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
      }
    };
    return createPR;
  }

  private SerializableRunnable getCreatePRRunnable(final int redundancy, final int recoveryDelay) {
    return getCreatePRRunnable(redundancy, recoveryDelay, 113);
  }

  private SerializableRunnable getCreatePRRunnable(final int redundancy, final int recoveryDelay,
      final int numBuckets) {
    return getCreatePRRunnable(redundancy, recoveryDelay, numBuckets, true);
  }

  private SerializableRunnable getCreatePRRunnable(final int redundancy, final int recoveryDelay,
      final int numBuckets, final boolean synchronous) {
    SerializableRunnable createPR = new SerializableRunnable("create pr") {

      public void run() {
        final CountDownLatch recoveryDone;
        if (redundancy > 0) {
          recoveryDone = new CountDownLatch(1);

          ResourceObserver observer = new InternalResourceManager.ResourceObserverAdapter() {
            @Override
            public void recoveryFinished(Region region) {
              recoveryDone.countDown();
            }
          };
          InternalResourceManager.setResourceObserver(observer);
        } else {
          recoveryDone = null;
        }

        Cache cache = getCache();

        RegionAttributes attr =
            getPersistentPRAttributes(redundancy, recoveryDelay, cache, numBuckets, synchronous);
        cache.createRegion(PR_REGION_NAME, attr);
        if (recoveryDone != null) {
          try {
            recoveryDone.await();
          } catch (InterruptedException e) {
            Assert.fail("Interrupted", e);
          }
        }
      }
    };
    return createPR;
  }

  protected RegionAttributes getPersistentPRAttributes(final int redundancy,
      final int recoveryDelay, Cache cache, int numBuckets, boolean synchronous) {
    DiskStore ds = cache.findDiskStore("disk");
    if (ds == null) {
      ds = cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
    }
    AttributesFactory af = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundancy);
    paf.setRecoveryDelay(recoveryDelay);
    paf.setTotalNumBuckets(numBuckets);
    // Make sure all vms end up with the same local max memory
    paf.setLocalMaxMemory(500);
    af.setPartitionAttributes(paf.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    af.setDiskStoreName("disk");
    af.setDiskSynchronous(synchronous);
    RegionAttributes attr = af.create();
    return attr;
  }

  protected AsyncInvocation createPRAsync(VM vm0, final int redundancy, int recoveryDelay,
      int numBuckets) {
    SerializableRunnable createPR = getCreatePRRunnable(redundancy, recoveryDelay, numBuckets);
    return vm0.invokeAsync(createPR);
  }

  protected AsyncInvocation createPRAsync(VM vm0, final int redundancy) {
    SerializableRunnable createPR = getCreatePRRunnable(redundancy, -1);
    return vm0.invokeAsync(createPR);
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

  protected void waitForBuckets(VM vm, final Set<Integer> expectedBuckets,
      final String regionName) {
    SerializableCallable getBuckets = new SerializableCallable("get buckets") {

      public Object call() throws Exception {
        Cache cache = getCache();
        final PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        Wait.waitForCriterion(new WaitCriterion() {

          public boolean done() {
            return expectedBuckets.equals(getActualBuckets());
          }

          public String description() {
            return "Buckets on vm " + getActualBuckets() + " never became equal to expected "
                + expectedBuckets;
          }

          public TreeSet<Integer> getActualBuckets() {
            return new TreeSet<Integer>(region.getDataStore().getAllLocalBucketIds());
          }
        }, 30 * 1000, 100, true);

        return null;
      }
    };

    vm.invoke(getBuckets);
  }

  protected Set<Integer> getPrimaryBucketList(VM vm0) {
    return getPrimaryBucketList(vm0, PR_REGION_NAME);
  }

  protected Set<Integer> getPrimaryBucketList(VM vm0, final String regionName) {
    SerializableCallable getPrimaryBuckets = new SerializableCallable("get primary buckets") {

      public Object call() throws Exception {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        return new TreeSet<Integer>(region.getDataStore().getAllLocalPrimaryBucketIds());
      }
    };

    return (Set<Integer>) vm0.invoke(getPrimaryBuckets);
  }


  protected void revokeKnownMissingMembers(VM vm2, final int numExpectedMissing) {
    vm2.invoke(new SerializableRunnable("Revoke the member") {

      public void run() {
        final DistributedSystemConfig config;
        final AdminDistributedSystem adminDS;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          adminDS.waitToBeConnected(MAX_WAIT);
          try {
            final WaitCriterion wc = new WaitCriterion() {

              public boolean done() {
                try {
                  final Set<PersistentID> missingIds = adminDS.getMissingPersistentMembers();
                  if (missingIds.size() != numExpectedMissing) {
                    return false;
                  }
                  for (PersistentID missingId : missingIds) {
                    adminDS.revokePersistentMember(missingId.getUUID());
                  }
                  return true;
                } catch (AdminException ae) {
                  throw new RuntimeException(ae);
                }
              }

              public String description() {
                try {
                  return "expected " + numExpectedMissing
                      + " missing members for revocation, current: "
                      + adminDS.getMissingPersistentMembers();
                } catch (AdminException ae) {
                  throw new RuntimeException(ae);
                }
              }
            };
            Wait.waitForCriterion(wc, MAX_WAIT, 500, true);
          } finally {
            adminDS.disconnect();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  protected void revokeAllMembers(VM vm) {
    vm.invoke(new SerializableRunnable("Revoke the member") {

      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          adminDS.waitToBeConnected(MAX_WAIT);
          adminDS.revokePersistentMember(InetAddress.getLocalHost(), null);
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

  protected void revokeMember(VM vm, final File directory) {
    vm.invoke(new SerializableRunnable("Revoke the member") {

      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          adminDS.waitToBeConnected(MAX_WAIT);
          adminDS.revokePersistentMember(InetAddress.getLocalHost(), directory.getCanonicalPath());
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

  protected boolean moveBucket(final int bucketId, VM source, VM target) {

    SerializableCallable getId = new SerializableCallable("Get Id") {

      public Object call() throws Exception {
        Cache cache = getCache();
        return cache.getDistributedSystem().getDistributedMember();
      }
    };

    final InternalDistributedMember sourceId = (InternalDistributedMember) source.invoke(getId);

    SerializableCallable move = new SerializableCallable("move bucket") {

      public Object call() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(PR_REGION_NAME);
        return region.getDataStore().moveBucket(bucketId, sourceId, false);
      }
    };

    return (Boolean) target.invoke(move);

  }

  protected Set<PersistentMemberID> getOfflineMembers(final int bucketId, VM vm) {

    SerializableCallable getId = new SerializableCallable("Get Id") {

      public Object call() throws Exception {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(PR_REGION_NAME);
        return region.getRegionAdvisor().getProxyBucketArray()[bucketId].getPersistenceAdvisor()
            .getMembershipView().getOfflineMembers();
      }
    };


    return (Set<PersistentMemberID>) vm.invoke(getId);


  }

  protected Set<PersistentMemberID> getOnlineMembers(final int bucketId, VM vm) {

    SerializableCallable getId = new SerializableCallable("Get Id") {

      public Object call() throws Exception {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(PR_REGION_NAME);
        return region.getRegionAdvisor().getProxyBucketArray()[bucketId].getPersistenceAdvisor()
            .getPersistedOnlineOrEqualMembers();
      }
    };


    return (Set<PersistentMemberID>) vm.invoke(getId);
  }

  protected void waitForBucketRecovery(final VM vm2, final Set<Integer> lostBuckets) {
    waitForBucketRecovery(vm2, lostBuckets, PR_REGION_NAME);
  }

  protected void waitForBucketRecovery(final VM vm2, final Set<Integer> lostBuckets,
      final String regionName) {
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
        final PartitionedRegionDataStore dataStore = region.getDataStore();
        Wait.waitForCriterion(new WaitCriterion() {

          public boolean done() {
            Set<Integer> vm2Buckets = dataStore.getAllLocalBucketIds();
            return lostBuckets.equals(vm2Buckets);
          }

          public String description() {
            return "expected to recover " + lostBuckets + " buckets, now have "
                + dataStore.getAllLocalBucketIds();
          }
        }, MAX_WAIT, 100, true);
      }
    });
  }

  protected void waitForRedundancyRecovery(VM vm, final int expectedRedundancy,
      final String regionName) {
    vm.invoke(new SerializableRunnable() {

      public void run() {
        Cache cache = getCache();
        final Region region = cache.getRegion(regionName);
        Wait.waitForCriterion(new WaitCriterion() {

          public boolean done() {
            PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(region);
            return info.getActualRedundantCopies() == expectedRedundancy;
          }

          public String description() {
            PartitionRegionInfo info = PartitionRegionHelper.getPartitionRegionInfo(region);
            return "Did not reach expected redundancy " + expectedRedundancy + " redundancy info = "
                + info.getActualRedundantCopies();
          }
        }, 30 * 1000, 100, true);
      }
    });
  }

  protected void invalidateData(VM vm, final int startKey, final int endKey) {
    SerializableRunnable createData = new SerializableRunnable() {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(PR_REGION_NAME);

        for (int i = startKey; i < endKey; i++) {
          region.destroy(i);
          region.create(i, null);
          region.invalidate(i);
        }
      }
    };
    vm.invoke(createData);
  }

  // used for above test
  protected BackupStatus backup(VM vm) {
    return (BackupStatus) vm.invoke(new SerializableCallable("Backup all members") {

      public Object call() {
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          adminDS.waitToBeConnected(MAX_WAIT);
          return adminDS.backupAllMembers(getBackupDir());

        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

  protected void restoreBackup(int expectedNumScripts) throws IOException, InterruptedException {
    List<File> restoreScripts = FileUtil.findAll(getBackupDir(), ".*restore.*");
    assertEquals("Restore scripts " + restoreScripts, expectedNumScripts, restoreScripts.size());
    for (File script : restoreScripts) {
      execute(script);
    }

  }

  private void execute(File script) throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(script.getAbsolutePath());
    pb.redirectErrorStream(true);
    Process process = pb.start();

    InputStream is = process.getInputStream();
    byte[] buffer = new byte[1024];
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line;
    while ((line = br.readLine()) != null) {
      LogWriterUtils.getLogWriter().fine("OUTPUT:" + line);
      // TODO validate output
    } ;

    assertEquals(0, process.waitFor());

  }

  protected static File getBackupDir() {
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File dir = new File(tmpDir, "backupDir");
    dir.mkdirs();
    return dir;
  }
}
