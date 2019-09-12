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
package org.apache.geode.management;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.DiskRegionStats;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

/**
 * Test cases to cover all test cases which pertains to disk from Management layer
 */

@SuppressWarnings({"serial", "unused"})
public class DiskManagementDUnitTest implements Serializable {

  private static final String REGION_NAME =
      DiskManagementDUnitTest.class.getSimpleName() + "_region";

  private File diskDir;

  @Manager
  private VM managerVM;

  @Member
  private VM[] memberVMs;

  @Rule
  public ManagementTestRule managementTestRule = ManagementTestRule.builder().start(true).build();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void before() throws Exception {
    this.diskDir = this.temporaryFolder.newFolder("diskDir");
  }

  /**
   * Tests Disk Compaction from a MemberMXBean which is at cache level. All the disks which belong
   * to the cache should be compacted.
   */
  @Test
  public void testDiskCompact() throws Exception {
    for (VM memberVM : this.memberVMs) {
      createPersistentRegion(memberVM);
      makeDiskCompactable(memberVM);
    }

    for (VM memberVM : this.memberVMs) {
      compactAllDiskStores(memberVM);
    }
  }

  /**
   * Tests Disk Compaction from a MemberMXBean which is at cache level. All the disks which belong
   * to the cache should be compacted.
   */
  @Test
  public void testDiskCompactRemote() throws Exception {
    for (VM memberVM : this.memberVMs) {
      createPersistentRegion(memberVM);
      makeDiskCompactable(memberVM);
    }

    compactDiskStoresRemote(this.managerVM, this.memberVMs.length);
  }

  /**
   * Tests various operations defined on DiskStore Mbean
   */
  @Test
  public void testDiskOps() throws Exception {
    for (VM memberVM : this.memberVMs) {
      createPersistentRegion(memberVM);
      makeDiskCompactable(memberVM);
      invokeFlush(memberVM);
      invokeForceRoll(memberVM);
      invokeForceCompaction(memberVM);
    }
  }

  @Test
  public void testDiskBackupAllMembers() throws Exception {
    for (VM memberVM : this.memberVMs) {
      createPersistentRegion(memberVM);
      makeDiskCompactable(memberVM);
    }

    backupAllMembers(this.managerVM, this.memberVMs.length);
  }

  /**
   * Checks the test case of missing disks and revoking them through MemberMXBean interfaces
   */
  @Test
  public void testMissingMembers() throws Exception {
    VM memberVM1 = this.memberVMs[0];
    VM memberVM2 = this.memberVMs[1];

    createPersistentRegion(memberVM1);
    createPersistentRegion(memberVM2);

    putAnEntry(memberVM1);

    this.managerVM.invoke("checkForMissingDiskStores", () -> {
      ManagementService service = this.managementTestRule.getManagementService();
      DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();
      PersistentMemberDetails[] missingDiskStores = distributedSystemMXBean.listMissingDiskStores();

      assertThat(missingDiskStores).isEmpty();
    });

    closeCache(memberVM1);

    updateTheEntry(memberVM2, "C");

    closeCache(memberVM2);

    AsyncInvocation creatingPersistentRegionAsync = createPersistentRegionAsync(memberVM1);

    memberVM1.invoke(() -> GeodeAwaitility.await().until(() -> {
      GemFireCacheImpl cache = (GemFireCacheImpl) this.managementTestRule.getCache();
      PersistentMemberManager persistentMemberManager = cache.getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> regions = persistentMemberManager.getWaitingRegions();
      return !regions.isEmpty();
    }));

    assertThat(creatingPersistentRegionAsync.isAlive()).isTrue();

    this.managerVM.invoke("revokeMissingDiskStore", () -> {
      ManagementService service = this.managementTestRule.getManagementService();
      DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
      PersistentMemberDetails[] missingDiskStores = bean.listMissingDiskStores();

      assertThat(missingDiskStores).isNotNull().hasSize(1);

      assertThat(bean.revokeMissingDiskStores(missingDiskStores[0].getDiskStoreId())).isTrue();
    });

    await(creatingPersistentRegionAsync);

    verifyRecoveryStats(memberVM1, true);

    // Check to make sure we recovered the old value of the entry.
    memberVM1.invoke("check for the entry", () -> {
      Cache cache = this.managementTestRule.getCache();
      Region region = cache.getRegion(REGION_NAME);
      assertThat(region.get("A")).isEqualTo("B");
    });
  }

  /**
   * Invokes flush on the given disk store by MBean interface
   */
  private void invokeFlush(final VM memberVM) {
    memberVM.invoke("invokeFlush", () -> {
      Cache cache = this.managementTestRule.getCache();
      DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
      String name = "testFlush_" + ProcessUtils.identifyPid();
      DiskStore diskStore = diskStoreFactory.create(name);

      ManagementService service = this.managementTestRule.getManagementService();
      DiskStoreMXBean diskStoreMXBean = service.getLocalDiskStoreMBean(name);
      assertThat(diskStoreMXBean).isNotNull();
      assertThat(diskStoreMXBean.getName()).isEqualTo(diskStore.getName());

      diskStoreMXBean.flush();
    });
  }

  /**
   * Invokes force roll on disk store by MBean interface
   */
  private void invokeForceRoll(final VM memberVM) {
    memberVM.invoke("invokeForceRoll", () -> {
      Cache cache = this.managementTestRule.getCache();
      DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
      String name = "testForceRoll_" + ProcessUtils.identifyPid();
      DiskStore diskStore = diskStoreFactory.create(name);

      ManagementService service = this.managementTestRule.getManagementService();
      DiskStoreMXBean diskStoreMXBean = service.getLocalDiskStoreMBean(name);
      assertThat(diskStoreMXBean).isNotNull();
      assertThat(diskStoreMXBean.getName()).isEqualTo(diskStore.getName());

      diskStoreMXBean.forceRoll();
    });
  }

  /**
   * Invokes force compaction on disk store by MBean interface
   */
  private void invokeForceCompaction(final VM memberVM) {
    memberVM.invoke("invokeForceCompaction", () -> {
      Cache cache = this.managementTestRule.getCache();
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      dsf.setAllowForceCompaction(true);
      String name = "testForceCompaction_" + ProcessUtils.identifyPid();
      DiskStore diskStore = dsf.create(name);

      ManagementService service = this.managementTestRule.getManagementService();
      DiskStoreMXBean diskStoreMXBean = service.getLocalDiskStoreMBean(name);
      assertThat(diskStoreMXBean).isNotNull();
      assertThat(diskStoreMXBean.getName()).isEqualTo(diskStore.getName());

      assertThat(diskStoreMXBean.forceCompaction()).isFalse();
    });
  }

  /**
   * Makes the disk compactable by adding and deleting some entries
   */
  private void makeDiskCompactable(final VM memberVM) throws Exception {
    memberVM.invoke("makeDiskCompactable", () -> {
      Cache cache = this.managementTestRule.getCache();
      Region region = cache.getRegion(REGION_NAME);
      region.put("key1", "value1");
      region.put("key2", "value2");
      region.remove("key2");
      // now that it is compactable the following forceCompaction should
      // go ahead and do a roll and compact it.
    });
  }

  /**
   * Compacts all DiskStores belonging to a member
   */
  private void compactAllDiskStores(final VM memberVM) throws Exception {
    memberVM.invoke("compactAllDiskStores", () -> {
      ManagementService service = this.managementTestRule.getManagementService();
      MemberMXBean memberMXBean = service.getMemberMXBean();
      String[] compactedDiskStores = memberMXBean.compactAllDiskStores();
      assertThat(compactedDiskStores).hasSize(1);
    });
  }

  /**
   * Takes a back up of all the disk store in a given directory
   */
  private void backupAllMembers(final VM managerVM, final int memberCount) {
    managerVM.invoke("backupAllMembers", () -> {
      ManagementService service = this.managementTestRule.getManagementService();
      DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
      File backupDir = this.temporaryFolder.newFolder("backupDir");

      DiskBackupStatus status = bean.backupAllMembers(backupDir.getAbsolutePath(), null);

      assertThat(status.getBackedUpDiskStores().keySet().size()).isEqualTo(memberCount);
      assertThat(status.getOfflineDiskStores()).isEmpty(); // TODO: fix GEODE-1946
    });
  }

  /**
   * Compact a disk store from managerVM VM
   */
  private void compactDiskStoresRemote(final VM managerVM, final int memberCount) {
    managerVM.invoke("compactDiskStoresRemote", () -> {
      List<DistributedMember> otherMemberSet = this.managementTestRule.getOtherNormalMembers();
      assertThat(otherMemberSet.size()).isEqualTo(memberCount);

      SystemManagementService service = this.managementTestRule.getSystemManagementService();

      for (DistributedMember member : otherMemberSet) {
        MemberMXBean memberMXBean = awaitMemberMXBeanProxy(member);

        String[] allDisks = memberMXBean.listDiskStores(true);
        assertThat(allDisks).isNotNull().hasSize(1);

        String[] compactedDiskStores = memberMXBean.compactAllDiskStores();
        assertThat(compactedDiskStores).hasSize(1);
      }
    });
  }

  private void updateTheEntry(final VM memberVM, final String value) {
    memberVM.invoke("updateTheEntry", () -> {
      Cache cache = this.managementTestRule.getCache();
      Region region = cache.getRegion(REGION_NAME);
      region.put("A", value);
    });
  }

  private void putAnEntry(final VM memberVM) {
    memberVM.invoke("putAnEntry", () -> {
      Cache cache = this.managementTestRule.getCache();
      Region region = cache.getRegion(REGION_NAME);
      region.put("A", "B");
    });
  }

  private void closeCache(final VM memberVM) {
    memberVM.invoke("closeRegion", () -> {
      Cache cache = this.managementTestRule.getCache();
      Region region = cache.getRegion(REGION_NAME);
      cache.close();
    });
  }

  private void createPersistentRegion(final VM memberVM)
      throws InterruptedException, ExecutionException, TimeoutException {
    await(createPersistentRegionAsync(memberVM));
  }

  private AsyncInvocation createPersistentRegionAsync(final VM memberVM) {
    return memberVM.invokeAsync("createPersistentRegionAsync", () -> {
      File dir = new File(diskDir, String.valueOf(ProcessUtils.identifyPid()));

      Cache cache = this.managementTestRule.getCache();

      DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
      diskStoreFactory.setDiskDirs(new File[] {dir});
      diskStoreFactory.setMaxOplogSize(1);
      diskStoreFactory.setAllowForceCompaction(true);
      diskStoreFactory.setAutoCompact(false);
      DiskStore diskStore = diskStoreFactory.create(REGION_NAME);

      RegionFactory regionFactory = cache.createRegionFactory();
      regionFactory.setDiskStoreName(diskStore.getName());
      regionFactory.setDiskSynchronous(true);
      regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      regionFactory.setScope(Scope.DISTRIBUTED_ACK);
      regionFactory.create(REGION_NAME);
    });
  }

  private void verifyRecoveryStats(final VM memberVM, final boolean localRecovery) {
    memberVM.invoke("verifyRecoveryStats", () -> {
      Cache cache = this.managementTestRule.getCache();
      Region region = cache.getRegion(REGION_NAME);
      DistributedRegion distributedRegion = (DistributedRegion) region;
      DiskRegionStats stats = distributedRegion.getDiskRegion().getStats();

      if (localRecovery) {
        assertThat(stats.getLocalInitializations()).isEqualTo(1);
        assertThat(stats.getRemoteInitializations()).isEqualTo(0);
      } else {
        assertThat(stats.getLocalInitializations()).isEqualTo(0);
        assertThat(stats.getRemoteInitializations()).isEqualTo(1);
      }
    });
  }

  private MemberMXBean awaitMemberMXBeanProxy(final DistributedMember member) {
    SystemManagementService service = this.managementTestRule.getSystemManagementService();
    ObjectName objectName = service.getMemberMBeanName(member);
    GeodeAwaitility.await()
        .untilAsserted(
            () -> assertThat(service.getMBeanProxy(objectName, MemberMXBean.class)).isNotNull());
    return service.getMBeanProxy(objectName, MemberMXBean.class);
  }

  private void await(final AsyncInvocation createPersistentRegionAsync)
      throws InterruptedException, ExecutionException, TimeoutException {
    createPersistentRegionAsync.await(2, MINUTES);
  }

}
