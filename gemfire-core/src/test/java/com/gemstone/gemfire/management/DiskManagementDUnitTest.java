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
package com.gemstone.gemfire.management;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.ObjectName;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.DiskRegionStats;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberManager;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Test cases to cover all test cases which pertains to disk from Management
 * layer
 * 
 * @author rishim
 * 
 */
public class DiskManagementDUnitTest extends ManagementTestBase {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  // This must be bigger than the dunit ack-wait-threshold for the revoke
  // tests. The command line is setting the ack-wait-threshold to be
  // 60 seconds.
  private static final int MAX_WAIT = 70 * 1000;

  boolean testFailed = false;

  String failureCause = "";
  static final String REGION_NAME = "region";

  private File diskDir;

  protected static LogWriter logWriter;

  public DiskManagementDUnitTest(String name) throws Exception {
    super(name);
  
    diskDir = new File("diskDir-" + getName()).getAbsoluteFile();
    com.gemstone.gemfire.internal.FileUtil.delete(diskDir);
    diskDir.mkdir();
    diskDir.deleteOnExit();
  }

  public void setUp() throws Exception {
    super.setUp();
    failureCause = "";
    testFailed = false;

  }

  public void tearDown2() throws Exception {
    super.tearDown2();    
    com.gemstone.gemfire.internal.FileUtil.delete(diskDir);
  }

  /**
   * Tests Disk Compaction from a MemberMbean which is at cache level. All the
   * disks which belong to the cache should be compacted.
   * 
   * @throws Exception
   */

  public void testDiskCompact() throws Throwable {
    initManagement(false);
    for (VM vm : getManagedNodeList()) {
      createPersistentRegion(vm);
      makeDiskCompactable(vm);
    }
    
    for (VM vm : getManagedNodeList()) {
      compactAllDiskStores(vm);
    }

  }

  /**
   * Tests Disk Compaction from a MemberMbean which is at cache level. All the
   * disks which belong to the cache should be compacted.
   * 
   * @throws Exception
   */

  public void testDiskCompactRemote() throws Throwable {

    initManagement(false);
    for (VM vm : getManagedNodeList()) {
      createPersistentRegion(vm);
      makeDiskCompactable(vm);
    }
    compactDiskStoresRemote(managingNode);

  }

  /**
   * Tests various operations defined on DiskStore Mbean
   * 
   * @throws Exception
   */

  public void testDiskOps() throws Throwable {

    initManagement(false);
    for (VM vm : getManagedNodeList()) {
      createPersistentRegion(vm);
      makeDiskCompactable(vm);
      invokeFlush(vm);
      invokeForceRoll(vm);
      invokeForceCompaction(vm);
    }

  }

  public void testDiskBackupAllMembers() throws Throwable {
    initManagement(false);
    for (VM vm : getManagedNodeList()) {
      createPersistentRegion(vm);
      makeDiskCompactable(vm);

    }
    backupAllMembers(managingNode);
  }

  /**
   * Checks the test case of missing disks and revoking them through MemberMbean
   * interfaces
   * 
   * @throws Throwable
   */
  @SuppressWarnings("serial")
  public void testMissingMembers() throws Throwable {

    initManagement(false);
    VM vm0 = getManagedNodeList().get(0);
    VM vm1 = getManagedNodeList().get(1);
    VM vm2 = getManagedNodeList().get(2);
    
    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);

    putAnEntry(vm0);

 
    managingNode.invoke(new SerializableRunnable("Check for waiting regions") {

      public void run() {
        Cache cache = getCache();
        ManagementService service = getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        PersistentMemberDetails[] missingDiskStores = bean
            .listMissingDiskStores();

        assertNull(missingDiskStores);
      }
    });

    getLogWriter().info("closing region in vm0");
    closeRegion(vm0);

    updateTheEntry(vm1);

    getLogWriter().info("closing region in vm1");
    closeRegion(vm1);
    AsyncInvocation future = createPersistentRegionAsync(vm0);
    waitForBlockedInitialization(vm0);
    assertTrue(future.isAlive());

    managingNode.invoke(new SerializableRunnable("Revoke the member") {

      public void run() {
        Cache cache = getCache();
        GemFireCacheImpl cacheImpl = (GemFireCacheImpl) cache;
        ManagementService service = getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        PersistentMemberDetails[] missingDiskStores = bean
        .listMissingDiskStores();
        getLogWriter().info("waiting members=" + missingDiskStores);
        assertNotNull(missingDiskStores);
        assertEquals(1, missingDiskStores.length);

        for (PersistentMemberDetails id : missingDiskStores) {
          getLogWriter().info("Missing DiskStoreID is =" + id.getDiskStoreId());
          getLogWriter().info("Missing Host is =" + id.getHost());
          getLogWriter().info("Missing Directory is =" + id.getDirectory());

          try {
            bean.revokeMissingDiskStores(id.getDiskStoreId());
          } catch (Exception e) {
            fail("revokeMissingDiskStores failed with exception " + e);
          }
        }
      }
    });

    future.join(MAX_WAIT);
    if (future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    if (future.exceptionOccurred()) {
      throw new Exception(future.getException());
    }
    checkForRecoveryStat(vm0, true);
    // Check to make sure we recovered the old
    // value of the entry.
    SerializableRunnable checkForEntry = new SerializableRunnable(
        "check for the entry") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        assertEquals("B", region.get("A"));
      }
    };
    vm0.invoke(checkForEntry);

  }

  protected void checkNavigation(final VM vm,
      final DistributedMember diskMember, final String diskStoreName) {
    SerializableRunnable checkNavigation = new SerializableRunnable(
        "Check Navigation") {
      public void run() {

        final ManagementService service = getManagementService();

        DistributedSystemMXBean disMBean = service.getDistributedSystemMXBean();
        try {
          ObjectName expected = MBeanJMXAdapter.getDiskStoreMBeanName(diskMember.getId(), diskStoreName);
          ObjectName actual = disMBean.fetchDiskStoreObjectName(diskMember.getId(), diskStoreName);
          assertEquals(expected, actual);
        } catch (Exception e) {
          fail("Disk Store Navigation Failed " + e);
        }


      }
    };
    vm.invoke(checkNavigation);
  }

  /**
   * get Distributed member for a given vm
   * 
   * @param vm
   *          reference to VM
   */
  @SuppressWarnings("serial")
  protected static DistributedMember getMember() throws Exception {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    return cache.getDistributedSystem().getDistributedMember();
  }

  /**
   * Invokes flush on the given disk store by MBean interface
   * 
   * @param vm
   *          reference to VM
   */
  @SuppressWarnings("serial")
  public void invokeFlush(final VM vm) {
    SerializableRunnable invokeFlush = new SerializableRunnable(
        "Invoke Flush On Disk") {
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        String name = "testFlush_" + vm.getPid();
        DiskStore ds = dsf.create(name);

        ManagementService service = getManagementService();
        DiskStoreMXBean bean = service.getLocalDiskStoreMBean(name);
        assertNotNull(bean);
        bean.flush();
      }
    };
    vm.invoke(invokeFlush);
  }

  /**
   * Invokes force roll on disk store by MBean interface
   * 
   * @param vm
   *          reference to VM
   */
  @SuppressWarnings("serial")
  public void invokeForceRoll(final VM vm) {
    SerializableRunnable invokeForceRoll = new SerializableRunnable(
        "Invoke Force Roll") {
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        String name = "testForceRoll_" + vm.getPid();
        DiskStore ds = dsf.create(name);
        ManagementService service = getManagementService();
        DiskStoreMXBean bean = service.getLocalDiskStoreMBean(name);
        assertNotNull(bean);
        bean.forceRoll();
      }
    };
    vm.invoke(invokeForceRoll);
  }

  /**
   * Invokes force compaction on disk store by MBean interface
   * 
   * @param vm
   *          reference to VM
   */
  @SuppressWarnings("serial")
  public void invokeForceCompaction(final VM vm) {
    SerializableRunnable invokeForceCompaction = new SerializableRunnable(
        "Invoke Force Compaction") {
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        dsf.setAllowForceCompaction(true);
        String name = "testForceCompaction_" + vm.getPid();
        DiskStore ds = dsf.create(name);
        ManagementService service = getManagementService();
        DiskStoreMXBean bean = service.getLocalDiskStoreMBean(name);
        assertNotNull(bean);
        assertEquals(false, bean.forceCompaction());
      }
    };
    vm.invoke(invokeForceCompaction);
  }

  /**
   * Makes the disk compactable by adding and deleting some entries
   * 
   * @throws Exception
   */
  @SuppressWarnings("serial")
  public void makeDiskCompactable(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Make The Disk Compactable") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        DiskRegion dr = ((LocalRegion) region).getDiskRegion();
        getLogWriter().info("putting key1");
        region.put("key1", "value1");
        getLogWriter().info("putting key2");
        region.put("key2", "value2");
        getLogWriter().info("removing key2");
        region.remove("key2");
        // now that it is compactable the following forceCompaction should
        // go ahead and do a roll and compact it.
      }
    });

  }



 

  /**
   * Compacts all DiskStores belonging to a member
   * 
   * @param vm1
   *          reference to VM
   * @throws Exception
   */
  @SuppressWarnings("serial")
  public void compactAllDiskStores(VM vm1) throws Exception {

    vm1.invoke(new SerializableCallable("Compact All Disk Stores") {

      public Object call() throws Exception {
        ManagementService service = getManagementService();
        MemberMXBean memberBean = service.getMemberMXBean();
        String[] compactedDiskStores = memberBean.compactAllDiskStores();

        assertTrue(compactedDiskStores.length > 0);
        for (int i = 0; i < compactedDiskStores.length; i++) {
          getLogWriter().info(
              "<ExpectedString> Compacted Store " + i + " "
                  + compactedDiskStores[i] + "</ExpectedString> ");
        }

        return null;
      }
    });

  }

  /**
   * Takes a back up of all the disk store in a given directory
   * 
   * @param vm1
   *          reference to VM
   * @throws Exception
   */
  @SuppressWarnings("serial")
  public void backupAllMembers(final VM managingVM) throws Exception {

    managingVM.invoke(new SerializableCallable("Backup All Disk Stores") {

      public Object call() throws Exception {
        ManagementService service = getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        DiskBackupStatus status = bean.backupAllMembers(getBackupDir("test_backupAllMembers")
            .getAbsolutePath(), null);

        return null;
      }
    });

  }

  /**
   * Compact a disk store from Managing node
   * 
   * @param vm1
   *          reference to VM
   * @throws Exception
   */
  @SuppressWarnings("serial")
  public void compactDiskStoresRemote(VM managingVM) throws Exception {
    {

      managingVM.invoke(new SerializableCallable("Compact All Disk Stores Remote") {

        public Object call() throws Exception {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          Set<DistributedMember> otherMemberSet = cache
              .getDistributionManager().getOtherNormalDistributionManagerIds();

          for (DistributedMember member : otherMemberSet) {
            MemberMXBean bean = MBeanUtil.getMemberMbeanProxy(member);
            String[] allDisks = bean.listDiskStores(true);
            assertNotNull(allDisks);
            List<String> listString = Arrays.asList(allDisks);
            getLogWriter().info(
                "<ExpectedString> Remote All Disk Stores Are  "
                    + listString.toString() + "</ExpectedString> ");
            String[] compactedDiskStores = bean.compactAllDiskStores();
            assertTrue(compactedDiskStores.length > 0);
            for (int i = 0; i < compactedDiskStores.length; i++) {
              getLogWriter().info(
                  "<ExpectedString> Remote Compacted Store " + i + " "
                      + compactedDiskStores[i] + "</ExpectedString> ");
            }

          }
          return null;
        }
      });

    }

  }

  /**
   * Checks if a file with the given extension is present
   * 
   * @param fileExtension
   *          file extension
   * @throws Exception
   */
  protected void checkIfContainsFileWithExt(String fileExtension)
      throws Exception {
    File[] files = diskDir.listFiles();
    for (int j = 0; j < files.length; j++) {
      if (files[j].getAbsolutePath().endsWith(fileExtension)) {
        fail("file \"" + files[j].getAbsolutePath() + "\" still exists");
      }
    }

  }

  /**
   * Update Entry
   * 
   * @param vm1
   *          reference to VM
   */
  protected void updateTheEntry(VM vm1) {
    updateTheEntry(vm1, "C");
  }

  /**
   * Update an Entry
   * @param vm1
   *          reference to VM
   * @param value
   *          Value which is updated
   */
  @SuppressWarnings("serial")
  protected void updateTheEntry(VM vm1, final String value) {
    vm1.invoke(new SerializableRunnable("change the entry") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.put("A", value);
      }
    });
  }

  /**
   * Put an entry to region
   * 
   * @param vm0
   *          reference to VM
   */
  @SuppressWarnings("serial")
  protected void putAnEntry(VM vm0) {
    vm0.invoke(new SerializableRunnable("Put an entry") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.put("A", "B");
      }
    });
  }

  /**
   * Close the given region REGION_NAME
   * 
   * @param vm
   *          reference to VM
   */
  @SuppressWarnings("serial")
  protected void closeRegion(final VM vm) {
    SerializableRunnable closeRegion = new SerializableRunnable(
        "Close persistent region") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.close();
      }
    };
    vm.invoke(closeRegion);
  }

  /**
   * Waiting to blocked waiting for another persistent member to come online
   * 
   * @param vm
   *          reference to VM
   */
  @SuppressWarnings("serial")
  private void waitForBlockedInitialization(VM vm) {
    vm.invoke(new SerializableRunnable() {

      public void run() {
        waitForCriterion(new WaitCriterion() {

          public String description() {
            return "Waiting to blocked waiting for another persistent member to come online";
          }

          public boolean done() {
            Cache cache = getCache();
            GemFireCacheImpl cacheImpl = (GemFireCacheImpl) cache;
            PersistentMemberManager mm = cacheImpl.getPersistentMemberManager();
            Map<String, Set<PersistentMemberID>> regions = mm
                .getWaitingRegions();
            boolean done = !regions.isEmpty();
            return done;
          }

        }, MAX_WAIT, 100, true);

      }

    });
  }

  /**
   * Creates a persistent region
   * 
   * @param vm
   *          reference to VM
   * @throws Throwable
   */
  protected void createPersistentRegion(VM vm) throws Throwable {
    AsyncInvocation future = createPersistentRegionAsync(vm);
    future.join(MAX_WAIT);
    if (future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    if (future.exceptionOccurred()) {
      throw new RuntimeException(future.getException());
    }
  }

  /**
   * Creates a persistent region in async manner
   * 
   * @param vm
   *          reference to VM
   * @return reference to AsyncInvocation
   */
  @SuppressWarnings("serial")
  protected AsyncInvocation createPersistentRegionAsync(final VM vm) {
    SerializableRunnable createRegion = new SerializableRunnable(
        "Create persistent region") {
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        File dir = getDiskDirForVM(vm);
        dir.mkdirs();
        dsf.setDiskDirs(new File[] { dir });
        dsf.setMaxOplogSize(1);
        dsf.setAllowForceCompaction(true);
        dsf.setAutoCompact(false);
        DiskStore ds = dsf.create(REGION_NAME);
        RegionFactory rf = cache.createRegionFactory();
        rf.setDiskStoreName(ds.getName());
        rf.setDiskSynchronous(true);
        rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.create(REGION_NAME);
      }
    };
    return vm.invokeAsync(createRegion);
  }

  /**
   * Validates a persistent region
   * 
   * @param vm
   *          reference to VM
   */
  @SuppressWarnings("serial")
  protected void validatePersistentRegion(final VM vm) {
    SerializableRunnable validateDisk = new SerializableRunnable(
        "Validate persistent region") {
      public void run() {
        Cache cache = getCache();
        ManagementService service = getManagementService();
        DiskStoreMXBean bean = service.getLocalDiskStoreMBean(REGION_NAME);
        assertNotNull(bean);
      }
    };
    vm.invoke(validateDisk);
  }

  /**
   * Appends vm id to disk dir
   * 
   * @param vm
   *          reference to VM
   * @return
   */
  protected File getDiskDirForVM(final VM vm) {
    File dir = new File(diskDir, String.valueOf(vm.getPid()));
    return dir;
  }

  /**
   * Checks recovery status
   * 
   * @param vm
   *          reference to VM
   * @param localRecovery
   *          local recovery on or not
   */
  @SuppressWarnings("serial")
  private void checkForRecoveryStat(VM vm, final boolean localRecovery) {
    vm.invoke(new SerializableRunnable("check disk region stat") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        DistributedRegion distributedRegion = (DistributedRegion) region;
        DiskRegionStats stats = distributedRegion.getDiskRegion().getStats();
        if (localRecovery) {
          assertEquals(1, stats.getLocalInitializations());
          assertEquals(0, stats.getRemoteInitializations());
        } else {
          assertEquals(0, stats.getLocalInitializations());
          assertEquals(1, stats.getRemoteInitializations());
        }

      }
    });
  }

  /**
   * 
   * @return back up directory
   */
  protected static File getBackupDir(String name) throws Exception {
    File backUpDir = new File("BackupDir-" + name).getAbsoluteFile();
    com.gemstone.gemfire.internal.FileUtil.delete(backUpDir);
    backUpDir.mkdir();
    backUpDir.deleteOnExit();
    return backUpDir;
  }
}
