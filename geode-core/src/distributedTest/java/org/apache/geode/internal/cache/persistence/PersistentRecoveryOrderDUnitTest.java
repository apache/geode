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
package org.apache.geode.internal.cache.persistence;

import static org.apache.geode.admin.AdminDistributedSystemFactory.defineDistributedSystem;
import static org.apache.geode.admin.AdminDistributedSystemFactory.getDistributedSystem;
import static org.apache.geode.distributed.ConfigurationProperties.ACK_WAIT_THRESHOLD;
import static org.apache.geode.internal.lang.ThrowableUtils.getRootCause;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminDistributedSystemFactory;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.persistence.ConflictingPersistentDataException;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.cache.persistence.PersistentReplicatesOfflineException;
import org.apache.geode.cache.persistence.RevokedPersistentDataException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.AbstractUpdateOperation.AbstractUpdateMessage;
import org.apache.geode.internal.cache.DestroyRegionOperation.DestroyRegionMessage;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.DiskRegionStats;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InitialImageOperation.RequestImageMessage;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.PersistenceTest;

/**
 * This is a test of how persistent distributed regions recover. This test makes sure that when
 * multiple VMs are persisting the same region, they recover with the latest data during recovery.
 */
@Category({PersistenceTest.class})
public class PersistentRecoveryOrderDUnitTest extends PersistentReplicatedTestBase {

  @Override
  public Properties getDistributedSystemProperties() {
    LogWriterUtils.getLogWriter().info("Looking for ack-wait-threshold");
    String s = System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "ack-wait-threshold");
    if (s != null) {
      SAVED_ACK_WAIT_THRESHOLD = s;
      LogWriterUtils.getLogWriter().info("removing system property gemfire.ack-wait-threshold");
      System.getProperties().remove(DistributionConfig.GEMFIRE_PREFIX + "ack-wait-threshold");
    }
    Properties props = super.getDistributedSystemProperties();
    props.put(ACK_WAIT_THRESHOLD, "5");
    return props;
  }

  /**
   * Tests to make sure that a persistent region will wait for any members that were online when is
   * crashed before starting up.
   */
  @Test
  public void testWaitForLatestMember() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    LogWriterUtils.getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);

    putAnEntry(vm0);

    LogWriterUtils.getLogWriter().info("closing region in vm0");
    closeRegion(vm0);

    updateTheEntry(vm1);

    LogWriterUtils.getLogWriter().info("closing region in vm1");
    closeRegion(vm1);


    // This ought to wait for VM1 to come back
    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    AsyncInvocation future = createPersistentRegionAsync(vm0);

    waitForBlockedInitialization(vm0);

    assertTrue(future.isAlive());

    LogWriterUtils.getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);

    future.join(MAX_WAIT);
    if (future.isAlive()) {
      fail("Region not created within " + MAX_WAIT);
    }
    if (future.exceptionOccurred()) {
      throw new Exception(future.getException());
    }

    checkForEntry(vm0);
    checkForEntry(vm1);

    checkForRecoveryStat(vm1, true);
    checkForRecoveryStat(vm0, false);
  }

  /**
   * Tests to make sure that we stop waiting for a member that we revoke.
   */
  @Test
  public void testRevokeAMember() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    LogWriterUtils.getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);

    putAnEntry(vm0);

    vm0.invoke(new SerializableRunnable("Check for waiting regions") {

      @Override
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        PersistentMemberManager mm = cache.getPersistentMemberManager();
        Map<String, Set<PersistentMemberID>> waitingRegions = mm.getWaitingRegions();
        assertEquals(0, waitingRegions.size());
      }
    });

    LogWriterUtils.getLogWriter().info("closing region in vm0");
    closeRegion(vm0);

    updateTheEntry(vm1);

    LogWriterUtils.getLogWriter().info("closing region in vm1");
    closeCache(vm1);


    // This ought to wait for VM1 to come back
    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    AsyncInvocation future = createPersistentRegionAsync(vm0);

    waitForBlockedInitialization(vm0);

    assertTrue(future.isAlive());

    vm2.invoke(new SerializableRunnable("Revoke the member") {

      @Override
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          Set<PersistentID> missingIds = adminDS.getMissingPersistentMembers();
          LogWriterUtils.getLogWriter().info("waiting members=" + missingIds);
          assertEquals(1, missingIds.size());
          PersistentID missingMember = missingIds.iterator().next();
          adminDS.revokePersistentMember(missingMember.getUUID());
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
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
    SerializableRunnable checkForEntry = new SerializableRunnable("check for the entry") {

      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        assertEquals("B", region.get("A"));
      }
    };
    vm0.invoke(checkForEntry);

    // Now, we should not be able to create a region
    // in vm1, because the this member was revoked
    LogWriterUtils.getLogWriter().info("Creating region in VM1");
    IgnoredException e = IgnoredException
        .addIgnoredException(RevokedPersistentDataException.class.getSimpleName(), vm1);
    try {
      createPersistentRegion(vm1);
      fail("We should have received a split distributed system exception");
    } catch (RuntimeException expected) {
      if (!(expected.getCause() instanceof RevokedPersistentDataException)) {
        throw expected;
      }
    } finally {
      e.remove();
    }

    closeCache(vm1);
    // Restart vm0
    closeCache(vm0);
    createPersistentRegion(vm0);
  }

  /**
   * Tests to make sure that we can revoke a member before initialization, and that member will stay
   * revoked
   */
  @Test
  public void testRevokeAHostBeforeInitialization() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    LogWriterUtils.getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);

    putAnEntry(vm0);

    vm0.invoke(new SerializableRunnable("Check for waiting regions") {

      @Override
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        PersistentMemberManager mm = cache.getPersistentMemberManager();
        Map<String, Set<PersistentMemberID>> waitingRegions = mm.getWaitingRegions();
        assertEquals(0, waitingRegions.size());
      }
    });

    LogWriterUtils.getLogWriter().info("closing region in vm0");
    closeRegion(vm0);

    updateTheEntry(vm1);

    LogWriterUtils.getLogWriter().info("closing region in vm1");
    closeRegion(vm1);

    final File dirToRevoke = getDiskDirForVM(vm1);
    vm2.invoke(new SerializableRunnable("Revoke the member") {

      @Override
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          adminDS.revokePersistentMember(InetAddress.getLocalHost(),
              dirToRevoke.getCanonicalPath());
        } catch (Exception e) {
          Assert.fail("Unexpected exception", e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });

    // This shouldn't wait, because we revoked the member
    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);

    checkForRecoveryStat(vm0, true);

    // Check to make sure we recovered the old
    // value of the entry.
    SerializableRunnable checkForEntry = new SerializableRunnable("check for the entry") {

      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        assertEquals("B", region.get("A"));
      }
    };
    vm0.invoke(checkForEntry);

    // Now, we should not be able to create a region
    // in vm1, because the this member was revoked
    LogWriterUtils.getLogWriter().info("Creating region in VM1");
    IgnoredException e = IgnoredException
        .addIgnoredException(RevokedPersistentDataException.class.getSimpleName(), vm1);
    try {
      createPersistentRegion(vm1);
      fail("We should have received a split distributed system exception");
    } catch (RuntimeException expected) {
      if (!(expected.getCause() instanceof RevokedPersistentDataException)) {
        throw expected;
      }
      // Do nothing
    } finally {
      e.remove();
    }
  }

  /**
   * Test which members show up in the list of members we're waiting on.
   */
  @Test
  public void testWaitingMemberList() throws Exception {
    Host host = getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    createPersistentRegion(vm2);

    putAnEntry(vm0);

    vm0.invoke(new SerializableRunnable("Check for waiting regions") {

      @Override
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        PersistentMemberManager mm = cache.getPersistentMemberManager();
        Map<String, Set<PersistentMemberID>> waitingRegions = mm.getWaitingRegions();
        assertEquals(0, waitingRegions.size());
      }
    });

    getLogWriter().info("closing region in vm0");
    closeRegion(vm0);

    updateTheEntry(vm1);

    getLogWriter().info("closing region in vm1");
    closeRegion(vm1);

    updateTheEntry(vm2, "D");

    getLogWriter().info("closing region in vm2");
    closeRegion(vm2);


    // These ought to wait for VM2 to come back
    getLogWriter().info("Creating region in VM0");
    AsyncInvocation future0 = createPersistentRegionAsync(vm0);

    waitForBlockedInitialization(vm0);
    assertTrue(future0.isAlive());

    getLogWriter().info("Creating region in VM1");
    final AsyncInvocation future1 = createPersistentRegionAsync(vm1);
    waitForBlockedInitialization(vm1);
    assertTrue(future1.isAlive());

    vm3.invoke(new SerializableRunnable("check waiting members") {

      @Override
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
          config = defineDistributedSystem(getSystem(), "");
          adminDS = getDistributedSystem(config);
          adminDS.connect();
          Set<PersistentID> missingIds = adminDS.getMissingPersistentMembers();
          getLogWriter().info("waiting members=" + missingIds);
          assertEquals(1, missingIds.size());
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });

    vm1.invoke(new SerializableRunnable("close cache") {

      @Override
      public void run() {
        getCache().close();
      }
    });

    GeodeAwaitility.await().untilAsserted(new WaitCriterion() {

      @Override
      public boolean done() {
        return !future1.isAlive();
      }

      @Override
      public String description() {
        return "Waiting for blocked initialization to terminate because the cache was closed.";
      }
    });

    // Now we should be missing 2 members
    vm3.invoke(new SerializableRunnable("check waiting members again") {

      @Override
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
          config = defineDistributedSystem(getSystem(), "");
          adminDS = getDistributedSystem(config);
          adminDS.connect();
          final AdminDistributedSystem connectedDS = adminDS;
          GeodeAwaitility.await().untilAsserted(new WaitCriterion() {

            @Override
            public String description() {
              return "Waiting for waiting members to have 2 members";
            }

            @Override
            public boolean done() {
              Set<PersistentID> missingIds;
              try {
                missingIds = connectedDS.getMissingPersistentMembers();
              } catch (AdminException e) {
                throw new RuntimeException(e);
              }
              return 2 == missingIds.size();
            }

          });

        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });
  }

  /**
   * Use Case AB are alive A crashes. B crashes. B starts up. It should not wait for A.
   */
  @Test
  public void testDontWaitForOldMember() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    putAnEntry(vm0);

    closeRegion(vm0);

    updateTheEntry(vm1);

    closeRegion(vm1);


    // This shouldn't wait for vm0 to come back
    createPersistentRegion(vm1);

    checkForEntry(vm1);

    checkForRecoveryStat(vm1, true);
  }

  /**
   * Tests that if two members crash simultaneously, they negotiate which member should initialize
   * with what is on disk and which member should copy data from that member.
   */
  @Test
  public void testSimultaneousCrash() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    putAnEntry(vm0);
    updateTheEntry(vm1);


    // Copy the regions as they are with both
    // members online.
    backupDir(vm0);
    backupDir(vm1);

    // destroy the members
    closeCache(vm0);
    closeCache(vm1);

    // now restore from backup
    restoreBackup(vm0);
    restoreBackup(vm1);

    // This ought to wait for VM1 to come back
    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    AsyncInvocation future = createPersistentRegionAsync(vm0);
    waitForBlockedInitialization(vm0);
    assertTrue(future.isAlive());

    LogWriterUtils.getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);

    future.join(MAX_WAIT);
    if (future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    if (future.exceptionOccurred()) {
      throw new Exception(future.getException());
    }

    checkForEntry(vm0);
    checkForEntry(vm1);
  }

  /**
   * Tests that persistent members pass along the list of crashed members to later persistent
   * members. Eg. AB are running A crashes C is tarted B crashes C crashes AC are started, they
   * should figure out who has the latest data, without needing B.
   */
  @Test
  public void testTransmitCrashedMembers() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    putAnEntry(vm0);

    closeRegion(vm0);

    // VM 2 should be told about the fact
    // that VM1 has crashed.
    createPersistentRegion(vm2);

    updateTheEntry(vm1);

    closeRegion(vm1);

    closeRegion(vm2);


    // This ought to wait for VM1 to come back
    AsyncInvocation future = createPersistentRegionAsync(vm0);
    waitForBlockedInitialization(vm0);
    assertTrue(future.isAlive());

    // VM2 has the most recent data, it should start
    createPersistentRegion(vm2);

    // VM0 should be informed that VM2 is older, so it should start
    future.getResult(MAX_WAIT);

    checkForEntry(vm0);
    checkForEntry(vm2);
  }

  /**
   * Tests that a persistent region cannot recover from a non persistent region.
   */
  @Test
  public void testRecoverFromNonPeristentRegion() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    createPersistentRegion(vm0);
    createNonPersistentRegion(vm1);

    putAnEntry(vm0);

    closeRegion(vm0);

    try {
      updateTheEntry(vm1);
      fail("expected PersistentReplicatesOfflineException not thrown");
    } catch (Exception expected) {
      if (!(expected.getCause() instanceof PersistentReplicatesOfflineException)) {
        throw expected;
      }
    }

    // This should initialize from vm1
    createPersistentRegion(vm0);

    checkForRecoveryStat(vm0, true);

    updateTheEntry(vm1);
    checkForEntry(vm0);
    checkForEntry(vm1);
  }

  @Test
  public void testFinishIncompleteInitializationNoSend() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Add a hook which will disconnect the DS before sending a prepare message
    vm1.invoke(new SerializableRunnable() {

      @Override
      public void run() {

        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeSendMessage(ClusterDistributionManager dm,
              DistributionMessage message) {
            if (message instanceof PrepareNewPersistentMemberMessage) {
              DistributionMessageObserver.setInstance(null);
              getSystem().disconnect();
            }
          }

          @Override
          public void afterProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {

          }
        });
      }
    });
    createPersistentRegion(vm0);

    putAnEntry(vm0);
    updateTheEntry(vm0);

    try {
      createPersistentRegion(vm1);
    } catch (Exception e) {
      if (!(e.getCause() instanceof DistributedSystemDisconnectedException)) {
        throw e;
      }
    }

    closeRegion(vm0);

    // This wait for VM0 to come back
    AsyncInvocation async1 = createPersistentRegionAsync(vm1);

    waitForBlockedInitialization(vm1);

    createPersistentRegion(vm0);

    async1.getResult();

    checkForEntry(vm1);

    vm0.invoke(new SerializableRunnable("check for offline members") {
      @Override
      public void run() {
        Cache cache = getCache();
        DistributedRegion region = (DistributedRegion) cache.getRegion(REGION_NAME);
        PersistentMembershipView view = region.getPersistenceAdvisor().getMembershipView();
        DiskRegion dr = region.getDiskRegion();

        assertEquals(Collections.emptySet(), dr.getOfflineMembers());
        assertEquals(1, dr.getOnlineMembers().size());
      }
    });
  }

  HashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> getAllMemberToVersion(
      RegionVersionVector rvv) {
    HashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> allMemberToVersion =
        new HashMap(rvv.getMemberToVersion());
    RegionVersionHolder localHolder = rvv.getLocalExceptions().clone();
    localHolder.setVersion(rvv.getCurrentVersion());
    allMemberToVersion.put((DiskStoreID) rvv.getOwnerId(), localHolder);
    return allMemberToVersion;
  }

  protected Object getEntry(VM vm, final String key) {
    SerializableCallable getEntry = new SerializableCallable("get entry") {

      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        return region.get(key);
      }
    };

    return (vm.invoke(getEntry));
  }

  protected RegionVersionVector getRVV(VM vm) throws IOException, ClassNotFoundException {
    SerializableCallable createData = new SerializableCallable("getRVV") {

      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion) cache.getRegion(REGION_NAME);
        RegionVersionVector rvv = region.getVersionVector();
        rvv = rvv.getCloneForTransmission();
        HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);

        // Using gemfire serialization because
        // RegionVersionVector is not java serializable
        DataSerializer.writeObject(rvv, hdos);
        return hdos.toByteArray();
      }
    };
    byte[] result = (byte[]) vm.invoke(createData);
    ByteArrayInputStream bais = new ByteArrayInputStream(result);
    return DataSerializer.readObject(new DataInputStream(bais));
  }

  @Test
  public void testPersistConflictOperations() throws Exception {
    doTestPersistConflictOperations(true);
  }

  @Test
  public void testPersistConflictOperationsAsync() throws Exception {
    doTestPersistConflictOperations(false);
  }

  /**
   * vm0 and vm1 are peers, each holds a DR. They do put to the same key for different value at the
   * same time. Use DistributionMessageObserver.beforeSendMessage to hold on the distribution
   * message. One of the member will persist the conflict version tag, while another member will
   * persist both of the 2 operations. Overall, their RVV should match after the operations.
   */
  private void doTestPersistConflictOperations(boolean diskSync) throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Add a hook which will disconnect the DS before sending a prepare message
    SerializableRunnable addObserver = new SerializableRunnable() {
      @Override
      public void run() {
        // System.setProperty("disk.TRACE_WRITES", "true");
        // System.setProperty("disk.TRACE_RECOVERY", "true");
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeSendMessage(ClusterDistributionManager dm,
              DistributionMessage message) {
            if (message instanceof AbstractUpdateMessage) {
              try {
                Thread.sleep(2000);
                getCache().getLogger().info("testPersistConflictOperations, beforeSendMessage");
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
          }

          @Override
          public void afterProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {
            if (message instanceof AbstractUpdateMessage) {
              getCache().getLogger().info("testPersistConflictOperations, beforeSendMessage");
              DistributionMessageObserver.setInstance(null);
            }
          }
        });
      }
    };
    vm0.invoke(addObserver);
    vm1.invoke(addObserver);

    AsyncInvocation future0 = createPersistentRegionAsync(vm0, diskSync);
    AsyncInvocation future1 = createPersistentRegionAsync(vm1, diskSync);
    future0.join(MAX_WAIT);
    future1.join(MAX_WAIT);
    // createPersistentRegion(vm0);
    // createPersistentRegion(vm1);

    AsyncInvocation ins0 = vm0.invokeAsync(new SerializableRunnable("change the entry") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.put("A", "vm0");
      }
    });
    AsyncInvocation ins1 = vm1.invokeAsync(new SerializableRunnable("change the entry") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.put("A", "vm1");
      }
    });
    ins0.join(MAX_WAIT);
    ins1.join(MAX_WAIT);

    RegionVersionVector rvv0 = getRVV(vm0);
    RegionVersionVector rvv1 = getRVV(vm1);
    assertSameRVV(rvv1, rvv0);


    Object value0 = getEntry(vm0, "A");
    Object value1 = getEntry(vm1, "A");
    assertEquals(value0, value1);

    closeRegion(vm0);
    closeRegion(vm1);

    // recover
    future1 = createPersistentRegionAsync(vm1, diskSync);
    future0 = createPersistentRegionAsync(vm0, diskSync);
    future1.join(MAX_WAIT);
    future0.join(MAX_WAIT);

    value0 = getEntry(vm0, "A");
    value1 = getEntry(vm1, "A");
    assertEquals(value0, value1);

    rvv0 = getRVV(vm0);
    rvv1 = getRVV(vm1);
    assertSameRVV(rvv1, rvv0);

    // round 2: async disk write
    vm0.invoke(addObserver);
    vm1.invoke(addObserver);

    ins0 = vm0.invokeAsync(new SerializableRunnable("change the entry at vm0") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        for (int i = 0; i < 1000; i++) {
          region.put("A", "vm0-" + i);
        }
      }
    });
    ins1 = vm1.invokeAsync(new SerializableRunnable("change the entry at vm1") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        for (int i = 0; i < 1000; i++) {
          region.put("A", "vm1-" + i);
        }
      }
    });
    ins0.join(MAX_WAIT);
    ins1.join(MAX_WAIT);

    rvv0 = getRVV(vm0);
    rvv1 = getRVV(vm1);
    assertSameRVV(rvv1, rvv0);

    value0 = getEntry(vm0, "A");
    value1 = getEntry(vm1, "A");
    assertEquals(value0, value1);

    closeCache(vm0);
    closeCache(vm1);

    // recover again
    future1 = createPersistentRegionAsync(vm1, diskSync);
    future0 = createPersistentRegionAsync(vm0, diskSync);
    future1.join(MAX_WAIT);
    future0.join(MAX_WAIT);

    value0 = getEntry(vm0, "A");
    value1 = getEntry(vm1, "A");
    assertEquals(value0, value1);

    rvv0 = getRVV(vm0);
    rvv1 = getRVV(vm1);
    assertSameRVV(rvv1, rvv0);
  }

  /**
   * Tests that even non persistent regions can transmit the list of crashed members to other
   * persistent regions, So that the persistent regions can negotiate who has the latest data during
   * recovery.
   */
  @Test
  public void testTransmitCrashedMembersWithNonPeristentRegion() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    createPersistentRegion(vm0);
    createNonPersistentRegion(vm1);

    putAnEntry(vm0);

    closeRegion(vm0);

    // VM 2 should not do a GII from vm1,
    // it should wait for vm0
    AsyncInvocation future = createPersistentRegionWithWait(vm2);

    createPersistentRegion(vm0);

    future.getResult(MAX_WAIT);

    closeRegion(vm0);

    updateTheEntry(vm1);

    closeRegion(vm1);

    closeRegion(vm2);


    // VM2 has the most recent data, it should start
    createPersistentRegion(vm2);

    // VM0 should be informed that it has older data than VM2, so
    // it should initialize from vm2
    createPersistentRegion(vm0);


    checkForEntry(vm0);
    checkForEntry(vm2);
  }

  @Test
  public void testSplitBrain() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    createPersistentRegion(vm0);

    putAnEntry(vm0);

    closeRegion(vm0);

    createPersistentRegion(vm1);

    updateTheEntry(vm1);

    closeRegion(vm1);


    // VM0 doesn't know that VM1 ever existed
    // so it will start up.
    createPersistentRegion(vm0);

    IgnoredException e = IgnoredException
        .addIgnoredException(ConflictingPersistentDataException.class.getSimpleName(), vm1);
    try {
      // VM1 should not start up, because we should detect that vm1
      // was never in the same distributed system as vm0
      createPersistentRegion(vm1);
      fail("Should have thrown an exception, vm1 is from a 'different' distributed system");
    } catch (RuntimeException ok) {
      if (!(ok.getCause() instanceof ConflictingPersistentDataException)) {
        throw ok;
      }
    } finally {
      e.remove();
    }
  }

  /**
   * Test to make sure that if if a member crashes while a GII is in progress, we wait for the
   * member to come back for starting.
   */
  @Test
  public void testCrashDuringGII() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    LogWriterUtils.getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);

    putAnEntry(vm0);

    LogWriterUtils.getLogWriter().info("closing region in vm0");
    closeRegion(vm0);

    updateTheEntry(vm1);

    LogWriterUtils.getLogWriter().info("closing region in vm1");
    closeRegion(vm1);


    // This ought to wait for VM1 to come back
    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    AsyncInvocation future = createPersistentRegionAsync(vm0);

    waitForBlockedInitialization(vm0);

    assertTrue(future.isAlive());

    // Add a hook which will disconnect from the distributed
    // system when the initial image message shows up.
    vm1.invoke(new SerializableRunnable() {

      @Override
      public void run() {
        sawRequestImageMessage.set(false);

        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {
            if (message instanceof RequestImageMessage) {
              DistributionMessageObserver.setInstance(null);
              disconnectFromDS();
              synchronized (sawRequestImageMessage) {
                sawRequestImageMessage.set(true);
                sawRequestImageMessage.notifyAll();
              }
            }
          }

          @Override
          public void afterProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {

          }
        });
      }
    });

    createPersistentRegion(vm1);

    vm1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        synchronized (sawRequestImageMessage) {
          try {
            while (!sawRequestImageMessage.get()) {
              sawRequestImageMessage.wait();
            }
          } catch (InterruptedException ex) {
          }
        }
      }
    });

    waitForBlockedInitialization(vm0);

    assertTrue(future.isAlive());

    // Now create the region again. The initialization should
    // work (the observer was cleared when we disconnected from the DS.
    createPersistentRegion(vm1);;

    future.join(MAX_WAIT);
    if (future.isAlive()) {
      fail("Region not created within" + MAX_WAIT);
    }
    if (future.exceptionOccurred()) {
      throw new Exception(future.getException());
    }

    checkForEntry(vm0);
    checkForEntry(vm1);

    checkForRecoveryStat(vm1, true);
    checkForRecoveryStat(vm0, false);
  }

  /**
   * Test to make sure we don't leak any persistent ids if a member does GII while a distributed
   * destroy is in progress
   */
  @Test
  public void testGIIDuringDestroy() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);

    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);

    // Add a hook which will disconnect from the distributed
    // system when the initial image message shows up.
    vm1.invoke(new SerializableRunnable() {

      @Override
      public void run() {

        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {
            if (message instanceof DestroyRegionMessage) {
              createPersistentRegionAsync(vm2);
              try {
                Thread.sleep(10000);
              } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              } finally {
                DistributionMessageObserver.setInstance(null);
              }
            }
          }

          @Override
          public void afterProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {

          }

          @Override
          public void beforeSendMessage(ClusterDistributionManager dm,
              DistributionMessage message) {}
        });
      }
    });

    createPersistentRegion(vm1);

    vm0.invoke(new SerializableRunnable("Destroy region") {

      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.destroyRegion();
      }
    });


    vm1.invoke(new SerializableRunnable("check destroyed") {

      @Override
      public void run() {
        Cache cache = getCache();
        assertNull(cache.getRegion(REGION_NAME));
      }
    });

    vm2.invoke(new SerializableRunnable("Wait for region creation") {

      @Override
      public void run() {
        final Cache cache = getCache();
        GeodeAwaitility.await().untilAsserted(new WaitCriterion() {

          @Override
          public String description() {
            return "Waiting for creation of region " + REGION_NAME;
          }

          @Override
          public boolean done() {
            Region region = cache.getRegion(REGION_NAME);
            return region != null;
          }

        });
      }
    });

    vm2.invoke(new SerializableRunnable("Check offline members") {

      @Override
      public void run() {
        final Cache cache = getCache();
        DistributedRegion region = (DistributedRegion) cache.getRegion(REGION_NAME);
        PersistenceAdvisor persistAdvisor = region.getPersistenceAdvisor();
        assertEquals(Collections.emptySet(),
            persistAdvisor.getMembershipView().getOfflineMembers());
      }
    });
  }

  @Test
  public void testCrashDuringPreparePersistentId() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Add a hook which will disconnect from the distributed
    // system when the initial image message shows up.
    vm0.invoke(new SerializableRunnable() {

      @Override
      public void run() {

        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {
            if (message instanceof PrepareNewPersistentMemberMessage) {
              DistributionMessageObserver.setInstance(null);
              disconnectFromDS();
            }
          }

          @Override
          public void afterProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {

          }
        });
      }
    });
    createPersistentRegion(vm0);

    putAnEntry(vm0);

    updateTheEntry(vm0);

    AsyncInvocation async1 = createPersistentRegionAsync(vm1);

    // Wait for vm 1 to get stuck waiting for vm0, because vm0 has crashed
    waitForBlockedInitialization(vm1);

    // closeCache(vm0);
    closeCache(vm1);

    try {
      async1.getResult();
      fail("Should have seen a CacheClosedException");
    } catch (AssertionError e) {
      if (!CacheClosedException.class.isInstance(getRootCause(e))) {
        throw e;
      }
    }

    createPersistentRegion(vm0);

    createPersistentRegion(vm1);;

    checkForEntry(vm0);
    checkForEntry(vm1);
  }

  @Test
  public void testSplitBrainWithNonPersistentRegion() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    createPersistentRegion(vm1);

    putAnEntry(vm1);
    updateTheEntry(vm1);

    closeRegion(vm1);

    createNonPersistentRegion(vm0);

    IgnoredException e =
        IgnoredException.addIgnoredException(IllegalStateException.class.getSimpleName(), vm1);
    try {
      createPersistentRegion(vm1);
      fail("Should have received an IllegalState exception");
    } catch (Exception expected) {
      if (!(expected.getCause() instanceof IllegalStateException)) {
        throw expected;
      }
    } finally {
      e.remove();
    }

    closeRegion(vm0);

    createPersistentRegion(vm1);

    checkForEntry(vm1);

    checkForRecoveryStat(vm1, true);
  }

  @Test
  public void testMissingEntryOnDisk() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Add a hook which will perform some updates while the region is initializing
    vm0.invoke(new SerializableRunnable() {

      @Override
      public void run() {

        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {
            if (message instanceof RequestImageMessage) {
              Cache cache = getCache();
              Region region = cache.getRegion(REGION_NAME);
              if (region == null) {
                LogWriterUtils.getLogWriter().severe(
                    "removing listener for PersistentRecoveryOrderDUnitTest because region was not found: "
                        + REGION_NAME);
                Object old = DistributionMessageObserver.setInstance(null);
                if (old != this) {
                  LogWriterUtils.getLogWriter().severe(
                      "removed listener was not the invoked listener",
                      new Exception("stack trace"));
                }
                return;
              }
              region.put("A", "B");
              region.destroy("A");
              region.put("A", "C");
            }
          }

          @Override
          public void afterProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {

          }
        });
      }
    });
    createPersistentRegion(vm0);

    createPersistentRegion(vm1);

    checkForEntry(vm1);

    closeRegion(vm0);

    closeRegion(vm1);

    // This should work
    createPersistentRegion(vm1);

    checkForEntry(vm1);
  }

  /**
   * Tests to make sure that we stop waiting for a member that we revoke.
   *
   */
  @Test
  public void testCompactFromAdmin() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    createPersistentRegionWithoutCompaction(vm0);
    createPersistentRegionWithoutCompaction(vm1);

    vm1.invoke(new SerializableRunnable("Create some data") {

      @Override
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        Region region = cache.getRegion(REGION_NAME);
        for (int i = 0; i < 1024; i++) {
          region.put(i, new byte[1024]);
        }

        for (int i = 2; i < 1024; i++) {
          assertTrue(region.destroy(i) != null);
        }
        DiskStore store = cache.findDiskStore(REGION_NAME);
        store.forceRoll();
      }
    });

    vm2.invoke(new SerializableRunnable("Compact") {

      @Override
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        DistributedSystemConfig config;
        AdminDistributedSystem adminDS = null;
        try {
          config = AdminDistributedSystemFactory.defineDistributedSystem(getSystem(), "");
          adminDS = AdminDistributedSystemFactory.getDistributedSystem(config);
          adminDS.connect();
          Map<DistributedMember, Set<PersistentID>> missingIds = adminDS.compactAllDiskStores();
          assertEquals(2, missingIds.size());
          for (Set<PersistentID> value : missingIds.values()) {
            assertEquals(1, value.size());
          }
        } catch (AdminException e) {
          throw new RuntimeException(e);
        } finally {
          if (adminDS != null) {
            adminDS.disconnect();
          }
        }
      }
    });

    SerializableRunnable compactVM = new SerializableRunnable("compact") {
      @Override
      public void run() {
        Cache cache = getCache();
        DiskStore ds = cache.findDiskStore(REGION_NAME);
        assertFalse(ds.forceCompaction());
      }
    };

    vm0.invoke(compactVM);
    vm1.invoke(compactVM);
  }

  @Test
  public void testCloseDuringRegionOperation() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    // Try to make sure there are some operations in flight while closing the cache
    SerializableCallable createData0 = new SerializableCallable() {

      @Override
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);

        int i = 0;
        while (true) {
          try {
            region.put(0, i);
            i++;
          } catch (RegionDestroyedException e) {
            break;
          } catch (CacheClosedException e) {
            break;
          }
        }
        return i - 1;
      }
    };

    SerializableCallable createData1 = new SerializableCallable() {

      @Override
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);

        int i = 0;
        while (true) {
          try {
            region.put(1, i);
            i++;
          } catch (RegionDestroyedException e) {
            break;
          } catch (CacheClosedException e) {
            break;
          }
        }
        return i - 1;
      }
    };

    AsyncInvocation asyncCreate0 = vm0.invokeAsync(createData0);
    AsyncInvocation asyncCreate1 = vm1.invokeAsync(createData1);

    Thread.sleep(500);

    AsyncInvocation close0 = closeCacheAsync(vm0);
    AsyncInvocation close1 = closeCacheAsync(vm1);

    // wait for the close to finish
    close0.getResult();
    close1.getResult();

    Integer lastSuccessfulInt0 = (Integer) asyncCreate0.getResult();
    Integer lastSuccessfulInt1 = (Integer) asyncCreate1.getResult();
    System.err
        .println("Cache was closed on 0->" + lastSuccessfulInt0 + ",1->" + lastSuccessfulInt1);

    AsyncInvocation create1 = createPersistentRegionAsync(vm0);
    AsyncInvocation create2 = createPersistentRegionAsync(vm1);

    create1.getResult();
    create2.getResult();

    checkConcurrentCloseValue(vm0, vm1, 0, lastSuccessfulInt0);
    checkConcurrentCloseValue(vm0, vm1, 1, lastSuccessfulInt1);
  }

  @Ignore("TODO: Disabled due to bug #52240")
  @Test
  public void testCloseDuringRegionOperationWithTX() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    createInternalPersistentRegionAsync(vm0).getResult();
    createInternalPersistentRegionAsync(vm1).getResult();
    createInternalPersistentRegionAsync(vm2).getResult();


    AsyncInvocation asyncCreate0 = createDataAsyncTX(vm0, 0);
    AsyncInvocation asyncCreate1 = createDataAsyncTX(vm0, 1);
    AsyncInvocation asyncCreate2 = createDataAsyncTX(vm0, 2);

    Thread.sleep(500);

    AsyncInvocation close0 = closeCacheAsync(vm0);
    AsyncInvocation close1 = closeCacheAsync(vm1);
    AsyncInvocation close2 = closeCacheAsync(vm2);

    // wait for the close to finish
    close0.getResult();
    close1.getResult();
    close2.getResult();

    Integer lastSuccessfulInt0 = (Integer) asyncCreate0.getResult();
    Integer lastSuccessfulInt1 = (Integer) asyncCreate1.getResult();
    Integer lastSuccessfulInt2 = (Integer) asyncCreate2.getResult();
    System.err.println("Cache was closed on 0->" + lastSuccessfulInt0 + ",1->" + lastSuccessfulInt1
        + ",2->" + lastSuccessfulInt2);

    AsyncInvocation create0 = createInternalPersistentRegionAsync(vm0);
    AsyncInvocation create1 = createInternalPersistentRegionAsync(vm1);
    AsyncInvocation create2 = createInternalPersistentRegionAsync(vm2);

    create0.getResult();
    create1.getResult();
    create2.getResult();

    checkConcurrentCloseValue(vm0, vm1, 0, lastSuccessfulInt0);
    checkConcurrentCloseValue(vm0, vm1, 1, lastSuccessfulInt1);
    checkConcurrentCloseValue(vm0, vm1, 2, lastSuccessfulInt2);
  }

  /**
   * Tests to make sure that after we get a conflicting persistent data exception, we can still
   * recover.
   */
  @Test
  public void testRecoverAfterConflict() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    createPersistentRegion(vm0);
    putAnEntry(vm0);
    LogWriterUtils.getLogWriter().info("closing region in vm0");
    closeCache(vm0);

    LogWriterUtils.getLogWriter().info("Creating region in VM1");
    createPersistentRegion(vm1);
    putAnEntry(vm1);

    LogWriterUtils.getLogWriter().info("Creating region in VM0");
    IgnoredException ex =
        IgnoredException.addIgnoredException("ConflictingPersistentDataException", vm0);
    try {
      // this should cause a conflict
      createPersistentRegion(vm0);
      fail("Should have received a ConflictingPersistentDataException");
    } catch (RuntimeException e) {
      if (!(e.getCause() instanceof ConflictingPersistentDataException)) {
        throw e;
      }
    } finally {
      ex.remove();
    }

    LogWriterUtils.getLogWriter().info("closing region in vm1");
    closeCache(vm1);

    // This should work now
    createPersistentRegion(vm0);

    updateTheEntry(vm0);

    ex = IgnoredException.addIgnoredException("ConflictingPersistentDataException", vm1);
    // Now make sure vm1 gets a conflict
    LogWriterUtils.getLogWriter().info("Creating region in VM1");
    try {
      // this should cause a conflict
      createPersistentRegion(vm1);
      fail("Should have received a ConflictingPersistentDataException");
    } catch (RuntimeException e) {
      if (!(e.getCause() instanceof ConflictingPersistentDataException)) {
        throw e;
      }
    } finally {
      ex.remove();
    }
  }

  private static final AtomicBoolean sawRequestImageMessage = new AtomicBoolean(false);

  private AsyncInvocation createPersistentRegionAsync(final VM vm, final boolean diskSynchronous) {
    SerializableRunnable createRegion = new SerializableRunnable("Create persistent region") {
      @Override
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        File dir = getDiskDirForVM(vm);
        dir.mkdirs();
        dsf.setDiskDirs(new File[] {dir});
        dsf.setMaxOplogSize(1);
        DiskStore ds = dsf.create(REGION_NAME);
        RegionFactory rf = new RegionFactory();
        rf.setDiskStoreName(ds.getName());
        rf.setDiskSynchronous(diskSynchronous);
        rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        rf.create(REGION_NAME);
      }
    };
    return vm.invokeAsync(createRegion);
  }

  private void assertSameRVV(RegionVersionVector rvv1, RegionVersionVector rvv2) {
    if (!rvv1.sameAs(rvv2)) {
      fail("Expected " + rvv1 + " but was " + rvv2);
    }
  }

  private AsyncInvocation createDataAsyncTX(VM vm1, final int member) {
    SerializableCallable createData1 = new SerializableCallable() {

      @Override
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);

        int i = 0;
        TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
        while (true) {
          try {
            txManager.begin();
            region.put(member, i);
            txManager.commit();
            i++;
          } catch (RegionDestroyedException e) {
            break;
          } catch (CacheClosedException e) {
            break;
          } catch (IllegalArgumentException e) {
            if (!e.getMessage().contains("Invalid txLockId")) {
              throw e;
            }
            break;
          } catch (LockServiceDestroyedException e) {
            break;
          }
        }
        return i - 1;
      }
    };
    AsyncInvocation asyncCreate1 = vm1.invokeAsync(createData1);
    return asyncCreate1;
  }

  private void checkConcurrentCloseValue(VM vm0, VM vm1, final int key, int lastSuccessfulInt) {
    SerializableCallable getValue = new SerializableCallable() {

      @Override
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        int value = (Integer) region.get(key);
        return value;
      }
    };

    int vm1Value = (Integer) vm0.invoke(getValue);
    int vm2Value = (Integer) vm1.invoke(getValue);
    assertEquals(vm1Value, vm2Value);
    assertTrue("value = " + vm1Value + ", lastSuccessfulInt=" + lastSuccessfulInt,
        vm1Value == lastSuccessfulInt || vm1Value == lastSuccessfulInt + 1);
  }

  private void checkForEntry(VM vm) {
    SerializableRunnable checkForEntry = new SerializableRunnable("check for the entry") {

      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        assertEquals("C", region.get("A"));
      }
    };
    vm.invoke(checkForEntry);
  }

  protected void updateTheEntry(VM vm1) {
    updateTheEntry(vm1, "C");
  }

  protected void updateTheEntry(VM vm1, final String value) {
    vm1.invoke(new SerializableRunnable("change the entry") {

      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.put("A", value);
      }
    });
  }

  protected void putAnEntry(VM vm0) {
    vm0.invoke(new SerializableRunnable("Put an entry") {

      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(REGION_NAME);
        region.put("A", "B");
      }
    });
  }

  private void checkForRecoveryStat(VM vm, final boolean localRecovery) {
    vm.invoke(new SerializableRunnable("check disk region stat") {

      @Override
      public void run() {
        Cache cache = getCache();
        DistributedRegion region = (DistributedRegion) cache.getRegion(REGION_NAME);
        DiskRegionStats stats = region.getDiskRegion().getStats();
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

  protected AsyncInvocation createInternalPersistentRegionAsync(final VM vm) {
    SerializableRunnable createRegion = new SerializableRunnable("Create persistent region") {
      @Override
      public void run() {
        Cache cache = getCache();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        File dir = getDiskDirForVM(vm);
        dir.mkdirs();
        dsf.setDiskDirs(new File[] {dir});
        dsf.setMaxOplogSize(1);
        DiskStore ds = dsf.create(REGION_NAME);
        InternalRegionArguments internalArgs = new InternalRegionArguments();
        internalArgs.setIsUsedForMetaRegion(true);
        internalArgs.setMetaRegionWithTransactions(true);
        AttributesFactory rf = new AttributesFactory();
        rf.setDiskStoreName(ds.getName());
        rf.setDiskSynchronous(true);
        rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        rf.setScope(Scope.DISTRIBUTED_ACK);
        try {
          ((GemFireCacheImpl) cache).createVMRegion(REGION_NAME, rf.create(), internalArgs);
        } catch (ClassNotFoundException e) {
          Assert.fail("error", e);
        } catch (IOException e) {
          Assert.fail("error", e);
        }
      }
    };
    return vm.invokeAsync(createRegion);
  }
}
