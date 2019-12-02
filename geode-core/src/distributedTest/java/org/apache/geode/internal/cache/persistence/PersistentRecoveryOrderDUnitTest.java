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
import static org.apache.geode.internal.net.SocketCreator.getLocalHost;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.DataSerializer;
import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheTransactionManager;
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
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.AbstractUpdateOperation.AbstractUpdateMessage;
import org.apache.geode.internal.cache.DestroyRegionOperation.DestroyRegionMessage;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.DiskRegionStats;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.InitialImageOperation.RequestImageMessage;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.junit.categories.PersistenceTest;
import org.apache.geode.test.junit.rules.serializable.SerializableErrorCollector;

/**
 * This is a test of how persistent distributed regions recover. This test makes sure that when
 * multiple VMs are persisting the same region, they recover with the latest data during recovery.
 */
@Category(PersistenceTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class PersistentRecoveryOrderDUnitTest extends PersistentReplicatedTestBase {

  private static final AtomicBoolean SAW_REQUEST_IMAGE_MESSAGE = new AtomicBoolean();

  private static volatile InternalDistributedSystem system;

  @Rule
  public DistributedDiskDirRule diskDirRule = new DistributedDiskDirRule();

  @Rule
  public SerializableErrorCollector errorCollector = new SerializableErrorCollector();

  @After
  public void tearDown() {
    for (VM vm : toArray(getAllVMs(), getController())) {
      vm.invoke(() -> {
        DistributionMessageObserver.setInstance(null);
      });
    }
    disconnectAllFromDS();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties configProperties = super.getDistributedSystemProperties();
    configProperties.setProperty(ACK_WAIT_THRESHOLD, "5");
    return configProperties;
  }

  /**
   * Tests to make sure that a persistent region will wait for any members that were online when is
   * crashed before starting up.
   */
  @Test
  public void testWaitForLatestMember() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    putAnEntry(vm0);
    closeRegion(vm0);

    updateTheEntry(vm1);
    closeRegion(vm1);

    // This ought to wait for VM1 to come back
    AsyncInvocation createPersistentRegionInVM0 = createPersistentRegionAsync(vm0);
    waitForBlockedInitialization(vm0);
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    createPersistentRegion(vm1);

    createPersistentRegionInVM0.await();

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
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    putAnEntry(vm0);

    vm0.invoke(() -> {
      PersistentMemberManager persistentMemberManager = getCache().getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> waitingRegions =
          persistentMemberManager.getWaitingRegions();
      assertThat(waitingRegions).isEmpty();
    });

    closeRegion(vm0);

    updateTheEntry(vm1);

    closeCache(vm1);

    // This ought to wait for VM1 to come back
    AsyncInvocation createPersistentRegionInVM0 = createPersistentRegionAsync(vm0);
    waitForBlockedInitialization(vm0);
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    vm2.invoke(() -> {
      getCache();
      AdminDistributedSystem adminDS = null;
      try {
        DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
        adminDS = getDistributedSystem(config);
        adminDS.connect();

        Set<PersistentID> missingIds = adminDS.getMissingPersistentMembers();
        assertThat(missingIds).hasSize(1);

        PersistentID missingMember = missingIds.iterator().next();
        adminDS.revokePersistentMember(missingMember.getUUID());
      } finally {
        if (adminDS != null) {
          adminDS.disconnect();
        }
      }
    });

    createPersistentRegionInVM0.await();

    checkForRecoveryStat(vm0, true);

    // Check to make sure we recovered the old value of the entry.
    vm0.invoke(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      assertThat(region.get("A")).isEqualTo("B");
    });

    // Now, we should not be able to create a region in vm1, because the this member was revoked
    try (IgnoredException ignored = addIgnoredException(RevokedPersistentDataException.class)) {
      Throwable thrown = catchThrowable(() -> createPersistentRegion(vm1));
      assertThat(thrown).hasRootCauseInstanceOf(RevokedPersistentDataException.class);
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
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    putAnEntry(vm0);

    vm0.invoke(() -> {
      PersistentMemberManager persistentMemberManager = getCache().getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> waitingRegions =
          persistentMemberManager.getWaitingRegions();
      assertThat(waitingRegions).isEmpty();
    });

    closeRegion(vm0);

    updateTheEntry(vm1);

    closeRegion(vm1);

    File dirToRevoke = getDiskDirForVM(vm1);
    vm2.invoke(() -> {
      getCache();
      AdminDistributedSystem adminDS = null;
      try {
        DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
        adminDS = getDistributedSystem(config);
        adminDS.connect();
        adminDS.revokePersistentMember(getLocalHost(), dirToRevoke.getCanonicalPath());
      } finally {
        if (adminDS != null) {
          adminDS.disconnect();
        }
      }
    });

    // This shouldn't wait, because we revoked the member
    createPersistentRegion(vm0);

    checkForRecoveryStat(vm0, true);

    // Check to make sure we recovered the old value of the entry.
    vm0.invoke(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      assertThat(region.get("A")).isEqualTo("B");
    });

    // Now, we should not be able to create a region
    // in vm1, because the this member was revoked
    try (IgnoredException e = addIgnoredException(RevokedPersistentDataException.class)) {
      Throwable thrown = catchThrowable(() -> createPersistentRegion(vm1));
      assertThat(thrown).hasRootCauseInstanceOf(RevokedPersistentDataException.class);
    }
  }

  /**
   * Test which members show up in the list of members we're waiting on.
   */
  @Test
  public void testWaitingMemberList() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);
    VM vm3 = getVM(3);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    createPersistentRegion(vm2);

    putAnEntry(vm0);

    vm0.invoke(() -> {
      PersistentMemberManager persistentMemberManager = getCache().getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> waitingRegions =
          persistentMemberManager.getWaitingRegions();
      assertThat(waitingRegions).isEmpty();
    });

    closeCache(vm0);

    updateTheEntry(vm1);
    closeCache(vm1);

    updateTheEntry(vm2, "D");
    closeCache(vm2);

    // These ought to wait for VM2 to come back
    AsyncInvocation createPersistentRegionInVM0 = createPersistentRegionAsync(vm0);
    waitForBlockedInitialization(vm0);
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    AsyncInvocation createPersistentRegionInVM1 = createPersistentRegionAsync(vm1);
    waitForBlockedInitialization(vm1);
    assertThat(createPersistentRegionInVM1.isAlive()).isTrue();

    vm3.invoke(() -> {
      getCache();
      AdminDistributedSystem adminDS = null;
      try {
        DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
        adminDS = getDistributedSystem(config);
        adminDS.connect();

        Set<PersistentID> missingIds = adminDS.getMissingPersistentMembers();
        assertThat(missingIds).hasSize(1);
      } finally {
        if (adminDS != null) {
          adminDS.disconnect();
        }
      }
    });

    vm1.invoke(() -> {
      getCache().close();
    });

    await().until(() -> !createPersistentRegionInVM1.isAlive());

    // Now we should be missing 2 members
    vm3.invoke(() -> {
      getCache();
      AdminDistributedSystem adminDS = null;
      try {
        DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
        adminDS = getDistributedSystem(config);
        adminDS.connect();

        AdminDistributedSystem connectedDS = adminDS;
        await().until(() -> {
          Set<PersistentID> missingIds = connectedDS.getMissingPersistentMembers();
          return 2 == missingIds.size();
        });
      } finally {
        if (adminDS != null) {
          adminDS.disconnect();
        }
      }
    });
  }

  /**
   * Use Case AB are alive A crashes. B crashes. B starts up. It should not wait for A.
   */
  @Test
  public void testDoNotWaitForOldMember() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

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
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);
    putAnEntry(vm0);
    updateTheEntry(vm1);

    // Copy the regions as they are with both members online.
    backupDir(vm0);
    backupDir(vm1);

    // destroy the members
    closeCache(vm0);
    closeCache(vm1);

    // now restore from backup
    restoreBackup(vm0);
    restoreBackup(vm1);

    // This ought to wait for VM1 to come back
    AsyncInvocation createPersistentRegionInVM0 = createPersistentRegionAsync(vm0);
    waitForBlockedInitialization(vm0);
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    createPersistentRegion(vm1);

    createPersistentRegionInVM0.await();

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
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    putAnEntry(vm0);
    closeRegion(vm0);

    // VM 2 should be told about the fact that VM1 has crashed.
    createPersistentRegion(vm2);

    updateTheEntry(vm1);
    closeRegion(vm1);

    closeRegion(vm2);

    // This ought to wait for VM1 to come back
    AsyncInvocation createPersistentRegionInVM0 = createPersistentRegionAsync(vm0);
    waitForBlockedInitialization(vm0);
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    // VM2 has the most recent data, it should start
    createPersistentRegion(vm2);

    // VM0 should be informed that VM2 is older, so it should start
    createPersistentRegionInVM0.await();

    checkForEntry(vm0);
    checkForEntry(vm2);
  }

  /**
   * Tests that a persistent region cannot recover from a non persistent region.
   */
  @Test
  public void testRecoverFromNonPersistentRegion() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    createPersistentRegion(vm0);
    createNonPersistentRegion(vm1);

    putAnEntry(vm0);
    closeRegion(vm0);

    Throwable thrown = catchThrowable(() -> updateTheEntry(vm1));
    assertThat(thrown).hasRootCauseInstanceOf(PersistentReplicatesOfflineException.class);

    // This should initialize from vm1
    createPersistentRegion(vm0);

    checkForRecoveryStat(vm0, true);

    updateTheEntry(vm1);
    checkForEntry(vm0);
    checkForEntry(vm1);
  }

  @Test
  public void testFinishIncompleteInitializationNoSend() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    // Add a hook which will disconnect the DS before sending a prepare message
    vm1.invoke(() -> {
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

        @Override
        public void beforeSendMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof PrepareNewPersistentMemberMessage) {
            DistributionMessageObserver.setInstance(null);
            system = dm.getSystem();
            disconnectFromDS();
          }
        }
      });
    });

    createPersistentRegion(vm0);

    putAnEntry(vm0);
    updateTheEntry(vm0);

    Throwable thrown = catchThrowable(() -> createPersistentRegion(vm1));
    assertThat(thrown).hasRootCauseInstanceOf(DistributedSystemDisconnectedException.class);

    closeRegion(vm0);

    vm1.invoke(() -> {
      await().until(() -> system != null && system.isDisconnected());
    });

    // This wait for VM0 to come back
    AsyncInvocation createPersistentRegionInVM1 = createPersistentRegionAsync(vm1);
    waitForBlockedInitialization(vm1);

    createPersistentRegion(vm0);

    createPersistentRegionInVM1.await();

    checkForEntry(vm1);

    vm0.invoke(() -> {
      InternalRegion region = (InternalRegion) getCache().getRegion(regionName);
      DiskRegion diskRegion = region.getDiskRegion();

      assertThat(diskRegion.getOfflineMembers()).isEmpty();
      assertThat(diskRegion.getOnlineMembers()).hasSize(1);
    });
  }

  /**
   * vm0 and vm1 are peers, each holds a DR. They do put to the same key for different value at the
   * same time. Use DistributionMessageObserver.beforeSendMessage to hold on the distribution
   * message. One of the member will persist the conflict version tag, while another member will
   * persist both of the 2 operations. Overall, their RVV should match after the operations.
   */
  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}({params})")
  public void testPersistConflictOperations(boolean diskSync) throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    vm0.invoke(() -> addSleepBeforeSendAbstractUpdateMessage());
    vm1.invoke(() -> addSleepBeforeSendAbstractUpdateMessage());

    AsyncInvocation createPersistentRegionInVM0 = createPersistentRegionAsync(vm0, diskSync);
    AsyncInvocation createPersistentRegionInVM1 = createPersistentRegionAsync(vm1, diskSync);
    createPersistentRegionInVM0.await();
    createPersistentRegionInVM1.await();

    AsyncInvocation putInVM0 = vm0.invokeAsync(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      region.put("A", "vm0");
    });
    AsyncInvocation putInVM1 = vm1.invokeAsync(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      region.put("A", "vm1");
    });
    putInVM0.await();
    putInVM1.await();

    RegionVersionVector rvv0 = getRVV(vm0);
    RegionVersionVector rvv1 = getRVV(vm1);
    assertSameRVV(rvv1, rvv0);

    Object value0 = getEntry(vm0, "A");
    Object value1 = getEntry(vm1, "A");
    assertThat(value1).isEqualTo(value0);

    closeRegion(vm0);
    closeRegion(vm1);

    // recover
    createPersistentRegionInVM1 = createPersistentRegionAsync(vm1, diskSync);
    createPersistentRegionInVM0 = createPersistentRegionAsync(vm0, diskSync);
    createPersistentRegionInVM1.await();
    createPersistentRegionInVM0.await();

    value0 = getEntry(vm0, "A");
    value1 = getEntry(vm1, "A");
    assertThat(value1).isEqualTo(value0);

    rvv0 = getRVV(vm0);
    rvv1 = getRVV(vm1);
    assertSameRVV(rvv1, rvv0);

    // round 2: async disk write
    vm0.invoke(() -> addSleepBeforeSendAbstractUpdateMessage());
    vm1.invoke(() -> addSleepBeforeSendAbstractUpdateMessage());

    putInVM0 = vm0.invokeAsync(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      for (int i = 0; i < 1000; i++) {
        region.put("A", "vm0-" + i);
      }
    });
    putInVM1 = vm1.invokeAsync(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      for (int i = 0; i < 1000; i++) {
        region.put("A", "vm1-" + i);
      }
    });
    putInVM0.await();
    putInVM1.await();

    rvv0 = getRVV(vm0);
    rvv1 = getRVV(vm1);
    assertSameRVV(rvv1, rvv0);

    value0 = getEntry(vm0, "A");
    value1 = getEntry(vm1, "A");
    assertThat(value1).isEqualTo(value0);

    closeCache(vm0);
    closeCache(vm1);

    // recover again
    createPersistentRegionInVM1 = createPersistentRegionAsync(vm1, diskSync);
    createPersistentRegionInVM0 = createPersistentRegionAsync(vm0, diskSync);
    createPersistentRegionInVM1.await();
    createPersistentRegionInVM0.await();

    value0 = getEntry(vm0, "A");
    value1 = getEntry(vm1, "A");
    assertThat(value1).isEqualTo(value0);

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
  public void testTransmitCrashedMembersWithNonPersistentRegion() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    createPersistentRegion(vm0);
    createNonPersistentRegion(vm1);

    putAnEntry(vm0);

    closeRegion(vm0);

    // VM 2 should not do a GII from vm1, it should wait for vm0
    AsyncInvocation createPersistentRegionInVM2 = createPersistentRegionWithWait(vm2);

    createPersistentRegion(vm0);

    createPersistentRegionInVM2.await();

    closeRegion(vm0);

    updateTheEntry(vm1);

    closeRegion(vm1);
    closeRegion(vm2);

    // VM2 has the most recent data, it should start
    createPersistentRegion(vm2);

    // VM0 should be informed that it has older data than VM2, so it should initialize from vm2
    createPersistentRegion(vm0);

    checkForEntry(vm0);
    checkForEntry(vm2);
  }

  @Test
  public void testSplitBrain() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    createPersistentRegion(vm0);
    putAnEntry(vm0);
    closeRegion(vm0);

    createPersistentRegion(vm1);
    updateTheEntry(vm1);
    closeRegion(vm1);

    // VM0 doesn't know that VM1 ever existed
    // so it will start up.
    createPersistentRegion(vm0);

    try (IgnoredException e = addIgnoredException(ConflictingPersistentDataException.class)) {
      // VM1 should not start up, because we should detect that vm1
      // was never in the same distributed system as vm0
      Throwable thrown = catchThrowable(() -> createPersistentRegion(vm1));
      assertThat(thrown).hasRootCauseInstanceOf(ConflictingPersistentDataException.class);
    }
  }

  /**
   * Test to make sure that if if a member crashes while a GII is in progress, we wait for the
   * member to come back for starting.
   */
  @Test
  public void testCrashDuringGII() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    putAnEntry(vm0);
    closeRegion(vm0);

    updateTheEntry(vm1);
    closeRegion(vm1);

    // This ought to wait for VM1 to come back
    AsyncInvocation createPersistentRegionInVM0 = createPersistentRegionAsync(vm0);
    waitForBlockedInitialization(vm0);
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    // Add a hook which will disconnect from the system when the initial image message shows up.
    vm1.invoke(() -> {
      SAW_REQUEST_IMAGE_MESSAGE.set(false);

      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof RequestImageMessage) {
            DistributionMessageObserver.setInstance(null);
            system = dm.getSystem();
            disconnectFromDS();
            synchronized (SAW_REQUEST_IMAGE_MESSAGE) {
              SAW_REQUEST_IMAGE_MESSAGE.set(true);
              SAW_REQUEST_IMAGE_MESSAGE.notifyAll();
            }
          }
        }
      });
    });

    createPersistentRegion(vm1);

    vm1.invoke(() -> {
      synchronized (SAW_REQUEST_IMAGE_MESSAGE) {
        try {
          while (!SAW_REQUEST_IMAGE_MESSAGE.get()) {
            SAW_REQUEST_IMAGE_MESSAGE.wait();
          }
        } catch (InterruptedException e) {
          errorCollector.addError(e);
        }
      }
    });

    waitForBlockedInitialization(vm0);
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    vm1.invoke(() -> {
      await().until(() -> system != null && system.isDisconnected());
    });

    // Now create the region again. The initialization should
    // work (the observer was cleared when we disconnected from the DS.
    createPersistentRegion(vm1);

    createPersistentRegionInVM0.await();

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
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    createPersistentRegion(vm0);

    // Add a hook which will disconnect from the distributed
    // system when the initial image message shows up.
    vm1.invoke(() -> {
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof DestroyRegionMessage) {
            createPersistentRegionAsync(vm2);
            try {
              Thread.sleep(10_000);
            } catch (InterruptedException e) {
              errorCollector.addError(e);
            } finally {
              DistributionMessageObserver.setInstance(null);
            }
          }
        }
      });
    });

    createPersistentRegion(vm1);

    vm0.invoke(() -> {
      getCache().getRegion(regionName).destroyRegion();
    });


    vm1.invoke(() -> {
      assertThat(getCache().getRegion(regionName)).isNull();
    });

    vm2.invoke(() -> {
      Cache cache = getCache();
      await().until(() -> cache.getRegion(regionName) != null);
    });

    vm2.invoke(() -> {
      DistributedRegion region = (DistributedRegion) getCache().getRegion(regionName);
      PersistenceAdvisor persistAdvisor = region.getPersistenceAdvisor();
      assertThat(persistAdvisor.getMembershipView().getOfflineMembers()).isEmpty();
    });
  }

  @Test
  public void testCrashDuringPreparePersistentId() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    addIgnoredException(DistributedSystemDisconnectedException.class);
    addIgnoredException(CacheClosedException.class);

    // Add a hook which will disconnect from the distributed
    // system when the initial image message shows up.
    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof PrepareNewPersistentMemberMessage) {
            DistributionMessageObserver.setInstance(null);
            system = dm.getSystem();
            disconnectFromDS();
          }
        }
      });
    });

    createPersistentRegion(vm0);
    putAnEntry(vm0);
    updateTheEntry(vm0);

    AsyncInvocation createPersistentRegionInVM1 = createPersistentRegionAsync(vm1);
    waitForBlockedInitialization(vm1);
    closeCache(vm1);

    Throwable thrown = catchThrowable(() -> createPersistentRegionInVM1.await());
    assertThat(thrown).hasRootCauseInstanceOf(CacheClosedException.class);

    vm0.invoke(() -> {
      await().until(() -> system != null && system.isDisconnected());
    });

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    checkForEntry(vm0);
    checkForEntry(vm1);
  }

  @Test
  public void testSplitBrainWithNonPersistentRegion() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    createPersistentRegion(vm1);
    putAnEntry(vm1);
    updateTheEntry(vm1);
    closeRegion(vm1);

    createNonPersistentRegion(vm0);

    try (IgnoredException e = addIgnoredException(IllegalStateException.class)) {
      Throwable thrown = catchThrowable(() -> createPersistentRegion(vm1));
      assertThat(thrown).hasRootCauseInstanceOf(IllegalStateException.class);
    }

    closeRegion(vm0);

    createPersistentRegion(vm1);
    checkForEntry(vm1);
    checkForRecoveryStat(vm1, true);
  }

  @Test
  public void testMissingEntryOnDisk() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    // Add a hook which will perform some updates while the region is initializing
    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof RequestImageMessage) {
            Region<String, String> region = getCache().getRegion(regionName);

            errorCollector.checkSucceeds(() -> assertThat(region).isNotNull());

            region.put("A", "B");
            region.destroy("A");
            region.put("A", "C");
          }
        }
      });
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
   */
  @Test
  public void testCompactFromAdmin() {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    createPersistentRegionWithoutCompaction(vm0);
    createPersistentRegionWithoutCompaction(vm1);

    vm1.invoke(() -> {
      Region region = getCache().getRegion(regionName);

      for (int i = 0; i < 1024; i++) {
        region.put(i, new byte[1024]);
      }

      for (int i = 2; i < 1024; i++) {
        assertThat(region.destroy(i) != null).isTrue();
      }

      DiskStore store = cache.findDiskStore(regionName);
      store.forceRoll();
    });

    vm2.invoke(() -> {
      getCache();
      AdminDistributedSystem adminDS = null;
      try {
        DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
        adminDS = getDistributedSystem(config);
        adminDS.connect();

        Map<DistributedMember, Set<PersistentID>> missingIds = adminDS.compactAllDiskStores();
        assertThat(missingIds).hasSize(2);

        for (Set<PersistentID> value : missingIds.values()) {
          assertThat(value).hasSize(1);
        }
      } finally {
        if (adminDS != null) {
          adminDS.disconnect();
        }
      }
    });

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        DiskStore diskStore = getCache().findDiskStore(regionName);
        assertThat(diskStore.forceCompaction()).isFalse();
      });
    }
  }

  @Test
  public void testCloseDuringRegionOperation() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    createPersistentRegion(vm0);
    createPersistentRegion(vm1);

    // Try to make sure there are some operations in flight while closing the cache

    AsyncInvocation<Integer> createDataInVM0 = vm0.invokeAsync(() -> {
      Region<Integer, Integer> region = getCache().getRegion(regionName);
      int i = 0;
      while (true) {
        try {
          region.put(0, i);
          i++;
        } catch (CacheClosedException | RegionDestroyedException e) {
          break;
        }
      }
      return i - 1;
    });

    AsyncInvocation<Integer> createDataInVM1 = vm1.invokeAsync(() -> {
      Region<Integer, Integer> region = getCache().getRegion(regionName);
      int i = 0;
      while (true) {
        try {
          region.put(1, i);
          i++;
        } catch (CacheClosedException | RegionDestroyedException e) {
          break;
        }
      }
      return i - 1;
    });

    Thread.sleep(500);

    AsyncInvocation closeCacheInVM0 = closeCacheAsync(vm0);
    AsyncInvocation closeCacheInVM1 = closeCacheAsync(vm1);

    // wait for the close to finish
    closeCacheInVM0.await();
    closeCacheInVM1.await();

    int lastSuccessfulCreateInVM0 = createDataInVM0.get();
    int lastSuccessfulCreateInVM1 = createDataInVM1.get();

    AsyncInvocation createPersistentRegionInVM0 = createPersistentRegionAsync(vm0);
    AsyncInvocation createPersistentRegionInVM1 = createPersistentRegionAsync(vm1);

    createPersistentRegionInVM0.await();
    createPersistentRegionInVM1.await();

    checkConcurrentCloseValue(vm0, vm1, 0, lastSuccessfulCreateInVM0);
    checkConcurrentCloseValue(vm0, vm1, 1, lastSuccessfulCreateInVM1);
  }

  @Ignore("TODO: Disabled due to bug #52240")
  @Test
  public void testCloseDuringRegionOperationWithTX() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);

    createInternalPersistentRegionAsync(vm0).await();
    createInternalPersistentRegionAsync(vm1).await();
    createInternalPersistentRegionAsync(vm2).await();

    AsyncInvocation<Integer> createDataInVM0 = createDataAsyncTX(vm0, 0);
    AsyncInvocation<Integer> createDataInVM1 = createDataAsyncTX(vm0, 1);
    AsyncInvocation<Integer> createDataInVM2 = createDataAsyncTX(vm0, 2);

    Thread.sleep(500);

    AsyncInvocation closeCacheInVM0 = closeCacheAsync(vm0);
    AsyncInvocation closeCacheInVM1 = closeCacheAsync(vm1);
    AsyncInvocation closeCacheInVM2 = closeCacheAsync(vm2);

    // wait for the close to finish
    closeCacheInVM0.await();
    closeCacheInVM1.await();
    closeCacheInVM2.await();

    int lastPutInVM0 = createDataInVM0.getResult();
    int lastPutInVM1 = createDataInVM1.getResult();
    int lastPutInVM2 = createDataInVM2.getResult();

    AsyncInvocation createPersistentRegionInVM0 = createInternalPersistentRegionAsync(vm0);
    AsyncInvocation createPersistentRegionInVM1 = createInternalPersistentRegionAsync(vm1);
    AsyncInvocation createPersistentRegionInVM2 = createInternalPersistentRegionAsync(vm2);

    createPersistentRegionInVM0.await();
    createPersistentRegionInVM1.await();
    createPersistentRegionInVM2.await();

    checkConcurrentCloseValue(vm0, vm1, 0, lastPutInVM0);
    checkConcurrentCloseValue(vm0, vm1, 1, lastPutInVM1);
    checkConcurrentCloseValue(vm0, vm1, 2, lastPutInVM2);
  }

  /**
   * Tests to make sure that after we get a conflicting persistent data exception, we can still
   * recover.
   */
  @Test
  public void testRecoverAfterConflict() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);

    createPersistentRegion(vm0);
    putAnEntry(vm0);
    closeCache(vm0);

    createPersistentRegion(vm1);
    putAnEntry(vm1);

    try (IgnoredException e = addIgnoredException(ConflictingPersistentDataException.class)) {
      // this should cause a conflict
      Throwable thrown = catchThrowable(() -> createPersistentRegion(vm0));
      assertThat(thrown).hasRootCauseInstanceOf(ConflictingPersistentDataException.class);
    }

    closeCache(vm1);

    // This should work now
    createPersistentRegion(vm0);

    updateTheEntry(vm0);

    // Now make sure vm1 gets a conflict
    try (IgnoredException e = addIgnoredException(ConflictingPersistentDataException.class)) {
      // this should cause a conflict
      Throwable thrown = catchThrowable(() -> createPersistentRegion(vm1));
      assertThat(thrown).hasRootCauseInstanceOf(ConflictingPersistentDataException.class);
    }
  }

  private Object getEntry(VM vm, String key) {
    return vm.invoke(() -> getCache().getRegion(regionName).get(key));
  }

  private RegionVersionVector getRVV(VM vm) throws IOException, ClassNotFoundException {
    byte[] result = vm.invoke(() -> {
      InternalRegion region = (InternalRegion) getCache().getRegion(regionName);

      RegionVersionVector regionVersionVector = region.getVersionVector();
      regionVersionVector = regionVersionVector.getCloneForTransmission();
      HeapDataOutputStream outputStream = new HeapDataOutputStream(2048);

      // Using gemfire serialization because RegionVersionVector is not java serializable
      DataSerializer.writeObject(regionVersionVector, outputStream);
      return outputStream.toByteArray();
    });

    ByteArrayInputStream inputStream = new ByteArrayInputStream(result);
    return DataSerializer.readObject(new DataInputStream(inputStream));
  }

  private SerializableRunnable addSleepBeforeSendAbstractUpdateMessage() {
    return new SerializableRunnable() {
      @Override
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeSendMessage(ClusterDistributionManager dm,
              DistributionMessage message) {
            if (message instanceof AbstractUpdateMessage) {
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e) {
                errorCollector.addError(e);
              }
            }
          }

          @Override
          public void afterProcessMessage(ClusterDistributionManager dm,
              DistributionMessage message) {
            if (message instanceof AbstractUpdateMessage) {
              DistributionMessageObserver.setInstance(null);
            }
          }
        });
      }
    };
  }

  private AsyncInvocation createPersistentRegionAsync(VM vm, boolean diskSynchronous) {
    return vm.invokeAsync(() -> {
      getCache();

      File dir = getDiskDirForVM(vm);
      dir.mkdirs();

      DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
      diskStoreFactory.setDiskDirs(new File[] {dir});
      diskStoreFactory.setMaxOplogSize(1);

      DiskStore diskStore = diskStoreFactory.create(regionName);

      RegionFactory regionFactory = new RegionFactory();
      regionFactory.setDiskStoreName(diskStore.getName());
      regionFactory.setDiskSynchronous(diskSynchronous);
      regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      regionFactory.setScope(Scope.DISTRIBUTED_ACK);

      regionFactory.create(regionName);
    });
  }

  private void assertSameRVV(RegionVersionVector expectedRVV, RegionVersionVector actualRVV) {
    assertThat(expectedRVV.sameAs(actualRVV))
        .as("Expected " + expectedRVV + " but was " + actualRVV)
        .isTrue();
  }

  private AsyncInvocation<Integer> createDataAsyncTX(VM vm, int member) {
    return vm.invokeAsync(() -> {
      Region<Integer, Integer> region = getCache().getRegion(regionName);
      CacheTransactionManager txManager = getCache().getCacheTransactionManager();
      int i = 0;
      while (true) {
        try {
          txManager.begin();
          region.put(member, i);
          txManager.commit();
          i++;
        } catch (CacheClosedException | LockServiceDestroyedException
            | RegionDestroyedException e) {
          break;
        } catch (IllegalArgumentException e) {
          if (!e.getMessage().contains("Invalid txLockId")) {
            throw e;
          }
          break;
        }
      }
      return i - 1;
    });
  }

  private void checkConcurrentCloseValue(VM vm0, VM vm1, int key, int lastSuccessfulInt) {
    int valueInVM0 = vm0.invoke(() -> doRegionGet(key));
    int valueInVM1 = vm1.invoke(() -> doRegionGet(key));

    assertThat(valueInVM1).isEqualTo(valueInVM0);
    assertThat(valueInVM0 == lastSuccessfulInt || valueInVM0 == lastSuccessfulInt + 1)
        .as("value = " + valueInVM0 + ", lastSuccessfulInt=" + lastSuccessfulInt)
        .isTrue();
  }

  private int doRegionGet(int key) {
    Region<Integer, Integer> region = getCache().getRegion(regionName);
    return region.get(key);
  }

  private void checkForEntry(VM vm) {
    vm.invoke(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      assertThat(region.get("A")).isEqualTo("C");
    });
  }

  private void updateTheEntry(VM vm) {
    updateTheEntry(vm, "C");
  }

  private void updateTheEntry(VM vm, String value) {
    vm.invoke(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      region.put("A", value);
    });
  }

  private void putAnEntry(VM vm) {
    vm.invoke(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      region.put("A", "B");
    });
  }

  private void checkForRecoveryStat(VM vm, boolean localRecovery) {
    vm.invoke(() -> {
      InternalRegion region = (InternalRegion) getCache().getRegion(regionName);
      DiskRegionStats diskRegionStats = region.getDiskRegion().getStats();

      if (localRecovery) {
        assertThat(diskRegionStats.getLocalInitializations()).isEqualTo(1);
        assertThat(diskRegionStats.getRemoteInitializations()).isEqualTo(0);
      } else {
        assertThat(diskRegionStats.getLocalInitializations()).isEqualTo(0);
        assertThat(diskRegionStats.getRemoteInitializations()).isEqualTo(1);
      }
    });
  }

  private AsyncInvocation createInternalPersistentRegionAsync(VM vm) {
    return vm.invokeAsync(() -> {
      File dir = getDiskDirForVM(vm);
      dir.mkdirs();

      DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
      diskStoreFactory.setDiskDirs(new File[] {dir});
      diskStoreFactory.setMaxOplogSize(1);

      DiskStore diskStore = diskStoreFactory.create(regionName);

      InternalRegionArguments internalRegionArguments = new InternalRegionArguments();
      internalRegionArguments.setIsUsedForMetaRegion(true);
      internalRegionArguments.setMetaRegionWithTransactions(true);

      AttributesFactory attributesFactory = new AttributesFactory();
      attributesFactory.setDiskStoreName(diskStore.getName());
      attributesFactory.setDiskSynchronous(true);
      attributesFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      attributesFactory.setScope(Scope.DISTRIBUTED_ACK);

      getCache().createVMRegion(regionName, attributesFactory.create(), internalRegionArguments);
    });
  }
}
