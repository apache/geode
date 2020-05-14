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

import static java.lang.String.valueOf;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.management.MBeanServerInvocationHandler.newProxyInstance;
import static org.apache.commons.io.FileUtils.copyDirectory;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.ACK_WAIT_THRESHOLD;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ObjectName;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.persistence.ConflictingPersistentDataException;
import org.apache.geode.cache.persistence.PersistentReplicatesOfflineException;
import org.apache.geode.cache.persistence.RevokedPersistentDataException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
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
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.management.DiskStoreMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.PersistentMemberDetails;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedExecutorServiceRule;
import org.apache.geode.test.junit.categories.PersistenceTest;
import org.apache.geode.test.junit.rules.serializable.SerializableErrorCollector;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

/**
 * This is a test of how persistent distributed regions recover. This test makes sure that when
 * multiple VMs are persisting the same region, they recover with the latest data during recovery.
 */
@Category(PersistenceTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class PersistentRecoveryOrderDUnitTest extends CacheTestCase {

  private static final long TIMEOUT_MILLIS = getTimeout().toMillis();
  private static final AtomicBoolean SAW_REQUEST_IMAGE_MESSAGE = new AtomicBoolean();
  private static final AtomicReference<CountDownLatch> LATCH = new AtomicReference<>();
  private static final AtomicReference<CountDownLatch> SLEEP = new AtomicReference<>();
  private static final AtomicInteger COUNT = new AtomicInteger();

  private static volatile InternalDistributedSystem system;

  private final Map<Integer, File> diskDirs = new HashMap<>();

  private String regionName;
  private File rootDir;
  private int jmxManagerPort;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedDiskDirRule diskDirRule = new DistributedDiskDirRule();

  @Rule
  public DistributedExecutorServiceRule executorServiceRule = new DistributedExecutorServiceRule();

  @Rule
  public SerializableErrorCollector errorCollector = new SerializableErrorCollector();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() throws Exception {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    rootDir = temporaryFolder.newFolder("rootDir-" + getName()).getAbsoluteFile();
    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      diskDirs.put(vm.getId(), new File(rootDir, "vm-" + vm.getId()));
    }

    regionName = getUniqueName() + "Region";
    jmxManagerPort = getRandomAvailablePort(SOCKET);
  }

  @After
  public void tearDown() {
    for (VM vm : toArray(getAllVMs(), getController())) {
      vm.invoke(() -> {
        DistributionMessageObserver.setInstance(null);
        CountDownLatch latch = SLEEP.get();
        while (latch != null && latch.getCount() > 0) {
          latch.countDown();
        }
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
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    vm0.invoke(() -> {
      putEntry("A", "B");
      getCache().getRegion(regionName).close();
    });

    vm1.invoke(() -> {
      updateEntry("A", "C");
      getCache().getRegion(regionName).close();
    });

    // This ought to wait for VM1 to come back
    AsyncInvocation<Void> createPersistentRegionInVM0 = vm0.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });
    vm0.invoke(() -> waitForBlockedInitialization());
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    vm1.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));

    createPersistentRegionInVM0.await();

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> validateEntry("A", "C"));
    }

    vm1.invoke(() -> validateDiskRegionInitializationStats(true));
    vm0.invoke(() -> validateDiskRegionInitializationStats(false));
  }

  /**
   * Tests to make sure that we stop waiting for a member that we revoke.
   */
  @Test
  public void testRevokeAMember() throws Exception {
    vm2.invoke(() -> {
      Properties props = getDistributedSystemProperties();
      props.setProperty(JMX_MANAGER, "true");
      props.setProperty(JMX_MANAGER_PORT, valueOf(jmxManagerPort));
      props.setProperty(JMX_MANAGER_START, "true");
      props.setProperty(HTTP_SERVICE_PORT, "0");
      getCache(props);
    });

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    vm0.invoke(() -> {
      putEntry("A", "B");

      PersistentMemberManager manager = getCache().getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> waitingRegions = manager.getWaitingRegions();
      assertThat(waitingRegions).isEmpty();

      getCache().getRegion(regionName).close();
    });

    vm1.invoke(() -> {
      updateEntry("A", "C");
      getCache().close();
    });

    // This ought to wait for VM1 to come back
    AsyncInvocation<Void> createPersistentRegionInVM0 = vm0.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });
    vm0.invoke(() -> waitForBlockedInitialization());
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    vm2.invoke(() -> {
      ManagementService managementService = ManagementService.getManagementService(getCache());

      await().untilAsserted(() -> {
        assertThat(managementService.getDistributedSystemMXBean()).isNotNull();
      });

      DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();

      await().until(() -> dsMXBean.listMissingDiskStores().length > 0);

      PersistentMemberDetails[] persistentMemberDetails = dsMXBean.listMissingDiskStores();
      assertThat(persistentMemberDetails).hasSize(1);

      String missingDiskStoreId = persistentMemberDetails[0].getDiskStoreId();
      boolean revoked = dsMXBean.revokeMissingDiskStores(missingDiskStoreId);
      assertThat(revoked).isTrue();
    });

    createPersistentRegionInVM0.await();

    vm0.invoke(() -> {
      validateDiskRegionInitializationStats(true);

      // Check to make sure we recovered the old value of the entry.
      Region<String, String> region = getCache().getRegion(regionName);
      assertThat(region.get("A")).isEqualTo("B");
    });

    // Now, we should not be able to create a region in vm1, because the this member was revoked
    vm1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(RevokedPersistentDataException.class)) {
        Throwable thrown = catchThrowable(() -> {
          createReplicateRegion(regionName, getDiskDirs(getVMId()));
        });
        assertThat(thrown).isInstanceOf(RevokedPersistentDataException.class);
      }
    });

    vm1.invoke(() -> getCache().close());

    // Restart vm0
    vm0.invoke(() -> {
      getCache().close();
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });
  }

  /**
   * Tests to make sure that we can revoke a member before initialization, and that member will stay
   * revoked
   */
  @Test
  public void testRevokeAHostBeforeInitialization() throws Exception {
    vm2.invoke(() -> {
      Properties props = getDistributedSystemProperties();
      props.setProperty(JMX_MANAGER, "true");
      props.setProperty(JMX_MANAGER_PORT, "1099");
      props.setProperty(JMX_MANAGER_START, "true");
      props.setProperty(HTTP_SERVICE_PORT, "0");
      getCache(props);
    });

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    vm0.invoke(() -> {
      putEntry("A", "B");

      PersistentMemberManager manager = getCache().getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> waitingRegions = manager.getWaitingRegions();
      assertThat(waitingRegions).isEmpty();

      getCache().getRegion(regionName).close();
    });

    vm1.invoke(() -> {
      updateEntry("A", "C");
      getCache().close();
    });

    AsyncInvocation createRegionInVM0 = vm0.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });

    vm2.invoke(() -> {
      ManagementService managementService = ManagementService.getManagementService(getCache());

      await().untilAsserted(() -> {
        assertThat(managementService.getDistributedSystemMXBean()).isNotNull();
      });

      DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();

      await().until(() -> dsMXBean.listMissingDiskStores().length > 0);

      PersistentMemberDetails[] persistentMemberDetails = dsMXBean.listMissingDiskStores();
      assertThat(persistentMemberDetails).hasSize(1);

      String missingDiskStoreId = persistentMemberDetails[0].getDiskStoreId();
      boolean revoked = dsMXBean.revokeMissingDiskStores(missingDiskStoreId);
      assertThat(revoked).isTrue();
    });

    // This shouldn't wait, because we revoked the member
    createRegionInVM0.await();

    vm0.invoke(() -> {
      validateDiskRegionInitializationStats(true);

      // Check to make sure we recovered the old value of the entry.
      Region<String, String> region = getCache().getRegion(regionName);
      assertThat(region.get("A")).isEqualTo("B");
    });

    // Now, we should not be able to create a region
    // in vm1, because the this member was revoked
    vm1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(RevokedPersistentDataException.class)) {
        Throwable thrown = catchThrowable(() -> {
          createReplicateRegion(regionName, getDiskDirs(getVMId()));
        });
        assertThat(thrown).isInstanceOf(RevokedPersistentDataException.class);
      }
    });
  }

  /**
   * Test which members show up in the list of members we're waiting on.
   */
  @Test
  public void testWaitingMemberList() {
    vm3.invoke(() -> {
      Properties props = getDistributedSystemProperties();
      props.setProperty(JMX_MANAGER, "true");
      props.setProperty(JMX_MANAGER_PORT, "1099");
      props.setProperty(JMX_MANAGER_START, "true");
      props.setProperty(HTTP_SERVICE_PORT, "0");
      getCache(props);
    });

    for (VM vm : toArray(vm0, vm1, vm2)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    vm0.invoke(() -> {
      putEntry("A", "B");

      PersistentMemberManager manager = getCache().getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> waitingRegions = manager.getWaitingRegions();
      assertThat(waitingRegions).isEmpty();

      getCache().close();
    });

    vm1.invoke(() -> {
      updateEntry("A", "C");
      getCache().close();
    });

    vm2.invoke(() -> {
      updateEntry("A", "D");
      getCache().close();
    });

    // These ought to wait for VM2 to come back
    AsyncInvocation<Void> createPersistentRegionInVM0 = vm0.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });
    vm0.invoke(() -> waitForBlockedInitialization());
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    AsyncInvocation<Void> createPersistentRegionInVM1 = vm1.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });
    vm1.invoke(() -> waitForBlockedInitialization());
    assertThat(createPersistentRegionInVM1.isAlive()).isTrue();

    vm3.invoke(() -> {
      ManagementService managementService = ManagementService.getManagementService(getCache());

      await().untilAsserted(() -> {
        assertThat(managementService.getDistributedSystemMXBean()).isNotNull();
      });

      DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();

      await().untilAsserted(() -> {
        PersistentMemberDetails[] persistentMemberDetails = dsMXBean.listMissingDiskStores();
        assertThat(persistentMemberDetails).hasSize(1);
      });
    });

    vm1.invoke(() -> {
      getCache().close();
    });

    await().until(() -> !createPersistentRegionInVM1.isAlive());

    // Now we should be missing 2 members
    vm3.invoke(() -> {
      ManagementService managementService = ManagementService.getManagementService(getCache());

      await().untilAsserted(() -> {
        assertThat(managementService.getDistributedSystemMXBean()).isNotNull();
      });

      DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();

      await().untilAsserted(() -> {
        PersistentMemberDetails[] persistentMemberDetails = dsMXBean.listMissingDiskStores();
        assertThat(persistentMemberDetails).hasSize(2);
      });
    });
  }

  /**
   * Use Case AB are alive A crashes. B crashes. B starts up. It should not wait for A.
   */
  @Test
  public void testDoNotWaitForOldMember() {
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    vm0.invoke(() -> {
      putEntry("A", "B");
      getCache().getRegion(regionName).close();
    });

    vm1.invoke(() -> {
      updateEntry("A", "C");
      getCache().getRegion(regionName).close();
    });

    vm1.invoke(() -> {
      // This shouldn't wait for vm0 to come back
      createReplicateRegion(regionName, getDiskDirs(getVMId()));

      validateEntry("A", "C");
      validateDiskRegionInitializationStats(true);
    });
  }

  /**
   * Tests that if two members crash simultaneously, they negotiate which member should initialize
   * with what is on disk and which member should copy data from that member.
   */
  @Test
  public void testSimultaneousCrash() throws Exception {
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    vm0.invoke(() -> putEntry("A", "B"));

    vm1.invoke(() -> updateEntry("A", "C"));

    // Copy the regions as they are with both members online.
    for (VM vm : toArray(vm0, vm1)) {
      backupDir(vm.getId());
    }

    // destroy the members
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> getCache().close());
    }

    // now restore from backup
    for (VM vm : toArray(vm0, vm1)) {
      restoreBackup(vm.getId());
    }

    // This ought to wait for VM1 to come back
    AsyncInvocation<Void> createPersistentRegionInVM0 = vm0.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });
    vm0.invoke(() -> waitForBlockedInitialization());
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    vm1.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));

    createPersistentRegionInVM0.await();

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> validateEntry("A", "C"));
    }
  }

  /**
   * Tests that persistent members pass along the list of crashed members to later persistent
   * members. Eg. AB are running A crashes C is tarted B crashes C crashes AC are started, they
   * should figure out who has the latest data, without needing B.
   */
  @Test
  public void testTransmitCrashedMembers() throws Exception {
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    vm0.invoke(() -> {
      putEntry("A", "B");
      getCache().getRegion(regionName).close();
    });

    // VM 2 should be told about the fact that VM1 has crashed.
    vm2.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));

    vm1.invoke(() -> {
      updateEntry("A", "C");
      getCache().getRegion(regionName).close();
    });

    vm2.invoke(() -> getCache().getRegion(regionName).close());

    // This ought to wait for VM1 to come back
    AsyncInvocation<Void> createPersistentRegionInVM0 = vm0.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });
    vm0.invoke(() -> waitForBlockedInitialization());
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    // VM2 has the most recent data, it should start
    vm2.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));

    // VM0 should be informed that VM2 is older, so it should start
    createPersistentRegionInVM0.await();

    for (VM vm : toArray(vm0, vm2)) {
      vm.invoke(() -> validateEntry("A", "C"));
    }
  }

  /**
   * Tests that a persistent region cannot recover from a non persistent region.
   */
  @Test
  public void testRecoverFromNonPersistentRegion() {
    vm0.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    vm1.invoke(() -> createReplicateRegion(regionName));

    vm0.invoke(() -> {
      putEntry("A", "B");
      getCache().getRegion(regionName).close();
    });

    vm1.invoke(() -> {
      Throwable thrown = catchThrowable(() -> updateEntry("A", "C"));
      assertThat(thrown).isInstanceOf(PersistentReplicatesOfflineException.class);
    });

    // This should initialize from vm1
    vm0.invoke(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
      validateDiskRegionInitializationStats(true);
    });

    vm1.invoke(() -> updateEntry("A", "C"));

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> validateEntry("A", "C"));
    }
  }

  @Test
  public void testFinishIncompleteInitializationNoSend() throws Exception {
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

    vm0.invoke(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
      putEntry("A", "B");
      updateEntry("A", "C");
    });

    vm1.invoke(() -> {
      Throwable thrown = catchThrowable(() -> {
        createReplicateRegion(regionName, getDiskDirs(getVMId()));
      });
      assertThat(thrown).isInstanceOf(DistributedSystemDisconnectedException.class);
    });

    vm0.invoke(() -> getCache().getRegion(regionName).close());

    vm1.invoke(() -> {
      await().until(() -> system != null && system.isDisconnected());
    });

    // This wait for VM0 to come back
    AsyncInvocation<Void> createPersistentRegionInVM1 = vm1.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });
    vm1.invoke(() -> waitForBlockedInitialization());

    vm0.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));

    createPersistentRegionInVM1.await();

    vm1.invoke(() -> validateEntry("A", "C"));

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
  public void testPersistConflictOperations(boolean diskSynchronous) throws Exception {
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> addSleepBeforeSendAbstractUpdateMessage());
    }

    AsyncInvocation<Void> createPersistentRegionInVM0 = vm0.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()), diskSynchronous);
    });
    AsyncInvocation<Void> createPersistentRegionInVM1 = vm1.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()), diskSynchronous);
    });

    createPersistentRegionInVM0.await();
    createPersistentRegionInVM1.await();

    AsyncInvocation<Void> putInVM0 = vm0.invokeAsync(() -> {
      getCache().<String, String>getRegion(regionName).put("A", "vm0");
    });
    AsyncInvocation<Void> putInVM1 = vm1.invokeAsync(() -> {
      getCache().<String, String>getRegion(regionName).put("A", "vm1");
    });

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> SLEEP.get().countDown());
    }

    putInVM0.await();
    putInVM1.await();

    RegionVersionVector rvvInVM0 = toRVV(vm0.invoke(() -> getRVVBytes()));
    RegionVersionVector rvvInVM1 = toRVV(vm1.invoke(() -> getRVVBytes()));
    assertSameRVV(rvvInVM1, rvvInVM0);

    Object valueInVM0 = vm0.invoke(() -> getCache().getRegion(regionName).get("A"));
    Object valueInVM1 = vm1.invoke(() -> getCache().getRegion(regionName).get("A"));
    assertThat(valueInVM1).isEqualTo(valueInVM0);

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> getCache().getRegion(regionName).close());
    }

    // recover
    createPersistentRegionInVM1 = vm1.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()), diskSynchronous);
    });
    createPersistentRegionInVM0 = vm0.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()), diskSynchronous);
    });

    createPersistentRegionInVM1.await();
    createPersistentRegionInVM0.await();

    valueInVM0 = vm0.invoke(() -> getCache().getRegion(regionName).get("A"));
    valueInVM1 = vm1.invoke(() -> getCache().getRegion(regionName).get("A"));
    assertThat(valueInVM1).isEqualTo(valueInVM0);

    rvvInVM0 = toRVV(vm0.invoke(() -> getRVVBytes()));
    rvvInVM1 = toRVV(vm1.invoke(() -> getRVVBytes()));
    assertSameRVV(rvvInVM1, rvvInVM0);

    // round 2: async disk write
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> addSleepBeforeSendAbstractUpdateMessage());
    }

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

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> SLEEP.get().countDown());
    }

    putInVM0.await();
    putInVM1.await();

    rvvInVM0 = toRVV(vm0.invoke(() -> getRVVBytes()));
    rvvInVM1 = toRVV(vm1.invoke(() -> getRVVBytes()));
    assertSameRVV(rvvInVM1, rvvInVM0);

    valueInVM0 = vm0.invoke(() -> getCache().getRegion(regionName).get("A"));
    valueInVM1 = vm1.invoke(() -> getCache().getRegion(regionName).get("A"));
    assertThat(valueInVM1).isEqualTo(valueInVM0);

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> getCache().close());
    }

    // recover again
    createPersistentRegionInVM1 = vm1.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()), diskSynchronous);
    });
    createPersistentRegionInVM0 = vm0.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()), diskSynchronous);
    });

    createPersistentRegionInVM1.await();
    createPersistentRegionInVM0.await();

    valueInVM0 = vm0.invoke(() -> getCache().getRegion(regionName).get("A"));
    valueInVM1 = vm1.invoke(() -> getCache().getRegion(regionName).get("A"));
    assertThat(valueInVM1).isEqualTo(valueInVM0);

    rvvInVM0 = toRVV(vm0.invoke(() -> getRVVBytes()));
    rvvInVM1 = toRVV(vm1.invoke(() -> getRVVBytes()));
    assertSameRVV(rvvInVM1, rvvInVM0);
  }

  /**
   * Tests that even non persistent regions can transmit the list of crashed members to other
   * persistent regions, So that the persistent regions can negotiate who has the latest data during
   * recovery.
   */
  @Test
  public void testTransmitCrashedMembersWithNonPersistentRegion() throws Exception {
    vm0.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    vm1.invoke(() -> createReplicateRegion(regionName));

    vm0.invoke(() -> {
      putEntry("A", "B");
      getCache().getRegion(regionName).close();
    });

    // VM 2 should not do a GII from vm1, it should wait for vm0
    AsyncInvocation<Void> createPersistentRegionInVM2 = vm2.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });
    vm2.invoke(() -> waitForBlockedInitialization());
    assertThat(createPersistentRegionInVM2.isAlive()).isTrue();

    vm0.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));

    createPersistentRegionInVM2.await();

    vm0.invoke(() -> getCache().getRegion(regionName).close());

    vm1.invoke(() -> updateEntry("A", "C"));

    for (VM vm : toArray(vm1, vm2)) {
      vm.invoke(() -> getCache().getRegion(regionName).close());
    }

    // VM2 has the most recent data, it should start
    // VM0 should be informed that it has older data than VM2, so it should initialize from vm2
    for (VM vm : toArray(vm2, vm0)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    for (VM vm : toArray(vm0, vm2)) {
      vm.invoke(() -> validateEntry("A", "C"));
    }
  }

  @Test
  public void testSplitBrain() {
    vm0.invoke(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
      putEntry("A", "B");
      getCache().getRegion(regionName).close();
    });

    vm1.invoke(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
      updateEntry("A", "C");
      getCache().getRegion(regionName).close();
    });

    // VM0 doesn't know that VM1 ever existed so it will start up.
    vm0.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));

    vm1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(ConflictingPersistentDataException.class)) {
        // VM1 should not start up, because we should detect that vm1
        // was never in the same distributed system as vm0
        Throwable thrown = catchThrowable(() -> {
          createReplicateRegion(regionName, getDiskDirs(getVMId()));
        });
        assertThat(thrown).isInstanceOf(ConflictingPersistentDataException.class);
      }
    });
  }

  /**
   * Test to make sure that if if a member crashes while a GII is in progress, we wait for the
   * member to come back for starting.
   */
  @Test
  public void testCrashDuringGII() throws Exception {
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    vm0.invoke(() -> {
      putEntry("A", "B");
      getCache().getRegion(regionName).close();
    });

    vm1.invoke(() -> {
      updateEntry("A", "C");
      getCache().getRegion(regionName).close();
    });

    // This ought to wait for VM1 to come back
    AsyncInvocation<Void> createPersistentRegionInVM0 = vm0.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });
    vm0.invoke(() -> waitForBlockedInitialization());
    assertThat(createPersistentRegionInVM0.isAlive()).isTrue();

    vm1.invoke(() -> {
      // Add a hook which will disconnect from the system when the initial image message shows up.
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

      createReplicateRegion(regionName, getDiskDirs(getVMId()));

      synchronized (SAW_REQUEST_IMAGE_MESSAGE) {
        try {
          while (!SAW_REQUEST_IMAGE_MESSAGE.get()) {
            SAW_REQUEST_IMAGE_MESSAGE.wait(TIMEOUT_MILLIS);
          }
        } catch (InterruptedException e) {
          errorCollector.addError(e);
        }
      }

      await().until(() -> system != null && system.isDisconnected());

      // Now create the region again. The initialization should
      // work (the observer was cleared when we disconnected from the DS.
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });


    createPersistentRegionInVM0.await();

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> validateEntry("A", "C"));
    }

    vm1.invoke(() -> validateDiskRegionInitializationStats(true));
    vm0.invoke(() -> validateDiskRegionInitializationStats(false));
  }

  /**
   * Test to make sure we don't leak any persistent ids if a member does GII while a distributed
   * destroy is in progress
   */
  @Test
  public void testGIIDuringDestroy() throws Exception {
    vm0.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));

    vm2.invoke(() -> LATCH.set(new CountDownLatch(1)));

    AsyncInvocation<Void> createPersistentRegionAsyncOnVM2 = vm2.invokeAsync(() -> {
      LATCH.get().await(TIMEOUT_MILLIS, MILLISECONDS);
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });

    // Add a hook which will disconnect from the distributed
    // system when the initial image message shows up.
    vm1.invoke(() -> {
      SLEEP.set(new CountDownLatch(1));
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof DestroyRegionMessage) {
            vm2.invoke(() -> LATCH.get().countDown());
            try {
              SLEEP.get().await(TIMEOUT_MILLIS, MILLISECONDS);
            } catch (InterruptedException e) {
              errorCollector.addError(e);
            } finally {
              DistributionMessageObserver.setInstance(null);
            }
          }
        }
      });
    });

    vm1.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));

    AsyncInvocation<Void> destroyRegionInVM0 = vm0.invokeAsync(() -> {
      getCache().getRegion(regionName).destroyRegion();
    });

    vm1.invoke(() -> {
      SLEEP.get().countDown();
      await().ignoreException(RegionDestroyedException.class).untilAsserted(() -> {
        assertThat(getCache().getRegion(regionName)).isNull();
      });
    });

    createPersistentRegionAsyncOnVM2.await();
    destroyRegionInVM0.await();

    vm2.invoke(() -> {
      DistributedRegion region = (DistributedRegion) getCache().getRegion(regionName);
      PersistenceAdvisor persistAdvisor = region.getPersistenceAdvisor();
      assertThat(persistAdvisor.getMembershipView().getOfflineMembers()).isEmpty();
    });
  }

  @Test
  public void testCrashDuringPreparePersistentId() throws Exception {
    addIgnoredException(CacheClosedException.class);
    addIgnoredException(DistributedSystemDisconnectedException.class);

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

      createReplicateRegion(regionName, getDiskDirs(getVMId()));
      putEntry("A", "B");
      updateEntry("A", "C");
    });

    AsyncInvocation<Void> createPersistentRegionInVM1 = vm1.invokeAsync(() -> {
      Throwable thrown = catchThrowable(() -> {
        createReplicateRegion(regionName, getDiskDirs(getVMId()));
      });
      assertThat(thrown).isInstanceOf(CacheClosedException.class);
    });

    vm1.invoke(() -> {
      waitForBlockedInitialization();
      getCache().close();
    });

    createPersistentRegionInVM1.await();

    vm0.invoke(() -> {
      await().until(() -> system != null && system.isDisconnected());
    });

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> validateEntry("A", "C"));
    }
  }

  @Test
  public void testSplitBrainWithNonPersistentRegion() {
    vm1.invoke(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
      putEntry("A", "B");
      updateEntry("A", "C");
      getCache().getRegion(regionName).close();
    });

    vm0.invoke(() -> createReplicateRegion(regionName));

    vm1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(IllegalStateException.class)) {
        Throwable thrown = catchThrowable(() -> {
          createReplicateRegion(regionName, getDiskDirs(getVMId()));
        });
        assertThat(thrown).isInstanceOf(IllegalStateException.class);
      }
    });

    vm0.invoke(() -> getCache().getRegion(regionName).close());

    vm1.invoke(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));

      validateEntry("A", "C");
      validateDiskRegionInitializationStats(true);
    });
  }

  @Test
  public void testMissingEntryOnDisk() {
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

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    vm1.invoke(() -> validateEntry("A", "C"));

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> getCache().getRegion(regionName).close());
    }

    vm1.invoke(() -> {
      // This should work
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
      validateEntry("A", "C");
    });
  }

  /**
   * Tests to make sure that we stop waiting for a member that we revoke.
   */
  @Test
  public void testCompactFromAdmin() {
    vm2.invoke(() -> {
      Properties props = getDistributedSystemProperties();
      props.setProperty(JMX_MANAGER, "true");
      props.setProperty(JMX_MANAGER_PORT, valueOf(jmxManagerPort));
      props.setProperty(JMX_MANAGER_START, "true");
      props.setProperty(HTTP_SERVICE_PORT, "0");
      getCache(props);
    });

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createReplicateRegionWithoutCompaction(regionName, getDiskDirs(getVMId())));
    }

    vm1.invoke(() -> {
      Region<Integer, byte[]> region = getCache().getRegion(regionName);
      for (int i = 0; i < 1024; i++) {
        region.put(i, new byte[1024]);
      }
      for (int i = 2; i < 1024; i++) {
        assertThat(region.destroy(i)).isNotNull();
      }
      DiskStore diskStore = getCache().findDiskStore(regionName);
      diskStore.forceRoll();
    });

    vm2.invoke(() -> {
      // GemFire:service=DiskStore,name={0},type=Member,member={1}
      ObjectName pattern = new ObjectName("GemFire:service=DiskStore,*");

      await().untilAsserted(() -> {
        Set<ObjectName> mbeanNames = getPlatformMBeanServer().queryNames(pattern, null);

        assertThat(mbeanNames).hasSize(2);
      });

      Set<ObjectName> mbeanNames = getPlatformMBeanServer().queryNames(pattern, null);
      for (ObjectName objectName : mbeanNames) {
        DiskStoreMXBean diskStoreMXBean =
            newProxyInstance(getPlatformMBeanServer(), objectName, DiskStoreMXBean.class, false);
        assertThat(diskStoreMXBean.forceCompaction()).isTrue();
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
    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> createReplicateRegion(regionName, getDiskDirs(getVMId())));
    }

    // Try to make sure there are some operations in flight while closing the cache

    AsyncInvocation<Integer> createDataInVM0 = vm0.invokeAsync(() -> {
      Region<Integer, Integer> region = getCache().getRegion(regionName);
      COUNT.set(0);
      while (true) {
        try {
          region.put(0, COUNT.incrementAndGet());
        } catch (CacheClosedException | RegionDestroyedException e) {
          break;
        }
      }
      return COUNT.get();
    });

    AsyncInvocation<Integer> createDataInVM1 = vm1.invokeAsync(() -> {
      Region<Integer, Integer> region = getCache().getRegion(regionName);
      COUNT.set(0);
      while (true) {
        try {
          region.put(1, COUNT.incrementAndGet());
        } catch (CacheClosedException | RegionDestroyedException e) {
          break;
        }
      }
      return COUNT.get();
    });

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        await().until(() -> COUNT.get() > 1);
      });
    }

    AsyncInvocation<Void> closeCacheInVM0 = vm0.invokeAsync(() -> getCache().close());
    AsyncInvocation<Void> closeCacheInVM1 = vm1.invokeAsync(() -> getCache().close());

    closeCacheInVM0.await();
    closeCacheInVM1.await();

    int expectedValueFor0 = createDataInVM0.get();
    int expectedValueFor1 = createDataInVM1.get();

    AsyncInvocation<Void> createPersistentRegionInVM0 = vm0.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });
    AsyncInvocation<Void> createPersistentRegionInVM1 = vm1.invokeAsync(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
    });

    createPersistentRegionInVM0.await();
    createPersistentRegionInVM1.await();

    for (int key : asList(0, 1)) {
      int valueInVM0 = vm0.invoke(() -> getValue(key));
      int valueInVM1 = vm1.invoke(() -> getValue(key));

      int expectedValue = key == 0 ? expectedValueFor0 : expectedValueFor1;

      assertThat(valueInVM0)
          .isEqualTo(valueInVM1)
          .isBetween(expectedValue - 1, expectedValue);
    }
  }

  /**
   * Tests to make sure that after we get a conflicting persistent data exception, we can still
   * recover.
   */
  @Test
  public void testRecoverAfterConflict() {
    vm0.invoke(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
      putEntry("A", "B");
      getCache().close();
    });

    vm1.invoke(() -> {
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
      putEntry("A", "B");
    });

    vm0.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(ConflictingPersistentDataException.class)) {
        // this should cause a conflict
        Throwable thrown = catchThrowable(() -> {
          createReplicateRegion(regionName, getDiskDirs(getVMId()));
        });
        assertThat(thrown).isInstanceOf(ConflictingPersistentDataException.class);
      }
    });

    vm1.invoke(() -> getCache().close());

    vm0.invoke(() -> {
      // This should work now
      createReplicateRegion(regionName, getDiskDirs(getVMId()));
      updateEntry("A", "C");
    });

    // Now make sure vm1 gets a conflict
    vm1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(ConflictingPersistentDataException.class)) {
        // this should cause a conflict
        Throwable thrown = catchThrowable(() -> {
          createReplicateRegion(regionName, getDiskDirs(getVMId()));
        });
        assertThat(thrown).isInstanceOf(ConflictingPersistentDataException.class);
      }
    });
  }

  protected void createReplicateRegion(String regionName, File[] diskDirs,
      boolean diskSynchronous) {
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(diskDirs);
    diskStoreFactory.setMaxOplogSize(1);

    DiskStore diskStore = diskStoreFactory.create(regionName);

    RegionFactory regionFactory = getCache().createRegionFactory(REPLICATE_PERSISTENT);
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDiskSynchronous(diskSynchronous);

    regionFactory.create(regionName);
  }

  private void createReplicateRegion(String regionName, File[] diskDirs) {
    createReplicateRegion(regionName, diskDirs, true);
  }

  private void createReplicateRegion(String regionName) {
    getCache().createRegionFactory(REPLICATE).create(regionName);
  }

  private void createReplicateRegionWithoutCompaction(String regionName, File[] diskDirs) {
    DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
    diskStoreFactory.setAllowForceCompaction(true);
    diskStoreFactory.setAutoCompact(false);
    diskStoreFactory.setCompactionThreshold(20);
    diskStoreFactory.setDiskDirs(diskDirs);
    diskStoreFactory.setMaxOplogSize(1);

    DiskStore diskStore = diskStoreFactory.create(regionName);

    RegionFactory regionFactory = getCache().createRegionFactory(REPLICATE_PERSISTENT);
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDiskSynchronous(true);

    regionFactory.create(regionName);
  }

  private int getValue(int key) {
    return getCache().<Integer, Integer>getRegion(regionName).get(key);
  }

  private void putEntry(String key, String value) {
    getCache().<String, String>getRegion(regionName).put(key, value);
  }

  private void updateEntry(String key, String value) {
    getCache().<String, String>getRegion(regionName).put(key, value);
  }

  private void validateEntry(String key, String value) {
    assertThat(getCache().<String, String>getRegion(regionName).get(key)).isEqualTo(value);
  }

  private byte[] getRVVBytes() throws IOException {
    InternalRegion region = (InternalRegion) getCache().getRegion(regionName);

    RegionVersionVector regionVersionVector = region.getVersionVector();
    regionVersionVector = regionVersionVector.getCloneForTransmission();
    HeapDataOutputStream outputStream = new HeapDataOutputStream(2048);

    // Using gemfire serialization because RegionVersionVector is not java serializable
    DataSerializer.writeObject(regionVersionVector, outputStream);
    return outputStream.toByteArray();
  }

  private RegionVersionVector toRVV(byte[] bytes) throws IOException, ClassNotFoundException {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    return DataSerializer.readObject(new DataInputStream(inputStream));
  }

  private void addSleepBeforeSendAbstractUpdateMessage() {
    SLEEP.set(new CountDownLatch(1));
    DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
      @Override
      public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
        if (message instanceof AbstractUpdateMessage) {
          try {
            SLEEP.get().await(TIMEOUT_MILLIS, MILLISECONDS);
          } catch (InterruptedException e) {
            errorCollector.addError(e);
          }
        }
      }

      @Override
      public void afterProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
        if (message instanceof AbstractUpdateMessage) {
          DistributionMessageObserver.setInstance(null);
        }
      }
    });
  }

  private void waitForBlockedInitialization() {
    await().until(() -> {
      PersistentMemberManager manager = getCache().getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> regions = manager.getWaitingRegions();
      return !regions.isEmpty();
    });
  }

  private void assertSameRVV(RegionVersionVector expectedRVV, RegionVersionVector actualRVV) {
    assertThat(expectedRVV.sameAs(actualRVV))
        .as("Expected " + expectedRVV + " but was " + actualRVV)
        .isTrue();
  }

  private void validateDiskRegionInitializationStats(boolean localRecovery) {
    InternalRegion region = (InternalRegion) getCache().getRegion(regionName);
    DiskRegionStats diskRegionStats = region.getDiskRegion().getStats();

    if (localRecovery) {
      assertThat(diskRegionStats.getLocalInitializations()).isEqualTo(1);
      assertThat(diskRegionStats.getRemoteInitializations()).isEqualTo(0);
    } else {
      assertThat(diskRegionStats.getLocalInitializations()).isEqualTo(0);
      assertThat(diskRegionStats.getRemoteInitializations()).isEqualTo(1);
    }
  }

  private File[] getDiskDirs(int vmId) {
    return new File[] {getDiskDir(vmId)};
  }

  private File getDiskDir(int vmId) {
    File diskDir = diskDirs.get(vmId);
    diskDir.mkdirs();
    return diskDir;
  }

  private void backupDir(int vmId) throws IOException {
    File diskDir = getDiskDir(vmId);
    File backupDir = new File(rootDir, diskDir.getName() + ".bk");

    copyDirectory(diskDir, backupDir);
  }

  private void restoreBackup(int vmId) throws IOException {
    File diskDir = getDiskDir(vmId);
    File backupDir = new File(rootDir, diskDir.getName() + ".bk");

    if (!backupDir.renameTo(diskDir)) {
      deleteDirectory(diskDir);
      copyDirectory(backupDir, diskDir);
      deleteDirectory(backupDir);
    }
  }
}
