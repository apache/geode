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
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortsForDUnitSite;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.persistence.OplogType;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Tests to verify WAN functionality when the gateway-sender(s) have isolated, non-shared with
 * other region(s), disk-store(s).
 */
@RunWith(JUnitParamsRunner.class)
public class PersistentGatewaySenderWithIsolatedDiskStoreDistributedTest implements Serializable {
  private static final String REGION_NAME = "TestRegion";
  private static final String DISK_STORE_ID = "testDisk";
  private static final String GATEWAY_SENDER_ID = "testSender";
  private static final String TEST_CASE_NAME = "[{index}] {method}(RegionType:{0}, Parallel:{1})";
  private int site1Port, site2Port;
  private VM serverCluster1, serverCluster2;

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedDiskDirRule distributedDiskDirRule = new DistributedDiskDirRule();

  private Properties createLocatorConfiguration(int distributedSystemId, int localLocatorPort,
      int remoteLocatorPort) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(DISTRIBUTED_SYSTEM_ID, String.valueOf(distributedSystemId));
    config.setProperty(LOCATORS, "localhost[" + localLocatorPort + ']');
    config.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocatorPort + ']');
    config.setProperty(START_LOCATOR,
        "localhost[" + localLocatorPort + "],server=true,peer=true,hostname-for-clients=localhost");

    return config;
  }

  private Properties createServerConfiguration(int localLocatorPort) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "localhost[" + localLocatorPort + ']');

    return config;
  }

  private void createDiskStore() {
    String basePath = distributedDiskDirRule.getDiskDir().getAbsolutePath();
    File diskDirectory = new File(basePath + File.separator + DISK_STORE_ID);
    DiskStoreFactory diskStoreFactory = cacheRule.getCache().createDiskStoreFactory();
    diskStoreFactory.setAutoCompact(true);
    diskStoreFactory.setAllowForceCompaction(true);
    diskStoreFactory.setDiskDirs(new File[] {diskDirectory});
    diskStoreFactory.create(DISK_STORE_ID);
  }

  private void createRegion(RegionShortcut regionShortcut) {
    cacheRule.getCache()
        .<String, String>createRegionFactory(regionShortcut)
        .create(REGION_NAME);
  }

  private void createGatewayReceiver() {
    GatewayReceiverFactory gatewayReceiverFactory =
        cacheRule.getCache().createGatewayReceiverFactory();
    gatewayReceiverFactory.setManualStart(false);
    gatewayReceiverFactory.create();
  }

  private void createGatewaySender(boolean parallel, int remoteDistributedSystemId) {
    GatewaySenderFactory gatewaySenderFactory = cacheRule.getCache().createGatewaySenderFactory();
    gatewaySenderFactory.setParallel(parallel);
    gatewaySenderFactory.setDiskSynchronous(true);
    gatewaySenderFactory.setPersistenceEnabled(true);
    gatewaySenderFactory.setDiskStoreName(DISK_STORE_ID);
    gatewaySenderFactory.create(GATEWAY_SENDER_ID, remoteDistributedSystemId);
  }

  private void createServerWithRegionAndGatewayReceiver(RegionShortcut regionShortcut) {
    createGatewayReceiver();
    createRegion(regionShortcut);
  }

  private void createServerWithRegionAndPersistentGatewaySender(RegionShortcut regionShortcut,
      int remoteDistributedSystemId, boolean parallel) {
    createDiskStore();
    createRegion(regionShortcut);
    createGatewaySender(parallel, remoteDistributedSystemId);
    Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
    region.getAttributesMutator().addGatewaySenderId(GATEWAY_SENDER_ID);
  }

  private void gracefullyDisconnect() {
    InternalDistributedSystem.getConnectedInstance().stopReconnectingNoDisconnect();
    InternalDistributedSystem.getConnectedInstance().disconnect();
    await()
        .untilAsserted(() -> assertThat(InternalDistributedSystem.getConnectedInstance()).isNull());
  }

  private void awaitForQueueSize(int queueSize) {
    GatewaySender gatewaySender = cacheRule.getCache().getGatewaySender(GATEWAY_SENDER_ID);
    await().untilAsserted(() -> {
      Set<RegionQueue> queues = ((AbstractGatewaySender) gatewaySender).getQueues();
      int totalSize = queues.stream().mapToInt(RegionQueue::size).sum();
      assertThat(queueSize).isEqualTo(totalSize);
    });
  }

  @SuppressWarnings("unused")
  static Object[] regionAndGatewayTypes() {
    ArrayList<Object[]> parameters = new ArrayList<>();
    parameters.add(new Object[] {RegionShortcut.PARTITION, true});
    parameters.add(new Object[] {RegionShortcut.PARTITION, false});
    parameters.add(new Object[] {RegionShortcut.REPLICATE, false});

    return parameters.toArray();
  }

  @Before
  public void setUp() {
    VM locatorCluster1 = getVM(0);
    serverCluster1 = getVM(1);
    VM locatorCluster2 = getVM(2);
    serverCluster2 = getVM(3);

    int[] ports = getRandomAvailableTCPPortsForDUnitSite(2);
    site1Port = ports[0];
    site2Port = ports[1];

    // Start 2 sites, one locator and one server per site.
    locatorCluster1
        .invoke(() -> cacheRule.createCache(createLocatorConfiguration(1, site1Port, site2Port)));
    locatorCluster2
        .invoke(() -> cacheRule.createCache(createLocatorConfiguration(2, site2Port, site1Port)));

    serverCluster1.invoke(() -> cacheRule.createCache(createServerConfiguration(site1Port)));
    serverCluster2.invoke(() -> cacheRule.createCache(createServerConfiguration(site2Port)));
  }

  /**
   * The tests executes the following:
   * - Creates region and gateway-receiver on cluster2.
   * - Creates the region and gateway-sender on cluster1.
   * - Populates the region and waits until WAN replication has finished.
   * - Restarts server on cluster1, and stops it afterwards (the initial compaction occurs during
   * startup and the disk validation is done offline).
   * - Asserts that there are no orphaned drf files, neither compact-able records on the disks-tore.
   */
  @Test
  @TestCaseName(TEST_CASE_NAME)
  @Parameters(method = "regionAndGatewayTypes")
  public void diskStoreShouldBeCompactedOnMemberRestartWhenAllEventsHaveBeenDispatched(
      RegionShortcut regionShortcut, boolean parallel) throws Exception {
    final int entries = 100;

    // Create Region and Receiver on Cluster2
    serverCluster2.invoke(() -> createServerWithRegionAndGatewayReceiver(regionShortcut));

    // Create Region, DiskStore and Gateway on Cluster1
    String diskStorePath = serverCluster1.invoke(() -> {
      createServerWithRegionAndPersistentGatewaySender(regionShortcut, 2, parallel);
      DiskStore diskStore = cacheRule.getCache().findDiskStore(DISK_STORE_ID);
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);

      // Insert entries and wait for WAN replication to finish.
      IntStream.range(0, entries).forEach(value -> region.put("Key" + value, "Value" + value));
      awaitForQueueSize(0);

      return diskStore.getDiskDirs()[0].getAbsolutePath();
    });

    // Wait for Cluster2 to receive all events.
    serverCluster2.invoke(() -> await().untilAsserted(
        () -> assertThat(cacheRule.getCache().getRegion(REGION_NAME).size()).isEqualTo(entries)));

    // Restart and Stop Server on Cluster1
    serverCluster1.invoke(() -> {
      gracefullyDisconnect();
      cacheRule.createCache(createServerConfiguration(site1Port));
      createServerWithRegionAndPersistentGatewaySender(regionShortcut, 2, parallel);
      gracefullyDisconnect();
    });

    // There should be no orphaned drf files, neither compact-able records on the disk-store.
    File gatewayDiskStore = new File(diskStorePath);
    assertThat(gatewayDiskStore.list())
        .hasSize(3)
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + ".if")
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + "_2.drf")
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + "_2.crf");

    DiskStore diskStore =
        DiskStoreImpl.offlineValidate(DISK_STORE_ID, new File[] {gatewayDiskStore});
    assertThat(((DiskStoreImpl) diskStore).getLiveEntryCount()).isEqualTo(0);
    assertThat(((DiskStoreImpl) diskStore).getDeadRecordCount()).isEqualTo(0);
  }

  /**
   * The tests executes the following:
   * - Creates the region and a gateway-sender on cluster2.
   * - Populates the region and waits until all events have been enqueued.
   * - Restarts server on cluster2 and stops it afterwards (the initial compaction occurs during
   * startup and the validation is done offline).
   * - Verifies that there are no orphaned files neither compact-able records on the disk-store.
   * - Creates the region and a gateway-receiver on cluster1.
   * - Starts server on cluster2 again and waits for WAN replication to finish.
   * - Restart server on cluster2, and stop it afterwards (the initial compaction occurs during
   * startup and the validation is done offline).
   * - Asserts that there are no orphaned drf files, neither compact-able records on the disks-tore.
   */
  @Test
  @TestCaseName(TEST_CASE_NAME)
  @Parameters(method = "regionAndGatewayTypes")
  public void diskStoreShouldNotBeCompactedOnMemberRestartWhenThereAreNonDispatchedEventsInTheQueue(
      RegionShortcut regionShortcut, boolean parallel) throws Exception {
    final int entries = 1000;

    // Create Region, DiskStore and Gateway on Cluster2
    String diskStorePath = serverCluster2.invoke(() -> {
      createServerWithRegionAndPersistentGatewaySender(regionShortcut, 1, parallel);
      DiskStore diskStore = cacheRule.getCache().findDiskStore(DISK_STORE_ID);
      Region<String, String> region = cacheRule.getCache().getRegion(REGION_NAME);
      region.getAttributesMutator().addGatewaySenderId(GATEWAY_SENDER_ID);

      // Insert entries and wait for all events enqueued.
      IntStream.range(0, entries).forEach(value -> region.put("Key" + value, "Value" + value));
      awaitForQueueSize(entries);

      return diskStore.getDiskDirs()[0].getAbsolutePath();
    });

    // Restart Server on Cluster2
    serverCluster2.invoke(() -> {
      gracefullyDisconnect();
      cacheRule.createCache(createServerConfiguration(site2Port));
      createServerWithRegionAndPersistentGatewaySender(regionShortcut, 1, parallel);
      gracefullyDisconnect();
    });

    // Assert Disk Store status.
    File gatewayDiskStore = new File(diskStorePath);
    assertThat(gatewayDiskStore.list())
        .hasSize(6)
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + ".if")
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + "_1.krf")
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + "_1.drf")
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + "_1.crf")
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + "_2.drf")
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + "_2.crf");
    DiskStore diskStore =
        DiskStoreImpl.offlineValidate(DISK_STORE_ID, new File[] {gatewayDiskStore});
    assertThat(((DiskStoreImpl) diskStore).getDeadRecordCount()).isEqualTo(0);
    assertThat(((DiskStoreImpl) diskStore).getLiveEntryCount()).isEqualTo(entries);

    // Create Region and Receiver on Cluster1
    serverCluster1.invoke(() -> createServerWithRegionAndGatewayReceiver(regionShortcut));

    // Start Server on Cluster2
    serverCluster2.invoke(() -> {
      cacheRule.createCache(createServerConfiguration(site2Port));
      createServerWithRegionAndPersistentGatewaySender(regionShortcut, 1, parallel);

      // Await for WAN replication to finish.
      awaitForQueueSize(0);
    });

    // Wait for Cluster1 to receive all events.
    serverCluster1.invoke(() -> await().untilAsserted(
        () -> assertThat(cacheRule.getCache().getRegion(REGION_NAME).size()).isEqualTo(entries)));

    // Restart and stop Server on Cluster2
    serverCluster2.invoke(() -> {
      gracefullyDisconnect();
      cacheRule.createCache(createServerConfiguration(site2Port));
      createServerWithRegionAndPersistentGatewaySender(regionShortcut, 1, parallel);
      gracefullyDisconnect();
    });

    // Assert Disk Store status.
    assertThat(gatewayDiskStore.list())
        .hasSize(3)
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + ".if")
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + "_4.drf")
        .contains(OplogType.BACKUP.getPrefix() + DISK_STORE_ID + "_4.crf");
    DiskStore diskStoreFinal =
        DiskStoreImpl.offlineValidate(DISK_STORE_ID, new File[] {gatewayDiskStore});
    assertThat(((DiskStoreImpl) diskStoreFinal).getLiveEntryCount()).isEqualTo(0);
    assertThat(((DiskStoreImpl) diskStoreFinal).getDeadRecordCount()).isEqualTo(0);
  }
}
