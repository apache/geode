
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
package org.apache.geode.internal.cache.wan.parallel;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ParallelWANOverflowStatsDistributedTest extends WANTestBase {
  private static final int NUM_PUTS = 100;
  private static final long serialVersionUID = 1L;

  private String testName;

  public ParallelWANOverflowStatsDistributedTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() {
    testName = getTestMethodName();
  }

  @Test
  public void testPutDataAndCheckTotalQueueSizeBytesInUse() {
    // 1. Create locators
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);


    vm4.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm5.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm6.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm7.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));


    vm4.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm6.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm7.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));


    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));

    vm4.invoke(() -> doPuts(testName, 100));

    vm4.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm5.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm6.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm7.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));

  }


  @Test
  public void testPutDataThenCleanQueueCheckTotalQueueSizeBytesInUse() {
    // 1. Create locators
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);


    vm4.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm5.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm6.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm7.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));


    vm4.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm6.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm7.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));


    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));

    vm4.invoke(() -> doPuts(testName, 100));

    vm4.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> stopSender("ln"));
    vm6.invoke(() -> stopSender("ln"));
    vm7.invoke(() -> stopSender("ln"));

    vm4.invoke(() -> waitForSenderNonRunningState("ln"));
    vm5.invoke(() -> waitForSenderNonRunningState("ln"));
    vm6.invoke(() -> waitForSenderNonRunningState("ln"));
    vm7.invoke(() -> waitForSenderNonRunningState("ln"));

    startSenderwithCleanQueuesInVMsAsync("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));

    vm4.invoke(() -> checkTotalQueueSizeBytesInUseIsZero("ln"));
    vm5.invoke(() -> checkTotalQueueSizeBytesInUseIsZero("ln"));
    vm6.invoke(() -> checkTotalQueueSizeBytesInUseIsZero("ln"));
    vm7.invoke(() -> checkTotalQueueSizeBytesInUseIsZero("ln"));

  }


  @Test
  public void testPutDataThenCleanQueueThenPutDataCheckTotalQueueSizeBytesInUse() {
    // 1. Create locators
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);


    vm4.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm5.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm6.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm7.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, false));


    vm4.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm6.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm7.invoke(() -> createPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));


    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));

    vm4.invoke(() -> doPuts(testName, 100));

    vm4.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> stopSender("ln"));
    vm6.invoke(() -> stopSender("ln"));
    vm7.invoke(() -> stopSender("ln"));

    vm4.invoke(() -> waitForSenderNonRunningState("ln"));
    vm5.invoke(() -> waitForSenderNonRunningState("ln"));
    vm6.invoke(() -> waitForSenderNonRunningState("ln"));
    vm7.invoke(() -> waitForSenderNonRunningState("ln"));

    startSenderwithCleanQueuesInVMsAsync("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));

    vm4.invoke(() -> doPuts(testName, 100));

    vm4.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm5.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm6.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm7.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));

  }

  @Test
  public void testPutDataRestartServerThenCheckTotalQueueSizeBytesInUse() {
    // 1. Create locators
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // create senders with disk store
    String diskStore1 = vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, false));
    String diskStore2 = vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, false));
    String diskStore3 = vm6.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, false));
    String diskStore4 = vm7.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, false));


    vm4.invoke(() -> createPersistentPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm5.invoke(() -> createPersistentPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm6.invoke(() -> createPersistentPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm7.invoke(() -> createPersistentPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));


    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));

    vm4.invoke(() -> doPuts(testName, 100));

    vm4.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm5.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm6.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm7.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));

    vm5.invoke(() -> WANTestBase.killSender());

    createCacheInVMs(lnPort, vm5);

    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore2, false));

    vm5.invoke(() -> createPersistentPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));

    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> checkTotalQueueSizeBytesInUseAfterRestore("ln"));

  }

  @Test
  public void testPutDataRestartServerThenCleanQueueThenPutDataCheckTotalQueueSizeBytesInUse() {
    // 1. Create locators
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // create senders with disk store
    String diskStore1 = vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, false));
    String diskStore2 = vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, false));
    String diskStore3 = vm6.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, false));
    String diskStore4 = vm7.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, false));


    vm4.invoke(() -> createPersistentPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm5.invoke(() -> createPersistentPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm6.invoke(() -> createPersistentPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));
    vm7.invoke(() -> createPersistentPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));


    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));

    vm4.invoke(() -> doPuts(testName, 100));

    vm5.invoke(() -> WANTestBase.killSender());

    createCacheInVMs(lnPort, vm5);

    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore2, false));

    vm5.invoke(() -> createPersistentPartitionedRegion(testName, "ln", 1, 100, isOffHeap()));

    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> checkTotalQueueSizeBytesInUseAfterRestore("ln"));

    vm4.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> stopSender("ln"));
    vm6.invoke(() -> stopSender("ln"));
    vm7.invoke(() -> stopSender("ln"));

    vm4.invoke(() -> waitForSenderNonRunningState("ln"));
    vm5.invoke(() -> waitForSenderNonRunningState("ln"));
    vm6.invoke(() -> waitForSenderNonRunningState("ln"));
    vm7.invoke(() -> waitForSenderNonRunningState("ln"));

    startSenderwithCleanQueuesInVMsAsync("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));

    vm4.invoke(() -> doPuts(testName, 100));

    vm4.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm5.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm6.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));
    vm7.invoke(() -> checkTotalQueueSizeBytesInUse("ln"));

  }


  private void checkTotalQueueSizeBytesInUse(String senderId) throws Exception {

    // Get gateway sender mbean
    ManagementService service = ManagementService.getManagementService(cache);
    GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean(senderId);
    assertThat(bean).isNotNull();

    // Wait for the sampler to take a few samples
    waitForSamplerToSample(5);

    // Verify the bean attributes match the stat values
    await().untilAsserted(() -> {
      assertThat(bean.getTotalQueueSizeBytesInUse()).isGreaterThan(20000);
    });
  }


  private void checkTotalQueueSizeBytesInUseAfterRestore(String senderId) throws Exception {

    // Get gateway sender mbean
    ManagementService service = ManagementService.getManagementService(cache);
    GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean(senderId);
    assertThat(bean).isNotNull();

    // Wait for the sampler to take a few samples
    waitForSamplerToSample(5);

    // Verify the bean attributes match the stat values
    await().untilAsserted(() -> {
      assertThat(bean.getTotalQueueSizeBytesInUse()).isGreaterThan(12000);
    });
  }

  private void checkTotalQueueSizeBytesInUseIsZero(String senderId) throws Exception {

    // Get gateway sender mbean
    ManagementService service = ManagementService.getManagementService(cache);
    GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean(senderId);
    assertThat(bean).isNotNull();

    // Wait for the sampler to take a few samples
    waitForSamplerToSample(5);

    // Verify the bean attributes match the stat values
    await().untilAsserted(() -> {
      assertThat(bean.getTotalQueueSizeBytesInUse()).isEqualTo(0);
    });
  }

  private void waitForSamplerToSample(int numTimesToSample) throws Exception {
    InternalDistributedSystem ids = (InternalDistributedSystem) cache.getDistributedSystem();
    assertThat(ids.getStatSampler().waitForSampleCollector(60000)).isNotNull();
    for (int i = 0; i < numTimesToSample; i++) {
      assertThat(ids.getStatSampler().waitForSample((60000))).isTrue();
    }
  }

}
