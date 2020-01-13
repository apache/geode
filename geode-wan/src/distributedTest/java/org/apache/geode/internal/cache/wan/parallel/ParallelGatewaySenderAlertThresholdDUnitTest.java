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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.WanTest;


/**
 * DUnit for ParallelSenderQueue alert threshold.
 */
@Category({WanTest.class})
public class ParallelGatewaySenderAlertThresholdDUnitTest extends WANTestBase {

  @Test
  public void testParallelSenderQueueEventsAlertThreshold() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    SerializableRunnableIF createSenderAlertThresholdWithoutDiskStoreRunnable =
        () -> WANTestBase.createSenderAlertThresholdWithoutDiskStore("ln", 2, 10, 10, false, true,
            100);
    vm4.invoke(createSenderAlertThresholdWithoutDiskStoreRunnable);
    vm5.invoke(createSenderAlertThresholdWithoutDiskStoreRunnable);
    vm6.invoke(createSenderAlertThresholdWithoutDiskStoreRunnable);
    vm7.invoke(createSenderAlertThresholdWithoutDiskStoreRunnable);

    SerializableRunnableIF createPartitionedRegionRunnableln =
        () -> WANTestBase.createPartitionedRegion(getUniqueName(), "ln", 1, 100, isOffHeap());
    vm4.invoke(createPartitionedRegionRunnableln);
    vm5.invoke(createPartitionedRegionRunnableln);
    vm6.invoke(createPartitionedRegionRunnableln);
    vm7.invoke(createPartitionedRegionRunnableln);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    SerializableRunnableIF createPartitionedRegionRunnable =
        () -> WANTestBase.createPartitionedRegion(getUniqueName(), null, 1, 100, isOffHeap());
    vm2.invoke(createPartitionedRegionRunnable);
    vm3.invoke(createPartitionedRegionRunnable);

    int numEventPuts = 50;
    vm4.invoke(() -> WANTestBase.doHeavyPuts(getUniqueName(), numEventPuts));


    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    SerializableRunnableIF serializableRunnableIF =
        () -> WANTestBase.validateRegionSize(getUniqueName(), 50);
    vm2.invoke(serializableRunnableIF);
    vm3.invoke(serializableRunnableIF);

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", -1));

    assertTrue("GatewaySenders Stats should contain number of EventsExceedingAlertThreshold > 0",
        (v4List.get(12) + v5List.get(12) + v6List.get(12) + v7List.get(12)) > 0);

    int v4alert = vm4.invoke(
        ParallelGatewaySenderAlertThresholdDUnitTest::checkSenderMBeanAlertThreshold);
    int v5alert = vm5.invoke(
        ParallelGatewaySenderAlertThresholdDUnitTest::checkSenderMBeanAlertThreshold);
    int v6alert = vm6.invoke(
        ParallelGatewaySenderAlertThresholdDUnitTest::checkSenderMBeanAlertThreshold);
    int v7alert = vm7.invoke(
        ParallelGatewaySenderAlertThresholdDUnitTest::checkSenderMBeanAlertThreshold);

    assertTrue("GatewaySenders MBean should contain number of EventsExceedingAlertThreshold > 0",
        (v4alert + v5alert + v6alert + v7alert) > 0);

  }

  private static int checkSenderMBeanAlertThreshold() {
    ManagementService service = ManagementService.getManagementService(cache);
    GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean("ln");
    assertNotNull(bean);
    await().untilAsserted(() -> assertTrue(bean.isConnected()));
    return bean.getEventsExceedingAlertThreshold();
  }


  @Test
  public void testParallelSenderQueueNoEventsExceedingHighAlertThreshold() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    SerializableRunnableIF createSenderAlertThresholdWithoutDiskStoreRunnable =
        () -> WANTestBase.createSenderAlertThresholdWithoutDiskStore("ln", 2, 10, 10, false, true,
            10000);
    vm4.invoke(createSenderAlertThresholdWithoutDiskStoreRunnable);
    vm5.invoke(createSenderAlertThresholdWithoutDiskStoreRunnable);
    vm6.invoke(createSenderAlertThresholdWithoutDiskStoreRunnable);
    vm7.invoke(createSenderAlertThresholdWithoutDiskStoreRunnable);

    SerializableRunnableIF createPartitionedRegionRunnableln =
        () -> WANTestBase.createPartitionedRegion(getUniqueName(), "ln", 1, 100, isOffHeap());
    vm4.invoke(createPartitionedRegionRunnableln);
    vm5.invoke(createPartitionedRegionRunnableln);
    vm6.invoke(createPartitionedRegionRunnableln);
    vm7.invoke(createPartitionedRegionRunnableln);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    SerializableRunnableIF createPartitionedRegionRunnable =
        () -> WANTestBase.createPartitionedRegion(getUniqueName(), null, 1, 100, isOffHeap());
    vm2.invoke(createPartitionedRegionRunnable);
    vm3.invoke(createPartitionedRegionRunnable);

    int numEventPuts = 50;
    vm4.invoke(() -> WANTestBase.doHeavyPuts(getUniqueName(), numEventPuts));

    SerializableRunnableIF serializableRunnableIF =
        () -> WANTestBase.validateRegionSize(getUniqueName(), 50);
    vm2.invoke(serializableRunnableIF);
    vm3.invoke(serializableRunnableIF);

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", -1));

    assertEquals("GatewaySenders Stats should contain number of EventsExceedingAlertThreshold = 0",
        (v4List.get(12) + v5List.get(12) + v6List.get(12) + v7List.get(12)), 0);

    int v4alert = vm4.invoke(
        ParallelGatewaySenderAlertThresholdDUnitTest::checkSenderMBeanAlertThreshold);
    int v5alert = vm5.invoke(
        ParallelGatewaySenderAlertThresholdDUnitTest::checkSenderMBeanAlertThreshold);
    int v6alert = vm6.invoke(
        ParallelGatewaySenderAlertThresholdDUnitTest::checkSenderMBeanAlertThreshold);
    int v7alert = vm7.invoke(
        ParallelGatewaySenderAlertThresholdDUnitTest::checkSenderMBeanAlertThreshold);

    assertEquals("GatewaySenders MBean should contain number of EventsExceedingAlertThreshold = 0",
        (v4alert + v5alert + v6alert + v7alert), 0);

  }

}
