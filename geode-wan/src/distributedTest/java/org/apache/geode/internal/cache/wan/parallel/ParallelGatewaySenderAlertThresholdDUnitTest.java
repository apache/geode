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
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.WanTest;


/**
 * DUnit for ParallelSenderQueue alert threshold.
 */
@Category({WanTest.class})
public class ParallelGatewaySenderAlertThresholdDUnitTest extends WANTestBase {

  @Test
  public void testParallelSenderQueueEventsAlertThreshold() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(
        () -> WANTestBase.createSenderAlertThresholdWithoutDiskStore("ln", 2, 10, 10, false, true));
    vm5.invoke(
        () -> WANTestBase.createSenderAlertThresholdWithoutDiskStore("ln", 2, 10, 10, false, true));
    vm6.invoke(
        () -> WANTestBase.createSenderAlertThresholdWithoutDiskStore("ln", 2, 10, 10, false, true));
    vm7.invoke(
        () -> WANTestBase.createSenderAlertThresholdWithoutDiskStore("ln", 2, 10, 10, false, true));

    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    // give some time for the senders to pause
    Wait.pause(1000);

    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));

    int numEventPuts = 50;
    vm4.invoke(() -> WANTestBase.doHeavyPuts(getTestMethodName(), numEventPuts));


    // considering a memory limit of 40 MB, maximum of 40 events can be in memory. Rest should be on
    // disk.
    await().untilAsserted(() -> {
      long numOvVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
      long numOvVm5 = (Long) vm5.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
      long numOvVm6 = (Long) vm6.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
      long numOvVm7 = (Long) vm7.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));

      long numMemVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
      long numMemVm5 = (Long) vm5.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
      long numMemVm6 = (Long) vm6.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
      long numMemVm7 = (Long) vm7.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));

      LogWriterUtils.getLogWriter().info("Entries overflown to disk: " + numOvVm4 + "," + numOvVm5
          + "," + numOvVm6 + "," + numOvVm7);
      LogWriterUtils.getLogWriter().info(
          "Entries in VM: " + numMemVm4 + "," + numMemVm5 + "," + numMemVm6 + "," + numMemVm7);
      long totalOverflown = numOvVm4 + numOvVm5 + numOvVm6 + numOvVm7;
      assertTrue("Total number of entries overflown to disk should be at least greater than 55",
          (totalOverflown > 55));

      long totalInMemory = numMemVm4 + numMemVm5 + numMemVm6 + numMemVm7;
      // expected is twice the number of events put due to redundancy level of 1
      assertEquals("Total number of entries on disk and in VM is incorrect", (numEventPuts * 2),
          (totalOverflown + totalInMemory));

    });

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 50, 240000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 50, 240000));

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", -1));

    assertTrue((v4List.get(12) + v5List.get(12) + v6List.get(12) + v7List.get(12)) > 0);

    int v4alert = (int) vm4.invoke(() -> checkSenderMBeanAlertThreshold(true));
    int v5alert = (int) vm5.invoke(() -> checkSenderMBeanAlertThreshold(true));
    int v6alert = (int) vm6.invoke(() -> checkSenderMBeanAlertThreshold(true));
    int v7alert = (int) vm7.invoke(() -> checkSenderMBeanAlertThreshold(true));

    assertTrue((v4alert + v5alert + v6alert + v7alert) > 0);

  }

  public static int checkSenderMBeanAlertThreshold(boolean connected) {
    ManagementService service = ManagementService.getManagementService(cache);
    GatewaySenderMXBean bean = service.getLocalGatewaySenderMXBean("ln");
    assertNotNull(bean);
    await().untilAsserted(() -> assertEquals(connected, bean.isConnected()));
    return bean.getEventsExceedingAlertThreshold();
  }

}
