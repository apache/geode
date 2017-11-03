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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * DUnit for ClearParallelSenderQueueWhenReceiverNotStartedRegressionTest operations. GEODE-3937
 */
@Category(DistributedTest.class)
public class ClearParallelSenderQueueWhenReceiverNotStartedRegressionTest extends WANTestBase {

  @Test
  public void testParallelSenderQueueEventsOverflow_NoDiskStoreSpecified() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);// backup-vm is not started in this regression
    createReceiverInVMs(vm2);

    createCacheInVMs(lnPort, vm4);// backup-vm is not started in this regression

    vm4.invoke(() -> WANTestBase.createSenderWithoutDiskStore("ln", 2, 10, 10, false, true));

    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4);

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));

    // give some time for the senders to pause
    Awaitility.await().atMost(1, TimeUnit.MINUTES)
        .until(() -> vm4.invoke(() -> WANTestBase.isSenderPaused("ln")));

    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));

    int numEventPuts = 50;
    vm4.invoke(() -> WANTestBase.doHeavyPuts(getTestMethodName(), numEventPuts));

    long numOvVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
    long numMemVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));

    assertEquals("Total number of entries  in VM4 is incorrect", (numEventPuts * 1),
        (numOvVm4 + numMemVm4));

    // clear the queue
    vm4.invoke(() -> WANTestBase.clearGatewaySender("ln"));

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));

    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {

      int numMemVm2 = vm2.invoke(() -> WANTestBase.getRegionSize(getTestMethodName()));

      assertEquals("Total number of entries  in VM2 is incorrect", 0, numMemVm2);

    });

  }


}
