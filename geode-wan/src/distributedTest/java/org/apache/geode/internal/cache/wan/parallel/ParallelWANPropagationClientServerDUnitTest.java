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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ParallelWANPropagationClientServerDUnitTest extends WANTestBase {

  /**
   * Normal happy scenario test case.
   */
  @Test
  public void testParallelPropagationWithClientServer() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createReceiverAndServer(nyPort));
    vm3.invoke(() -> WANTestBase.createReceiverAndServer(nyPort));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createClientWithLocatorAndRegion(nyPort, "localhost",
        getTestMethodName() + "_PR"));
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 100));

    vm5.invoke(() -> WANTestBase.createServer(lnPort));
    vm6.invoke(() -> WANTestBase.createServer(lnPort));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    vm7.invoke(() -> WANTestBase.createClientWithLocatorAndRegion(lnPort, "localhost",
        getTestMethodName() + "_PR"));

    startSenderInVMsAsync("ln", vm5, vm6);

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost

    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm7.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 10000));


    // verify all buckets drained on all sender nodes.
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));

    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));
    vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));

    vm7.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));

    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));

  }
}
