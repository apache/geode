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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ParallelWANPersistenceEnabledGatewaySenderCheckPossibleDuplicateDUnitTest
    extends WANTestBase {

  private static final long serialVersionUID = 2L;
  private static final Logger logger = LogService.getLogger();

  public ParallelWANPersistenceEnabledGatewaySenderCheckPossibleDuplicateDUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() throws Exception {
    // The restart tests log this string
    IgnoredException.addIgnoredException("failed accepting client connection");
  }

  /**
   * When gateway senders starts to unqueue, and check that received events are
   * not marked as possible duplicate.
   */
  @Test
  public void testPersistentPartitionedRegionWithGatewaySenderCheckReceiverNoPossibleDuplicate()
      throws InterruptedException {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5);
    vm4.invoke(() -> setNumDispatcherThreadsForTheRun(5));
    vm5.invoke(() -> setNumDispatcherThreadsForTheRun(5));

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, false));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, false));

    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), "ln", 1,
        100, isOffHeap()));


    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), null, 1,
        100, isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), null, 1,
        100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName(), 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));

    long vm2NumDuplicate = vm2.invoke(() -> WANTestBase.getReceiverStats().get(7));
    long vm3NumDuplicate = vm3.invoke(() -> WANTestBase.getReceiverStats().get(7));

    assertThat(vm2NumDuplicate + vm3NumDuplicate).isEqualTo(0);
  }


  /**
   * When gateway senders starts to unqueue, stop gateway sender, and check that some evnts are
   * dispatched to receiving side,
   * but events are not removed on sending side.
   */
  @Test
  public void testPersistentPartitionedRegionWithGatewaySenderCheckReceiverPossibleDuplicate()
      throws InterruptedException {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5);
    vm4.invoke(() -> setNumDispatcherThreadsForTheRun(5));
    vm5.invoke(() -> setNumDispatcherThreadsForTheRun(5));

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, false));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, false));

    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), "ln", 1,
        100, isOffHeap()));


    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), null, 1,
        100, isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), null, 1,
        100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName(), 1000));

    createReceiverInVMs(vm2, vm3);

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));

    long vm2NumDuplicate = vm2.invoke(() -> WANTestBase.getReceiverStats().get(7));
    long vm3NumDuplicate = vm3.invoke(() -> WANTestBase.getReceiverStats().get(7));

    assertThat(vm2NumDuplicate + vm3NumDuplicate).isEqualTo(100);
  }

  @Test
  public void testpersistentWanGateway_CheckReceiverPossibleDuplicate_afterSenderRestarted() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    createCacheInVMs(nyPort, vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // keep a larger batch to minimize number of exception occurrences in the log
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, false));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, false));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, false));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, false));

    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), "ln", 1,
        100, isOffHeap()));


    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), null, 1,
        100, isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName(), null, 1,
        100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName(), 1000));

    // Just making sure that though the remote site is started later,
    // remote site is still able to get the data. Since the receivers are
    // started before creating partition region it is quite possible that the
    // region may loose some of the events. This needs to be handled by the code

    vm5.invoke(() -> WANTestBase.killSender());

    createReceiverInVMs(vm2, vm3);

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));

    long vm2NumDuplicate = vm2.invoke(() -> WANTestBase.getReceiverStats().get(7));
    long vm3NumDuplicate = vm3.invoke(() -> WANTestBase.getReceiverStats().get(7));

    assertThat(vm2NumDuplicate + vm3NumDuplicate).isEqualTo(40);
  }


}
