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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.management.internal.cli.functions.ReplicateRegionFunction;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ReplicateRegionFunctionDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public ReplicateRegionFunctionDUnitTest() {
    super();
  }

  @Test
  public void testReplicateRegionFunction_WithReplicatedRegionAndSerialGatewaySender() {
    testReplicateRegionFunction(false, false);
  }

  @Test
  public void testReplicateRegionFunction_WithPartitionedRegionAndSerialGatewaySender() {
    testReplicateRegionFunction(true, false);
  }

  @Test
  public void testReplicateRegionFunction_WithPartitionedRegionAndParallelGatewaySender() {
    testReplicateRegionFunction(true, true);
  }

  public void testReplicateRegionFunction(boolean isPartitionedRegion,
      boolean isParallelGatewaySender) {
    Integer melPort = vm2.invoke(() -> WANTestBase.createFirstLocatorWithDSId(3));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, melPort));
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstRemoteLocator(1, nyPort));

    vm3.invoke(() -> WANTestBase.createServer(nyPort));
    vm4.invoke(() -> WANTestBase.createServer(melPort));
    vm5.invoke(() -> WANTestBase.createServer(lnPort));
    vm6.invoke(() -> WANTestBase.createServer(lnPort));
    vm7.invoke(() -> WANTestBase.createServer(lnPort));

    vm5.invoke(() -> FunctionService.registerFunction(new ReplicateRegionFunction()));
    vm6.invoke(() -> FunctionService.registerFunction(new ReplicateRegionFunction()));
    vm7.invoke(() -> FunctionService.registerFunction(new ReplicateRegionFunction()));

    String regionName;
    if (isPartitionedRegion) {
      regionName = getTestMethodName() + "_PR";
      vm5.invoke(() -> WANTestBase.createPartitionedRegion(regionName, "ny", 1, 100,
          isOffHeap()));
      vm6.invoke(() -> WANTestBase.createPartitionedRegion(regionName, "ny", 1, 100,
          isOffHeap()));
      vm7.invoke(() -> WANTestBase.createPartitionedRegion(regionName, "ny", 1, 100,
          isOffHeap()));
      vm3.invoke(() -> WANTestBase.createPartitionedRegion(regionName, "mel", 1, 100,
          isOffHeap()));
      vm4.invoke(() -> WANTestBase.createPartitionedRegion(regionName, null, 1, 100,
          isOffHeap()));
    } else {
      regionName = getTestMethodName() + "_RR";
      vm5.invoke(() -> WANTestBase.createReplicatedRegion(regionName, "ny",
          Scope.GLOBAL, DataPolicy.REPLICATE,
          isOffHeap()));
      vm6.invoke(() -> WANTestBase.createReplicatedRegion(regionName, "ny",
          Scope.GLOBAL, DataPolicy.REPLICATE,
          isOffHeap()));
      vm7.invoke(() -> WANTestBase.createReplicatedRegion(regionName, "ny",
          Scope.GLOBAL, DataPolicy.REPLICATE,
          isOffHeap()));
      vm3.invoke(() -> WANTestBase.createReplicatedRegion(regionName, "mel",
          Scope.GLOBAL, DataPolicy.REPLICATE,
          isOffHeap()));
      vm4.invoke(() -> WANTestBase.createReplicatedRegion(regionName, null,
          Scope.GLOBAL, DataPolicy.REPLICATE,
          isOffHeap()));
    }

    vm8.invoke(() -> WANTestBase.createClientWithLocator(lnPort, "localhost",
        regionName, ClientRegionShortcut.PROXY));

    vm8.invoke(() -> WANTestBase.doClientPutsFrom(regionName, 0, 100));

    vm5.invoke(() -> WANTestBase.validateRegionSize(regionName, 100));
    vm6.invoke(() -> WANTestBase.validateRegionSize(regionName, 100));
    vm7.invoke(() -> WANTestBase.validateRegionSize(regionName, 100));

    // Check that entries are not replicated
    vm3.invoke(() -> WANTestBase.validateRegionSize(regionName, 0));
    vm4.invoke(() -> WANTestBase.validateRegionSize(regionName, 0));

    // Create senders and receivers
    // ln (vm4, vm5, vm6) replicates to ny (vm3). ny (vm3) replicates to mel (vm4)
    createReceiverInVMs(vm3);
    createReceiverInVMs(vm4);
    vm5.invoke(() -> WANTestBase.createSender("ny", 2, isParallelGatewaySender, 100, 10, false,
        false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ny", 2, isParallelGatewaySender, 100, 10, false,
        false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ny", 2, isParallelGatewaySender, 100, 10, false,
        false, null, true));
    startSenderInVMsAsync("ny", vm5, vm6, vm7);
    vm3.invoke(() -> WANTestBase.createSender("mel", 3, isParallelGatewaySender, 100, 10, false,
        false, null, true));
    startSenderInVMsAsync("mel", vm3);

    // Check that entries are not replicated
    vm3.invoke(() -> WANTestBase.validateRegionSize(regionName, 0));
    vm4.invoke(() -> WANTestBase.validateRegionSize(regionName, 0));

    // Execute replicate region function
    vm8.invoke(() -> executeReplicateFunction(regionName, "ny", false, 50, 50));

    // Check that entries are replicated in ny
    vm3.invoke(() -> WANTestBase.validateRegionSize(regionName, 100));
    // Check that entries are not replicated in mel
    vm4.invoke(() -> WANTestBase.validateRegionSize(regionName, 0));
  }

  private void executeReplicateFunction(String region, String senderId, boolean isCancel,
      int batchSize, long maxRate) {
    Object[] args = {region, senderId, isCancel, maxRate, batchSize};
    ClientCacheFactory cacheFactory = new ClientCacheFactory();
    RegionService regionService = cacheFactory.create();
    Execution execution = FunctionService.onServers(regionService).setArguments(args);
    ResultCollector rc = execution.execute(new ReplicateRegionFunction().getId());
    System.out.println("Result: " + rc.getResult());
  }
}
