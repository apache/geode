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
package org.apache.geode.internal.cache.wan.serial;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.geode.internal.cache.wan.WANTestBase;


public class SerialWANConflationDUnitTest extends WANTestBase {

  @Test
  public void testSerialPropagationPartitionRegionBatchConflation() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);

    vm2.invoke(() -> createPartitionedRegion(getTestMethodName(), null, 1, 8, isOffHeap()));
    vm3.invoke(() -> createPartitionedRegion(getTestMethodName(), null, 1, 8, isOffHeap()));

    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 0, 8, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 0, 8, isOffHeap()));
    vm6.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 0, 8, isOffHeap()));
    vm7.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 0, 8, isOffHeap()));

    vm4.invoke(() -> createSender("ln", 2, false, 100, 50, false, false, null, true));
    vm5.invoke(() -> createSender("ln", 2, false, 100, 50, false, false, null, true));
    vm6.invoke(() -> createSender("ln", 2, false, 100, 50, false, false, null, true));
    vm7.invoke(() -> createSender("ln", 2, false, 100, 50, false, false, null, true));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> pauseSender("ln"));
    vm5.invoke(() -> pauseSender("ln"));
    vm6.invoke(() -> pauseSender("ln"));
    vm7.invoke(() -> pauseSender("ln"));


    final Map keyValues = new HashMap();

    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 10; j++) {
        keyValues.put(j, i);
      }
      vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), keyValues));
    }

    vm4.invoke(() -> enableConflation("ln"));
    vm5.invoke(() -> enableConflation("ln"));
    vm6.invoke(() -> enableConflation("ln"));
    vm7.invoke(() -> enableConflation("ln"));

    vm4.invoke(() -> resumeSender("ln"));
    vm5.invoke(() -> resumeSender("ln"));
    vm6.invoke(() -> resumeSender("ln"));
    vm7.invoke(() -> resumeSender("ln"));

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    assertTrue("No events conflated in batch",
        (v4List.get(8) + v5List.get(8) + v6List.get(8) + v7List.get(8)) > 0);

    vm2.invoke(() -> validateRegionSize(getTestMethodName(), 10));

  }

}
