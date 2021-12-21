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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class SerialWANPropagationLoopBackDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public SerialWANPropagationLoopBackDUnitTest() {
    super();
  }

  @Test
  public void testReplicatedSerialPropagationLoopBack() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createCache(lnPort));
    vm3.invoke(() -> WANTestBase.createCache(nyPort));
    vm2.invoke(() -> WANTestBase.createReceiver());
    vm3.invoke(() -> WANTestBase.createReceiver());

    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm6.invoke(() -> WANTestBase.createCache(nyPort));
    vm7.invoke(() -> WANTestBase.createCache(nyPort));

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ny", isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender("ln"));
    vm6.invoke(() -> WANTestBase.startSender("ny"));

    vm4.invoke(() -> WANTestBase.addQueueListener("ln", false));
    vm6.invoke(() -> WANTestBase.addQueueListener("ny", false));

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ny", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ny", isOffHeap()));

    final Map keyValues = new HashMap();
    for (int i = 0; i < 1; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    keyValues.clear();
    for (int i = 1; i < 2; i++) {
      keyValues.put(i, i);
    }
    vm6.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 2));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 2));

    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 2));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 2));
    vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 2));
    vm7.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 2));


    Wait.pause(5000);
    vm4.invoke(() -> WANTestBase.verifyQueueSize("ln", 0));
    vm6.invoke(() -> WANTestBase.verifyQueueSize("ny", 0));

    Map queueMap1 = vm4.invoke(() -> WANTestBase.checkQueue());
    Map queueMap2 = vm6.invoke(() -> WANTestBase.checkQueue());

    List createList1 = (List) queueMap1.get("Create");
    List updateList1 = (List) queueMap1.get("Update");
    List createList2 = (List) queueMap2.get("Create");
    List updateList2 = (List) queueMap2.get("Update");

    assertEquals(0, updateList1.size());
    assertEquals(0, updateList2.size());

    assertEquals(1, createList1.size());
    assertEquals(1, createList2.size());
  }

  @Test
  public void testReplicatedSerialPropagationLoopBack3SitesLoop() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer tkPort = vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    createCacheInVMs(lnPort, vm3, vm6);
    createCacheInVMs(nyPort, vm4, vm7);
    createCacheInVMs(tkPort, vm5);

    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ny", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "tk", isOffHeap()));

    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ny", isOffHeap()));

    vm3.invoke(() -> WANTestBase.createReceiver());
    vm4.invoke(() -> WANTestBase.createReceiver());
    vm5.invoke(() -> WANTestBase.createReceiver());

    vm6.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ny", 3, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("tk", 1, false, 100, 10, false, false, null, true));

    vm6.invoke(() -> WANTestBase.startSender("ln"));
    vm7.invoke(() -> WANTestBase.startSender("ny"));
    vm5.invoke(() -> WANTestBase.startSender("tk"));

    // using vm5 for sender in ds 3. cache is already created.
    vm6.invoke(() -> WANTestBase.addQueueListener("ln", false));
    vm7.invoke(() -> WANTestBase.addQueueListener("ny", false));
    vm5.invoke(() -> WANTestBase.addQueueListener("tk", false));

    int totalSize = 3;
    int increment = 1;

    final Map keyValues = new HashMap();
    for (int i = 0; i < increment; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    keyValues.clear();
    for (int i = increment; i < 2 * increment; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    keyValues.clear();
    for (int i = 2 * increment; i < totalSize; i++) {
      keyValues.put(i, i);
    }
    vm5.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));


    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", totalSize));
    vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", totalSize));
    vm7.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", totalSize));
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", totalSize));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", totalSize));


    vm6.invoke(() -> WANTestBase.verifyQueueSize("ln", 0));
    vm7.invoke(() -> WANTestBase.verifyQueueSize("ny", 0));
    vm5.invoke(() -> WANTestBase.verifyQueueSize("tk", 0));



    Map queueMap1 = vm6.invoke(() -> WANTestBase.checkQueue());
    Map queueMap2 = vm7.invoke(() -> WANTestBase.checkQueue());
    Map queueMap3 = vm5.invoke(() -> WANTestBase.checkQueue());

    List createList1 = (List) queueMap1.get("Create");
    List updateList1 = (List) queueMap1.get("Update");
    List createList2 = (List) queueMap2.get("Create");
    List updateList2 = (List) queueMap2.get("Update");
    List createList3 = (List) queueMap3.get("Create");
    List updateList3 = (List) queueMap3.get("Update");

    assertEquals(0, updateList1.size());
    assertEquals(0, updateList2.size());
    assertEquals(0, updateList3.size());

    assertEquals(2, createList1.size());
    assertEquals(2, createList2.size());
    assertEquals(2, createList3.size());
  }


  @Test
  public void testReplicatedSerialPropagationLoopBack3SitesNtoNPutInAllDS() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer tkPort = vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    createCacheInVMs(lnPort, vm3, vm6);
    createCacheInVMs(nyPort, vm4, vm7);
    createCacheInVMs(tkPort, vm5);

    vm3.invoke(() -> WANTestBase.createReceiver());
    vm4.invoke(() -> WANTestBase.createReceiver());
    vm5.invoke(() -> WANTestBase.createReceiver());

    // using vm5 for sender in ds 3. cache is already created.

    vm6.invoke(() -> WANTestBase.createSender("ln1", 2, false, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ny1", 3, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("tk1", 1, false, 100, 10, false, false, null, true));

    vm6.invoke(() -> WANTestBase.createSender("ln2", 3, false, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ny2", 1, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("tk2", 2, false, 100, 10, false, false, null, true));


    vm3.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1,ln2",
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ny1,ny2",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "tk1,tk2",
        isOffHeap()));

    vm6.invoke(() -> WANTestBase.startSender("ln1"));
    vm7.invoke(() -> WANTestBase.startSender("ny1"));
    vm5.invoke(() -> WANTestBase.startSender("tk1"));

    vm6.invoke(() -> WANTestBase.startSender("ln2"));
    vm7.invoke(() -> WANTestBase.startSender("ny2"));
    vm5.invoke(() -> WANTestBase.startSender("tk2"));


    vm6.invoke(() -> WANTestBase.addQueueListener("ln1", false));
    vm7.invoke(() -> WANTestBase.addQueueListener("ny1", false));
    vm5.invoke(() -> WANTestBase.addQueueListener("tk1", false));
    vm6.invoke(() -> WANTestBase.addSecondQueueListener("ln2", false));
    vm7.invoke(() -> WANTestBase.addSecondQueueListener("ny2", false));
    vm5.invoke(() -> WANTestBase.addSecondQueueListener("tk2", false));


    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1,ln2",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ny1,ny2",
        isOffHeap()));

    final Map keyValues = new HashMap();
    for (int i = 0; i < 1; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    keyValues.clear();
    for (int i = 1; i < 2; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    keyValues.clear();
    for (int i = 2; i < 3; i++) {

      keyValues.put(i, i);
    }
    vm5.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    Wait.pause(2000);
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3));
    vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3));
    vm7.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3));

    Wait.pause(5000);
    vm6.invoke(() -> WANTestBase.verifyQueueSize("ln1", 0));
    vm7.invoke(() -> WANTestBase.verifyQueueSize("ny1", 0));
    vm5.invoke(() -> WANTestBase.verifyQueueSize("tk1", 0));
    vm6.invoke(() -> WANTestBase.verifyQueueSize("ln2", 0));
    vm7.invoke(() -> WANTestBase.verifyQueueSize("ny2", 0));
    vm5.invoke(() -> WANTestBase.verifyQueueSize("tk2", 0));


    Map queueMap1 = vm6.invoke(() -> WANTestBase.checkQueue());
    Map queueMap2 = vm7.invoke(() -> WANTestBase.checkQueue());
    Map queueMap3 = vm5.invoke(() -> WANTestBase.checkQueue());
    Map queueMap4 = vm6.invoke(() -> WANTestBase.checkQueue2());
    Map queueMap5 = vm7.invoke(() -> WANTestBase.checkQueue2());
    Map queueMap6 = vm5.invoke(() -> WANTestBase.checkQueue2());

    List createList1 = (List) queueMap1.get("Create");
    List updateList1 = (List) queueMap1.get("Update");
    List createList2 = (List) queueMap2.get("Create");
    List updateList2 = (List) queueMap2.get("Update");
    List createList3 = (List) queueMap3.get("Create");
    List updateList3 = (List) queueMap3.get("Update");

    List createList4 = (List) queueMap4.get("Create");
    List updateList4 = (List) queueMap4.get("Update");

    List createList5 = (List) queueMap5.get("Create");
    List updateList5 = (List) queueMap5.get("Update");

    List createList6 = (List) queueMap6.get("Create");
    List updateList6 = (List) queueMap6.get("Update");


    assertEquals(0, updateList1.size());
    assertEquals(0, updateList2.size());
    assertEquals(0, updateList3.size());
    assertEquals(0, updateList4.size());
    assertEquals(0, updateList5.size());
    assertEquals(0, updateList6.size());

    assertEquals(1, createList1.size());
    assertEquals(1, createList2.size());
    assertEquals(1, createList3.size());
    assertEquals(1, createList4.size());
    assertEquals(1, createList5.size());
    assertEquals(1, createList6.size());
  }


  @Test
  public void testReplicatedSerialPropagationLoopBack3SitesNtoNPutFromOneDS() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer tkPort = vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    vm3.invoke(() -> WANTestBase.createCache(lnPort));
    vm4.invoke(() -> WANTestBase.createCache(nyPort));
    vm5.invoke(() -> WANTestBase.createCache(tkPort));
    vm3.invoke(() -> WANTestBase.createReceiver());
    vm4.invoke(() -> WANTestBase.createReceiver());
    vm5.invoke(() -> WANTestBase.createReceiver());


    vm3.invoke(() -> WANTestBase.createSender("ln1", 2, false, 100, 10, false, false, null, true));
    vm3.invoke(() -> WANTestBase.createSender("ln2", 3, false, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createSender("ny1", 3, false, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.createSender("ny2", 1, false, 100, 10, false, false, null, true));

    vm5.invoke(() -> WANTestBase.createSender("tk1", 1, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("tk2", 2, false, 100, 10, false, false, null, true));


    vm3.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1,ln2",
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ny1,ny2",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "tk1,tk2",
        isOffHeap()));


    vm3.invoke(() -> WANTestBase.startSender("ln1"));
    vm4.invoke(() -> WANTestBase.startSender("ny1"));
    vm5.invoke(() -> WANTestBase.startSender("tk1"));

    vm3.invoke(() -> WANTestBase.startSender("ln2"));
    vm4.invoke(() -> WANTestBase.startSender("ny2"));
    vm5.invoke(() -> WANTestBase.startSender("tk2"));


    vm3.invoke(() -> WANTestBase.addQueueListener("ln1", false));
    vm4.invoke(() -> WANTestBase.addQueueListener("ny1", false));
    vm5.invoke(() -> WANTestBase.addQueueListener("tk1", false));
    vm3.invoke(() -> WANTestBase.addSecondQueueListener("ln2", false));
    vm4.invoke(() -> WANTestBase.addSecondQueueListener("ny2", false));
    vm5.invoke(() -> WANTestBase.addSecondQueueListener("tk2", false));

    final Map keyValues = new HashMap();
    for (int i = 0; i < 1; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(() -> WANTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));

    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1));
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1));

    Wait.pause(5000);
    vm3.invoke(() -> WANTestBase.verifyQueueSize("ln1", 0));
    vm4.invoke(() -> WANTestBase.verifyQueueSize("ny1", 0));
    vm5.invoke(() -> WANTestBase.verifyQueueSize("tk1", 0));
    vm3.invoke(() -> WANTestBase.verifyQueueSize("ln2", 0));
    vm4.invoke(() -> WANTestBase.verifyQueueSize("ny2", 0));
    vm5.invoke(() -> WANTestBase.verifyQueueSize("tk2", 0));


    Map queueMap1 = vm3.invoke(() -> WANTestBase.checkQueue());
    Map queueMap2 = vm4.invoke(() -> WANTestBase.checkQueue());
    Map queueMap3 = vm5.invoke(() -> WANTestBase.checkQueue());
    Map queueMap4 = vm3.invoke(() -> WANTestBase.checkQueue2());
    Map queueMap5 = vm4.invoke(() -> WANTestBase.checkQueue2());
    Map queueMap6 = vm5.invoke(() -> WANTestBase.checkQueue2());

    List createList1 = (List) queueMap1.get("Create");
    List updateList1 = (List) queueMap1.get("Update");
    List createList2 = (List) queueMap2.get("Create");
    List updateList2 = (List) queueMap2.get("Update");
    List createList3 = (List) queueMap3.get("Create");
    List updateList3 = (List) queueMap3.get("Update");

    List createList4 = (List) queueMap4.get("Create");
    List updateList4 = (List) queueMap4.get("Update");

    List createList5 = (List) queueMap5.get("Create");
    List updateList5 = (List) queueMap5.get("Update");

    List createList6 = (List) queueMap6.get("Create");
    List updateList6 = (List) queueMap6.get("Update");


    assertEquals(0, updateList1.size());
    assertEquals(0, updateList2.size());
    assertEquals(0, updateList3.size());
    assertEquals(0, updateList4.size());
    assertEquals(0, updateList5.size());
    assertEquals(0, updateList6.size());

    assertEquals(1, createList1.size());
    assertEquals(0, createList2.size());
    assertEquals(0, createList3.size());
    assertEquals(1, createList4.size());
    assertEquals(0, createList5.size());
    assertEquals(0, createList6.size());
  }

}
