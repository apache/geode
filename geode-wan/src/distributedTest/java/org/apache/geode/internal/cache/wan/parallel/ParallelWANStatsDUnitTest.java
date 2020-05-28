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
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.Shipment;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ParallelWANStatsDUnitTest extends WANTestBase {

  private static final int NUM_PUTS = 100;
  private static final long serialVersionUID = 1L;

  private String testName;

  public ParallelWANStatsDUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() {
    this.testName = getTestMethodName();
  }

  @Test
  public void testConnectionStatsAreCreated() {
    // 1. Create locators
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // 2. Create cache & receiver in vm2
    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    // 3. Create cache & sender in vm4
    createSenderInVm(lnPort, vm4);

    // 4. Create Region in vm4 (sender)
    createSenderPRInVM(1, vm4);

    // 5. Create region in vm2 (receiver)
    createReceiverPR(vm2, 1);

    // 6. Start sender in vm4
    startSenderInVMs("ln", vm4);

    vm4.invoke(() -> WANTestBase.checkConnectionStats("ln"));
  }

  @Test
  public void testQueueSizeInSecondaryBucketRegionQueuesWithMemberRestart() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    createSendersWithConflation(lnPort);

    createSenderPRs(1);

    startPausedSenders();

    createReceiverPR(vm2, 1);
    putKeyValues();

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));

    assertEquals(NUM_PUTS,
        v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0)); // queue size
    assertEquals(NUM_PUTS * 2,
        v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)); // eventsReceived
    assertEquals(NUM_PUTS * 2,
        v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)); // events queued
    assertEquals(0,
        v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)); // events distributed
    assertEquals(NUM_PUTS,
        v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10)); // secondary queue size

    // stop vm7 to trigger rebalance and move some primary buckets
    System.out.println("Current secondary queue sizes:" + v4List.get(10) + ":" + v5List.get(10)
        + ":" + v6List.get(10) + ":" + v7List.get(10));
    vm7.invoke(() -> WANTestBase.closeCache());
    await().untilAsserted(() -> {
      int v4secondarySize = vm4.invoke(() -> WANTestBase.getSecondaryQueueSizeInStats("ln"));
      int v5secondarySize = vm5.invoke(() -> WANTestBase.getSecondaryQueueSizeInStats("ln"));
      int v6secondarySize = vm6.invoke(() -> WANTestBase.getSecondaryQueueSizeInStats("ln"));
      assertEquals(NUM_PUTS, v4secondarySize + v5secondarySize + v6secondarySize);
    });
    System.out.println("New secondary queue sizes:" + v4List.get(10) + ":" + v5List.get(10) + ":"
        + v6List.get(10));

    vm7.invoke(() -> WANTestBase.createCache(lnPort));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(testName, "ln", 1, 10, isOffHeap()));
    startSenderInVMs("ln", vm7);
    vm7.invoke(() -> pauseSender("ln"));

    v4List = (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    v5List = (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    v6List = (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    v7List = (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    assertEquals(NUM_PUTS,
        v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10)); // secondary
    // queue
    // size
    System.out.println("After restart vm7, secondary queue sizes:" + v4List.get(10) + ":"
        + v5List.get(10) + ":" + v6List.get(10) + ":" + v7List.get(10));

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));
    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(0, NUM_PUTS, NUM_PUTS));

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln", 0));

    v4List = (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    v5List = (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    v6List = (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    v7List = (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    // events distributed:
    assertEquals(NUM_PUTS, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3));
    // secondary queue size:
    assertEquals(0, v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10));
  }

  // TODO: add a test without redundancy for primary switch
  @Test
  public void testQueueSizeInSecondaryWithPrimarySwitch() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    createSendersWithConflation(lnPort);

    createSenderPRs(1);

    startPausedSenders();

    createReceiverPR(vm2, 1);

    putKeyValues();

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));

    // queue size:
    assertEquals(NUM_PUTS, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0));
    // events received:
    assertEquals(NUM_PUTS * 2, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1));
    // events queued:
    assertEquals(NUM_PUTS * 2, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2));
    // events distributed:
    assertEquals(0, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3));
    // secondary queue size:
    assertEquals(NUM_PUTS, v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10));

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));
    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));
    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(0, NUM_PUTS, NUM_PUTS));

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln", 0));

    v4List = (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    v5List = (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    v6List = (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    v7List = (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    // events distributed:
    assertEquals(NUM_PUTS, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3));
    // secondary queue size:
    assertEquals(0, v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10));
  }

  @Test
  public void testPartitionedRegionParallelPropagation_BeforeDispatch() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createSendersWithConflation(lnPort);

    createSenderPRs(0);

    startPausedSenders();

    createReceiverPR(vm2, 1);
    createReceiverPR(vm3, 1);

    putKeyValues();

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));

    // queue size:
    assertEquals(NUM_PUTS, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0));
    // events received:
    assertEquals(NUM_PUTS, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1));
    // events queued:
    assertEquals(NUM_PUTS, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2));
    // events distributed
    assertEquals(0, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3));
    // batches distributed:
    assertEquals(0, v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4));
    // batches redistributed
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5));
  }

  @Test
  public void testPartitionedRegionParallelPropagation_AfterDispatch_NoRedundancy() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    createSenders(lnPort);

    createReceiverPR(vm2, 0);

    createSenderPRs(0);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.doPuts(testName, NUM_PUTS));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    // queue size:
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0));
    // eventsReceived:
    assertEquals(NUM_PUTS, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1));
    // events queued:
    assertEquals(NUM_PUTS, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2));
    // events distributed:
    assertEquals(NUM_PUTS, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3));
    // batches distributed:
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 10);
    // batches redistributed:
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5));

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(10, NUM_PUTS, NUM_PUTS));
  }

  @Test
  public void testPRParallelPropagationWithoutGroupTransactionEventsSendsBatchesWithIncompleteTransactions() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    int batchTimeInterval = 10000;
    createSenders(lnPort, false, batchTimeInterval);

    createReceiverCustomerOrderShipmentPR(vm2, 0);

    createSenderCustomerOrderShipmentPRs(vm4, 0);
    createSenderCustomerOrderShipmentPRs(vm5, 0);
    createSenderCustomerOrderShipmentPRs(vm6, 0);
    createSenderCustomerOrderShipmentPRs(vm7, 0);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    final Map custKeyValue = new HashMap();
    int intCustId = 1;
    CustId custId = new CustId(intCustId);
    custKeyValue.put(custId, new Customer());
    vm4.invoke(() -> WANTestBase.putGivenKeyValue(customerRegionName, custKeyValue));

    int transactions = 3;
    final Map keyValues = new HashMap();
    for (int i = 0; i < transactions; i++) {
      OrderId orderId = new OrderId(i, custId);
      ShipmentId shipmentId1 = new ShipmentId(i, orderId);
      ShipmentId shipmentId2 = new ShipmentId(i + 1, orderId);
      ShipmentId shipmentId3 = new ShipmentId(i + 2, orderId);
      keyValues.put(orderId, new Order());
      keyValues.put(shipmentId1, new Shipment());
      keyValues.put(shipmentId2, new Shipment());
      keyValues.put(shipmentId3, new Shipment());
    }
    int eventsPerTransaction = 4;
    vm4.invoke(() -> WANTestBase.doOrderAndShipmentPutsInsideTransactions(keyValues,
        eventsPerTransaction));

    int entries = (transactions * eventsPerTransaction) + 1;

    vm4.invoke(() -> WANTestBase.validateRegionSize(customerRegionName, 1));
    vm4.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, transactions));
    vm4.invoke(() -> WANTestBase.validateRegionSize(shipmentRegionName, transactions * 3));

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    // queue size:
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0));
    // eventsReceived:
    assertEquals(entries, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1));
    // events queued:
    assertEquals(entries, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2));
    // events distributed:
    assertEquals(entries, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3));
    // batches distributed:
    assertEquals(2, v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4));
    // batches redistributed:
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5));
  }

  @Test
  public void testPRParallelPropagationWithGroupTransactionEventsSendsBatchesWithCompleteTransactions() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    int batchTimeInterval = 10000;
    createSenders(lnPort, true, batchTimeInterval);

    createReceiverCustomerOrderShipmentPR(vm2, 0);

    createSenderCustomerOrderShipmentPRs(vm4, 0);
    createSenderCustomerOrderShipmentPRs(vm5, 0);
    createSenderCustomerOrderShipmentPRs(vm6, 0);
    createSenderCustomerOrderShipmentPRs(vm7, 0);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    final Map custKeyValue = new HashMap();
    int intCustId = 1;
    CustId custId = new CustId(intCustId);
    custKeyValue.put(custId, new Customer());
    vm4.invoke(() -> WANTestBase.putGivenKeyValue(customerRegionName, custKeyValue));

    int transactions = 3;
    final Map keyValues = new HashMap();
    for (int i = 0; i < transactions; i++) {
      OrderId orderId = new OrderId(i, custId);
      ShipmentId shipmentId1 = new ShipmentId(i, orderId);
      ShipmentId shipmentId2 = new ShipmentId(i + 1, orderId);
      ShipmentId shipmentId3 = new ShipmentId(i + 2, orderId);
      keyValues.put(orderId, new Order());
      keyValues.put(shipmentId1, new Shipment());
      keyValues.put(shipmentId2, new Shipment());
      keyValues.put(shipmentId3, new Shipment());
    }

    // 3 transactions of 4 events each are sent so that the batch would
    // initially contain the first 2 transactions complete and the first
    // 2 events of the last transaction (10 entries).
    // As --group-transaction-events is configured in the senders, the remaining
    // 2 events of the last transaction are added to the batch which makes
    // that only one batch of 12 events is sent.
    int eventsPerTransaction = 4;
    vm4.invoke(() -> WANTestBase.doOrderAndShipmentPutsInsideTransactions(keyValues,
        eventsPerTransaction));

    int entries = (transactions * eventsPerTransaction) + 1;

    vm4.invoke(() -> WANTestBase.validateRegionSize(customerRegionName, 1));
    vm4.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, transactions));
    vm4.invoke(() -> WANTestBase.validateRegionSize(shipmentRegionName, transactions * 3));

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    // queue size:
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0));
    // eventsReceived:
    assertEquals(entries, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1));
    // events queued:
    assertEquals(entries, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2));
    // events distributed:
    assertEquals(entries, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3));
    // batches distributed:
    assertEquals(1, v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4));
    // batches redistributed:
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5));
  }

  @Test
  public void testPRParallelPropagationWithBatchRedistWithoutGroupTransactionEventsSendsBatchesWithIncompleteTransactions() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);

    int batchTimeInterval = 10000;
    createSenders(lnPort, false, batchTimeInterval);

    createSenderCustomerOrderShipmentPRs(vm4, 0);

    startSenderInVMs("ln", vm4);

    final Map custKeyValue = new HashMap();
    int intCustId = 1;
    CustId custId = new CustId(intCustId);
    custKeyValue.put(custId, new Customer());
    vm4.invoke(() -> WANTestBase.putGivenKeyValue(customerRegionName, custKeyValue));

    int transactions = 6;
    final Map keyValues = new HashMap();
    for (int i = 0; i < transactions; i++) {
      OrderId orderId = new OrderId(i, custId);
      ShipmentId shipmentId1 = new ShipmentId(i, orderId);
      ShipmentId shipmentId2 = new ShipmentId(i + 1, orderId);
      ShipmentId shipmentId3 = new ShipmentId(i + 2, orderId);
      keyValues.put(orderId, new Order());
      keyValues.put(shipmentId1, new Shipment());
      keyValues.put(shipmentId2, new Shipment());
      keyValues.put(shipmentId3, new Shipment());
    }
    int eventsPerTransaction = 4;
    vm4.invoke(() -> WANTestBase.doOrderAndShipmentPutsInsideTransactions(keyValues,
        eventsPerTransaction));

    int entries = (transactions * eventsPerTransaction) + 1;

    createReceiverCustomerOrderShipmentPR(vm2, 0);

    vm4.invoke(() -> WANTestBase.validateRegionSize(customerRegionName, 1));
    vm4.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, transactions));
    vm4.invoke(() -> WANTestBase.validateRegionSize(shipmentRegionName, transactions * 3));

    // wait for batches to be redistributed and then start the receiver
    vm4.invoke(() -> await()
        .until(() -> WANTestBase.getSenderStats("ln", -1).get(5) > 0));

    createReceiverInVMs(vm2);

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    // queue size:
    assertEquals(0, (int) v4List.get(0));
    // events received:
    assertEquals(entries, (int) v4List.get(1));
    // events queued:
    assertEquals(entries, (int) v4List.get(2));
    // events distributed:
    assertEquals(entries, (int) v4List.get(3));
    // batches distributed:
    assertEquals(3, (int) v4List.get(4));
    // batches redistributed:
    assertTrue("Batch was not redistributed", (v4List.get(5)) > 0);
  }

  @Test
  public void testPRParallelPropagationWithBatchRedistWithGroupTransactionEventsSendsBatchesWithCompleteTransactions() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);

    int batchTimeInterval = 10000;
    createSenders(lnPort, true, batchTimeInterval);

    createReceiverCustomerOrderShipmentPR(vm2, 0);

    createSenderCustomerOrderShipmentPRs(vm4, 0);

    startSenderInVMs("ln", vm4);


    final Map custKeyValue = new HashMap();
    int intCustId = 1;
    CustId custId = new CustId(intCustId);
    custKeyValue.put(custId, new Customer());
    vm4.invoke(() -> WANTestBase.putGivenKeyValue(customerRegionName, custKeyValue));

    int transactions = 6;
    final Map keyValues = new HashMap();
    for (int i = 0; i < transactions; i++) {
      OrderId orderId = new OrderId(i, custId);
      ShipmentId shipmentId1 = new ShipmentId(i, orderId);
      ShipmentId shipmentId2 = new ShipmentId(i + 1, orderId);
      ShipmentId shipmentId3 = new ShipmentId(i + 2, orderId);
      keyValues.put(orderId, new Order());
      keyValues.put(shipmentId1, new Shipment());
      keyValues.put(shipmentId2, new Shipment());
      keyValues.put(shipmentId3, new Shipment());
    }

    // 6 transactions of 4 events each are sent so that the first batch
    // would initially contain the first 2 transactions complete and the first
    // 2 events of the next transaction (10 entries).
    // As --group-transaction-events is configured in the senders, the remaining
    // 2 events of the second transaction are added to the batch which makes
    // that the first batch is sent with 12 events. The same happens with the
    // second batch which will contain 12 events too.
    int eventsPerTransaction = 4;
    vm4.invoke(() -> WANTestBase.doOrderAndShipmentPutsInsideTransactions(keyValues,
        eventsPerTransaction));

    int entries = (transactions * eventsPerTransaction) + 1;

    vm4.invoke(() -> WANTestBase.validateRegionSize(customerRegionName, 1));
    vm4.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, transactions));
    vm4.invoke(() -> WANTestBase.validateRegionSize(shipmentRegionName, transactions * 3));

    // wait for batches to be redistributed and then start the receiver
    vm4.invoke(() -> await()
        .until(() -> WANTestBase.getSenderStats("ln", -1).get(5) > 0));

    createReceiverInVMs(vm2);

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    // queue size:
    assertEquals(0, (int) v4List.get(0));
    // events received:
    assertEquals(entries, (int) v4List.get(1));
    // events queued:
    assertEquals(entries, (int) v4List.get(2));
    // events distributed:
    assertEquals(entries, (int) v4List.get(3));
    // batches distributed:
    assertEquals(2, (int) v4List.get(4));
    // batches redistributed:
    assertTrue("Batch was not redistributed", (v4List.get(5)) > 0);
  }

  @Test
  public void testPartitionedRegionParallelPropagation_AfterDispatch_Redundancy_3() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    createSenders(lnPort);

    createReceiverPR(vm2, 0);

    createSenderPRs(3);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.doPuts(testName, NUM_PUTS));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    // queue size:
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0));
    // events received:
    assertEquals(400, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1));
    // events queued:
    assertEquals(400, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2));
    // events distributed
    assertEquals(NUM_PUTS, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3));
    // batches distributed:
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 10);
    // batches redistributed:
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5));

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(10, NUM_PUTS, NUM_PUTS));
  }

  @Test
  public void testWANStatsTwoWanSites_Bug44331() {
    Integer lnPort = createFirstLocatorWithDSId(1);
    Integer nyPort = vm0.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer tkPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    createCacheInVMs(nyPort, vm2);
    createCacheInVMs(tkPort, vm3);
    createReceiverInVMs(vm2);
    createReceiverInVMs(vm3);

    vm4.invoke(() -> WANTestBase.createCache(lnPort));

    vm4.invoke(() -> WANTestBase.createSender("ln1", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createSender("ln2", 3, true, 100, 10, false, false, null, true));

    createReceiverPR(vm2, 0);
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(testName, null, 0, 10, isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(testName, "ln1,ln2", 0, 10, isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender("ln1"));

    vm4.invoke(() -> WANTestBase.startSender("ln2"));

    vm4.invoke(() -> WANTestBase.doPuts(testName, NUM_PUTS));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));
    vm3.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));

    ArrayList<Integer> v4Sender1List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln1", 0));
    ArrayList<Integer> v4Sender2List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln2", 0));

    assertEquals(0, v4Sender1List.get(0).intValue()); // queue size
    assertEquals(NUM_PUTS, v4Sender1List.get(1).intValue()); // eventsReceived
    assertEquals(NUM_PUTS, v4Sender1List.get(2).intValue()); // events queued
    assertEquals(NUM_PUTS, v4Sender1List.get(3).intValue()); // events distributed
    assertTrue(v4Sender1List.get(4).intValue() >= 10); // batches distributed
    assertEquals(0, v4Sender1List.get(5).intValue()); // batches redistributed

    assertEquals(0, v4Sender2List.get(0).intValue()); // queue size
    assertEquals(NUM_PUTS, v4Sender2List.get(1).intValue()); // eventsReceived
    assertEquals(NUM_PUTS, v4Sender2List.get(2).intValue()); // events queued
    assertEquals(NUM_PUTS, v4Sender2List.get(3).intValue()); // events distributed
    assertTrue(v4Sender2List.get(4).intValue() >= 10); // batches distributed
    assertEquals(0, v4Sender2List.get(5).intValue()); // batches redistributed

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(10, NUM_PUTS, NUM_PUTS));
    vm3.invoke(() -> WANTestBase.checkGatewayReceiverStats(10, NUM_PUTS, NUM_PUTS));
  }

  @Category({WanTest.class})
  @Test
  public void testParallelPropagationHA() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);

    createReceiverPR(vm2, 0);

    createReceiverInVMs(vm2);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    createSenderPRs(3);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    AsyncInvocation inv1 = vm5.invokeAsync(() -> WANTestBase.doPuts(testName, 1000));
    vm2.invoke(() -> await()
        .untilAsserted(() -> assertEquals("Waiting for first batch to be received", true,
            getRegionSize(testName) > 10)));
    AsyncInvocation inv2 = vm4.invokeAsync(() -> WANTestBase.killSender());
    inv1.join();
    inv2.join();

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, 1000));

    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    assertEquals(0, v5List.get(0) + v6List.get(0) + v7List.get(0)); // queue size
    int receivedEvents = v5List.get(1) + v6List.get(1) + v7List.get(1);
    // We may see a single retried event on all members due to the kill
    assertTrue("Received " + receivedEvents,
        3000 <= receivedEvents && 3003 >= receivedEvents); // eventsReceived
    int queuedEvents = v5List.get(2) + v6List.get(2) + v7List.get(2);
    assertTrue("Queued " + queuedEvents,
        3000 <= queuedEvents && 3003 >= queuedEvents); // eventsQueued
    // assertTrue(10000 <= v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed :
    // its quite possible that vm4 has distributed some of the events
    // assertTrue(v5List.get(4) + v6List.get(4) + v7List.get(4) > 1000); //batches distributed : its
    // quite possible that vm4 has distributed some of the batches.
    assertEquals(0, v5List.get(5) + v6List.get(5) + v7List.get(5)); // batches redistributed

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStatsHA(NUM_PUTS, 1000, 1000));
  }

  @Category({WanTest.class})
  @Test
  public void testParallelPropagationHAWithGroupTransactionEvents() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);

    createReceiverPR(vm2, 0);

    createReceiverInVMs(vm2);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    createSenderPRs(3);

    int batchSize = 9;
    boolean groupTransactionEvents = true;
    vm4.invoke(
        () -> WANTestBase.createSender("ln", 2, true, 100, batchSize, true, false, null, true,
            groupTransactionEvents,
            -1));
    vm5.invoke(
        () -> WANTestBase.createSender("ln", 2, true, 100, batchSize, true, false, null, true,
            groupTransactionEvents,
            -1));
    vm6.invoke(
        () -> WANTestBase.createSender("ln", 2, true, 100, batchSize, true, false, null, true,
            groupTransactionEvents,
            -1));
    vm7.invoke(
        () -> WANTestBase.createSender("ln", 2, true, 100, batchSize, true, false, null, true,
            groupTransactionEvents,
            -1));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    AsyncInvocation inv1 =
        vm5.invokeAsync(() -> WANTestBase.doTxPutsWithRetryIfError(testName, 2, 1000, 0));

    vm2.invoke(() -> await()
        .untilAsserted(() -> assertEquals("Waiting for some batches to be received", true,
            getRegionSize(testName) > 40)));
    AsyncInvocation inv3 = vm4.invokeAsync(() -> WANTestBase.killSender());
    inv1.join();
    inv3.join();

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, 2000));

    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    assertEquals(0, v5List.get(0) + v6List.get(0) + v7List.get(0)); // queue size
    int receivedEvents = v5List.get(1) + v6List.get(1) + v7List.get(1);
    // We may see two retried events (as transactions are made of 2 events) on all members due to
    // the kill
    assertTrue("Received " + receivedEvents,
        6000 <= receivedEvents && 6006 >= receivedEvents); // eventsReceived
    int queuedEvents = v5List.get(2) + v6List.get(2) + v7List.get(2);
    assertTrue("Queued " + queuedEvents,
        6000 <= queuedEvents && 6006 >= queuedEvents); // eventsQueued
    assertEquals(0, v5List.get(5) + v6List.get(5) + v7List.get(5)); // batches redistributed

    // batchesReceived is equal to numberOfEntries/(batchSize+1)
    // As transactions are 2 events long, for each batch it will always be necessary to
    // add one more entry to the 9 events batch in order to have complete transactions in the batch.
    int batchesReceived = (1000 + 1000) / (batchSize + 1);
    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStatsHA(batchesReceived, 2000, 2000));
  }


  /**
   * 1 region and sender configured on local site and 1 region and a receiver configured on remote
   * site. Puts to the local region are in progress. Remote region is destroyed in the middle.
   */
  @Test
  public void testParallelPropagationWithRemoteRegionDestroy() {
    addIgnoredException("RegionDestroyedException");
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverPR(vm2, 0);
    createReceiverInVMs(vm2);

    createSenders(lnPort);

    vm2.invoke(() -> WANTestBase.addCacheListenerAndDestroyRegion(testName));

    createSenderPRs(0);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    // start puts in RR_1 in another thread
    vm4.invoke(() -> WANTestBase.doPuts(testName, 2000));

    // verify that all is well in local site. All the events should be present in local region
    vm4.invoke(() -> WANTestBase.validateRegionSize(testName, 2000));

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", -1));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", -1));

    // batches distributed: it's quite possible that vm4 has distributed some of the batches.
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 1);
    // batches redistributed:
    assertTrue(v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5) >= 1);
  }

  @Test
  public void testParallelPropagationWithFilter() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);

    createReceiverPR(vm2, 1);

    createReceiverInVMs(vm2);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    createSenderPRs(0);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.doPuts(testName, 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, 800));

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    // queue size:
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0));
    // events received:
    assertEquals(1000, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1));
    // events queued:
    assertEquals(900, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2));
    // events distributed:
    assertEquals(800, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3));
    // batches distributed:
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 80);
    // batches redistributed:
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5));
    // events filtered:
    assertEquals(200, v4List.get(6) + v5List.get(6) + v6List.get(6) + v7List.get(6));
    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(80, 800, 800));
  }

  @Test
  public void testParallelPropagationConflation() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    createSendersWithConflation(lnPort);

    createSenderPRs(0);

    startPausedSenders();

    createReceiverPR(vm2, 1);

    Map keyValues = putKeyValues();

    // Verify the conflation indexes map is empty
    verifyConflationIndexesSize("ln", 0, vm4, vm5, vm6, vm7);

    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 50; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm4.invoke(() -> WANTestBase.putGivenKeyValue(testName, updateKeyValues));

    // Verify the conflation indexes map equals the number of updates
    verifyConflationIndexesSize("ln", 50, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln",
        keyValues.size() + updateKeyValues.size() /* creates aren't conflated */));

    // Do the puts again. Since these are updates, the previous updates will be conflated.
    vm4.invoke(() -> WANTestBase.putGivenKeyValue(testName, updateKeyValues));

    // Verify the conflation indexes map still equals the number of updates
    verifyConflationIndexesSize("ln", 50, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln",
        keyValues.size() + updateKeyValues.size() /* creates aren't conflated */));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, 0));

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(0, 0, 0));

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    keyValues.putAll(updateKeyValues);
    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, keyValues.size()));

    vm2.invoke(() -> WANTestBase.validateRegionContents(testName, keyValues));

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(0, 150, NUM_PUTS));

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln", 0));

    List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    // Verify final stats
    // 0 -> eventQueueSize
    // 1 -> eventsReceived
    // 2 -> eventsQueued
    // 3 -> eventsDistributed
    // 4 -> batchesDistributed
    // 5 -> batchesRedistributed
    // 7 -> eventsNotQueuedConflated
    // 9 -> conflationIndexesMapSize
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0));
    assertEquals(200, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1));
    assertEquals(200, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2));
    assertEquals(150, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3));
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 10);
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5));
    assertEquals(50, v4List.get(7) + v5List.get(7) + v6List.get(7) + v7List.get(7));
    assertEquals(0, v4List.get(9) + v5List.get(9) + v6List.get(9) + v7List.get(9));
  }

  @Test
  public void testConflationWithSameEntryPuts() {
    // Start locators
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm2.invoke(() -> createFirstRemoteLocator(2, lnPort));

    // Configure sending site member
    String senderId = "ny";
    String regionName = this.testName + "_PR";
    vm1.invoke(() -> createCache(lnPort));
    vm1.invoke(() -> createSender(senderId, 2, true, 100, 10, true, true, null, false));
    vm1.invoke(() -> createPartitionedRegion(regionName, senderId, 0, 10, isOffHeap()));

    // Do puts of the same key
    int numIterations = 100;
    vm1.invoke(() -> putSameEntry(regionName, numIterations));

    // Wait for appropriate queue size
    vm1.invoke(() -> checkQueueSize(senderId, 2));

    // Verify the conflation indexes size stat
    verifyConflationIndexesSize(senderId, 1, vm1);

    // Configure receiving site member
    vm3.invoke(() -> createCache(nyPort));
    vm3.invoke(() -> createReceiver());
    vm3.invoke(() -> createPartitionedRegion(regionName, null, 0, 10, isOffHeap()));

    // Wait for queue to drain
    vm1.invoke(() -> checkQueueSize(senderId, 0));

    // Verify the conflation indexes size stat
    verifyConflationIndexesSize(senderId, 0, vm1);
  }

  protected Map putKeyValues() {
    final Map keyValues = new HashMap();
    for (int i = 0; i < NUM_PUTS; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(() -> WANTestBase.putGivenKeyValue(testName, keyValues));

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln", keyValues.size()));

    return keyValues;
  }

  protected void createReceiverPR(VM vm, int redundancy) {
    vm.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, null, redundancy, 10, isOffHeap()));
  }

  protected void createReceiverCustomerOrderShipmentPR(VM vm, int redundancy) {
    vm.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion(null, redundancy, 10,
            isOffHeap()));
  }

  protected void createSenderCustomerOrderShipmentPRs(VM vm, int redundancy) {
    vm.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", redundancy, 10,
            isOffHeap()));
  }

  protected void createSenderPRs(int redundancy) {
    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, "ln", redundancy, 10, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, "ln", redundancy, 10, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, "ln", redundancy, 10, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, "ln", redundancy, 10, isOffHeap()));
  }

  protected void createSenderPRInVM(int redundancy, VM vm) {
    vm.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, "ln", redundancy, 10, isOffHeap()));
  }

  protected void startPausedSenders() {
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> pauseSender("ln"));
    vm5.invoke(() -> pauseSender("ln"));
    vm6.invoke(() -> pauseSender("ln"));
    vm7.invoke(() -> pauseSender("ln"));
  }

  protected void createSendersWithConflation(Integer lnPort) {
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
  }

  protected void createSenderInVm(Integer lnPort, VM vm) {
    createCacheInVMs(lnPort, vm);
    vm.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
  }

  protected void createSenders(Integer lnPort, boolean groupTransactionEvents,
      int batchTimeInterval) {
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true,
        groupTransactionEvents, batchTimeInterval));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true,
        groupTransactionEvents, batchTimeInterval));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true,
        groupTransactionEvents, batchTimeInterval));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true,
        groupTransactionEvents, batchTimeInterval));
  }

  protected void createSenders(Integer lnPort) {
    createSenders(lnPort, false, -1);
  }

  private void verifyConflationIndexesSize(String senderId, int expectedSize, VM... vms) {
    int actualSize = 0;
    for (VM vm : vms) {
      List<Integer> stats = vm.invoke(() -> WANTestBase.getSenderStats(senderId, -1));
      actualSize += stats.get(9);
    }
    assertEquals(expectedSize, actualSize);
  }

  private void putSameEntry(String regionName, int numIterations) {
    // This does one create and numInterations-1 updates
    Region region = cache.getRegion(regionName);
    for (int i = 0; i < numIterations; i++) {
      region.put(0, i);
    }
  }
}
