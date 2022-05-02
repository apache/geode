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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.locks.DLockReleaseProcessor;
import org.apache.geode.internal.cache.UpdateOperation;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.Shipment;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.partitioned.DeposePrimaryBucketMessage;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.DistributedBlackboard;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ParallelWANConflationDistributedTest extends WANTestBase {
  @Rule
  public DistributedBlackboard blackboard = new DistributedBlackboard();

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogService.getLogger();
  private static final String DEPOSE_PRIMARY_MESSAGE_RECEIVED = "received_deposePrimaryMessage";
  private static final String CONTINUE_PUT_OP = "continue_put_op";
  private static final String CONTINUE_DEPOSE_PRIMARY = "continue_depose_primary";

  public ParallelWANConflationDistributedTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() throws Exception {
    IgnoredException.addIgnoredException("java.net.ConnectException");
  }

  @Test
  public void testParallelPropagationConflationDisabled() {
    initialSetUp();

    createSendersNoConflation();

    createSenderPRs();

    startPausedSenders();

    createReceiverPrs();

    final Map<Integer, Object> keyValues = putKeyValues();

    vm4.invoke(() -> checkQueueSize("ln", keyValues.size()));

    final Map<Integer, Object> updateKeyValues = updateKeyValues();

    vm4.invoke(() -> checkQueueSize("ln", (keyValues.size() + updateKeyValues.size())));

    vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), updateKeyValues));

    // Since no conflation, all updates are in queue
    vm4.invoke(() -> checkQueueSize("ln", keyValues.size() + 2 * updateKeyValues.size()));

    vm2.invoke(() -> validateRegionSize(getTestMethodName(), 0));

    resumeSenders();

    keyValues.putAll(updateKeyValues);
    validateReceiverRegionSize(keyValues);
  }

  /**
   * This test is disabled as it is not guaranteed to pass it everytime. This test is related to the
   * conflation in batch. yet did find any way to ascertain that the vents in the batch will always
   * be conflated.
   *
   */
  @Test
  public void testParallelPropagationBatchConflation() {
    initialSetUp();

    vm4.invoke(() -> createSender("ln", 2, true, 100, 50, false, false, null, true));
    vm5.invoke(() -> createSender("ln", 2, true, 100, 50, false, false, null, true));
    vm6.invoke(() -> createSender("ln", 2, true, 100, 50, false, false, null, true));
    vm7.invoke(() -> createSender("ln", 2, true, 100, 50, false, false, null, true));

    createSenderPRs(1);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    pauseSenders();

    createReceiverPrs();

    final Map<Integer, Integer> keyValues = new HashMap<>();

    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 10; j++) {
        keyValues.put(j, i);
      }
      vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), keyValues));
    }

    // sender did not turn on conflation, so queue size will be 100 (otherwise it will be 20)
    vm4.invoke(() -> checkQueueSize("ln", 100));
    vm4.invoke(() -> enableConflation("ln"));
    vm5.invoke(() -> enableConflation("ln"));
    vm6.invoke(() -> enableConflation("ln"));
    vm7.invoke(() -> enableConflation("ln"));

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", 100));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", 100));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", 100));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", 100));
      assertThat(v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10)).as(
          "Event in secondary queue").isEqualTo(100);
    });

    resumeSenders();

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

      assertThat(v4List.get(8) + v5List.get(8) + v6List.get(8) + v7List.get(8)).as(
          "No events conflated in batch").isGreaterThan(0);
    });

    verifySecondaryEventQueuesDrained();
    vm2.invoke(() -> validateRegionSize(getTestMethodName(), 10));
    validateEventsProcessedByPQRM(100, 1);
  }

  private void verifySecondaryEventQueuesDrained() {
    await().untilAsserted(() -> {
      assertThat(vm4.invoke(() -> getSecondaryQueueSizeInStats("ln"))).isZero();
      assertThat(vm5.invoke(() -> getSecondaryQueueSizeInStats("ln"))).isZero();
      assertThat(vm6.invoke(() -> getSecondaryQueueSizeInStats("ln"))).isZero();
      assertThat(vm7.invoke(() -> getSecondaryQueueSizeInStats("ln"))).isZero();
    });
  }

  @Test
  public void testParallelPropagationConflation() {
    doTestParallelPropagationConflation(0);
  }

  @Test
  public void testParallelPropagationConflationRedundancy2() {
    doTestParallelPropagationConflation(2);
  }

  public void doTestParallelPropagationConflation(int redundancy) {
    initialSetUp();

    createSendersWithConflation();

    createSenderPRs(redundancy);

    startPausedSenders();

    createReceiverPrs();

    final Map<Integer, Object> keyValues = putKeyValues();

    vm4.invoke(() -> checkQueueSize("ln", keyValues.size()));
    final Map<Integer, Object> updateKeyValues = updateKeyValues();

    vm4.invoke(() -> checkQueueSize("ln", keyValues.size() + updateKeyValues.size())); // creates
                                                                                       // aren't
                                                                                       // conflated
    validateSecondaryEventQueueSize(keyValues.size() + updateKeyValues.size(), redundancy);

    vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), updateKeyValues));

    int expectedEventNumAfterConflation = keyValues.size() + updateKeyValues.size();

    // ParallelQueueRemovalMessage will send for each event conflated at primary to secondary queues
    int totalEventsProcessedByPQRM = expectedEventNumAfterConflation + updateKeyValues.size();

    vm4.invoke(() -> checkQueueSize("ln", expectedEventNumAfterConflation));

    validateSecondaryEventQueueSize(expectedEventNumAfterConflation, redundancy);

    vm2.invoke(() -> validateRegionSize(getTestMethodName(), 0));

    resumeSenders();

    keyValues.putAll(updateKeyValues);
    validateReceiverRegionSize(keyValues);

    // after dispatch, both primary and secondary queues are empty
    vm4.invoke(() -> checkQueueSize("ln", 0));
    verifySecondaryEventQueuesDrained();
    validateSecondaryEventQueueSize(0, redundancy);
    validateEventsProcessedByPQRM(totalEventsProcessedByPQRM, redundancy);
  }

  private void validateSecondaryEventQueueSize(int expectedNum, int redundancy) {
    await().untilAsserted(() -> {
      List<Integer> vm4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", expectedNum));
      List<Integer> vm5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", expectedNum));
      List<Integer> vm6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", expectedNum));
      List<Integer> vm7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", expectedNum));
      assertThat(vm4List.get(10) + vm5List.get(10) + vm6List.get(10) + vm7List.get(10))
          .as("Event in secondary queue")
          .isEqualTo(expectedNum * redundancy);
    });
  }

  private void validateEventsProcessedByPQRM(int expectedNum, int redundancy) {
    await().untilAsserted(() -> {
      List<Integer> vm4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> vm5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> vm6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> vm7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      assertThat(vm4List.get(11) + vm5List.get(11) + vm6List.get(11) + vm7List.get(11))
          .as("Event processed by queue removal message")
          .isEqualTo(expectedNum * redundancy);
    });
  }

  @Test
  public void testParallelPropagationConflationOfRandomKeys() {
    initialSetUp();

    createSendersWithConflation();

    createSenderPRs();

    startPausedSenders();

    createReceiverPrs();

    final Map<Integer, Object> keyValues = putKeyValues();

    vm4.invoke(() -> checkQueueSize("ln", keyValues.size()));

    final Map<Integer, Object> updateKeyValues = new HashMap<>();
    while (updateKeyValues.size() != 10) {
      int key = (new Random()).nextInt(keyValues.size());
      updateKeyValues.put(key, key + "_updated");
    }
    vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), updateKeyValues));

    vm4.invoke(() -> checkQueueSize("ln", keyValues.size() + updateKeyValues.size()));

    vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), updateKeyValues));

    vm4.invoke(() -> checkQueueSize("ln", keyValues.size() + updateKeyValues.size()));

    vm2.invoke(() -> validateRegionSize(getTestMethodName(), 0));

    resumeSenders();


    keyValues.putAll(updateKeyValues);
    validateReceiverRegionSize(keyValues);

  }

  @Test
  public void testParallelPropagationColocatedRegionConflation() {
    initialSetUp();

    createSendersWithConflation();

    createOrderShipmentOnSenders();

    startPausedSenders();

    createOrderShipmentOnReceivers();

    Map<CustId, Customer> custKeyValues = vm4.invoke(() -> putCustomerPartitionedRegion(20));
    Map<OrderId, Order> orderKeyValues = vm4.invoke(() -> putOrderPartitionedRegion(20));
    Map<ShipmentId, Shipment> shipmentKeyValues =
        vm4.invoke(() -> putShipmentPartitionedRegion(20));

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln",
        (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues.size())));

    Map<CustId, Customer> updatedCustKeyValues =
        vm4.invoke(() -> updateCustomerPartitionedRegion(10));
    Map<OrderId, Order> updatedOrderKeyValues = vm4.invoke(() -> updateOrderPartitionedRegion(10));
    Map<ShipmentId, Shipment> updatedShipmentKeyValues =
        vm4.invoke(() -> updateShipmentPartitionedRegion(10));
    int sum = (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues.size())
        + updatedCustKeyValues.size() + updatedOrderKeyValues.size()
        + updatedShipmentKeyValues.size();
    vm4.invoke(() -> WANTestBase.checkQueueSize("ln", sum));


    updatedCustKeyValues = vm4.invoke(() -> updateCustomerPartitionedRegion(10));
    updatedOrderKeyValues = vm4.invoke(() -> updateOrderPartitionedRegion(10));
    updatedShipmentKeyValues = vm4.invoke(() -> updateShipmentPartitionedRegion(10));
    int sum2 = (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues.size())
        + updatedCustKeyValues.size() + updatedOrderKeyValues.size()
        + updatedShipmentKeyValues.size();
    vm4.invoke(() -> WANTestBase.checkQueueSize("ln", sum2));

    vm2.invoke(() -> validateRegionSize(WANTestBase.customerRegionName, 0));
    vm2.invoke(() -> validateRegionSize(WANTestBase.orderRegionName, 0));
    vm2.invoke(() -> validateRegionSize(WANTestBase.shipmentRegionName, 0));

    resumeSenders();

    custKeyValues.putAll(updatedCustKeyValues);
    orderKeyValues.putAll(updatedOrderKeyValues);
    shipmentKeyValues.putAll(updatedShipmentKeyValues);

    validateColocatedRegionContents(custKeyValues, orderKeyValues, shipmentKeyValues);

  }

  //
  // This is the same as the previous test, except for the UsingCustId methods
  @Test
  public void testParallelPropagationColocatedRegionConflationSameKey() {
    initialSetUp();

    createSendersWithConflation();

    createOrderShipmentOnSenders();

    startPausedSenders();

    createOrderShipmentOnReceivers();

    Map<CustId, Customer> custKeyValues = vm4.invoke(() -> putCustomerPartitionedRegion(20));
    Map<CustId, Order> orderKeyValues = vm4.invoke(() -> putOrderPartitionedRegionUsingCustId(20));
    Map<CustId, Shipment> shipmentKeyValues =
        vm4.invoke(() -> putShipmentPartitionedRegionUsingCustId(20));

    vm4.invoke(() -> checkQueueSize("ln",
        (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues.size())));

    Map<CustId, Customer> updatedCustKeyValues =
        vm4.invoke(() -> updateCustomerPartitionedRegion(10));
    Map<CustId, Order> updatedOrderKeyValues =
        vm4.invoke(() -> updateOrderPartitionedRegionUsingCustId(10));
    Map<CustId, Shipment> updatedShipmentKeyValues =
        vm4.invoke(() -> updateShipmentPartitionedRegionUsingCustId(10));
    int sum = (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues.size())
        + updatedCustKeyValues.size() + updatedOrderKeyValues.size()
        + updatedShipmentKeyValues.size();

    vm4.invoke(() -> checkQueueSize("ln", sum));

    updatedCustKeyValues = vm4.invoke(() -> updateCustomerPartitionedRegion(10));
    updatedOrderKeyValues = vm4.invoke(() -> updateOrderPartitionedRegionUsingCustId(10));
    updatedShipmentKeyValues =
        vm4.invoke(() -> updateShipmentPartitionedRegionUsingCustId(10));

    int sum2 = (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues.size())
        + updatedCustKeyValues.size() + updatedOrderKeyValues.size()
        + updatedShipmentKeyValues.size();
    vm4.invoke(() -> checkQueueSize("ln", sum2));

    vm2.invoke(() -> validateRegionSize(WANTestBase.customerRegionName, 0));
    vm2.invoke(() -> validateRegionSize(WANTestBase.orderRegionName, 0));
    vm2.invoke(() -> validateRegionSize(WANTestBase.shipmentRegionName, 0));

    resumeSenders();

    custKeyValues.putAll(updatedCustKeyValues);
    orderKeyValues.putAll(updatedOrderKeyValues);
    shipmentKeyValues.putAll(updatedShipmentKeyValues);

    validateColocatedRegionContents(custKeyValues, orderKeyValues, shipmentKeyValues);
  }

  @Test
  public void wanEventNotLostWhenDoOperationOnColocatedRegionsDuringRebalance() throws Exception {
    blackboard.initBlackboard();

    initialSetUp();
    int numberOfBuckets = 2;
    int numberOfPuts = 101;
    int newId = numberOfPuts + 1;

    createSendersNoConflation();

    Stream.of(vm4, vm5, vm6)
        .forEach(server -> server.invoke(this::addDistributionMessageObserver));

    vm4.invoke(
        () -> createCustomerOrderShipmentPartitionedRegion("ln", 1, numberOfBuckets, isOffHeap(),
            RegionShortcut.PARTITION_PROXY));
    vm5.invoke(
        () -> createCustomerOrderShipmentPartitionedRegion("ln", 1, numberOfBuckets, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6);
    vm2.invoke(
        () -> createCustomerOrderShipmentPartitionedRegion(null, 0, numberOfBuckets, isOffHeap()));
    vm3.invoke(
        () -> createCustomerOrderShipmentPartitionedRegion(null, 0, numberOfBuckets, isOffHeap()));

    vm4.invoke(() -> putCustomerPartitionedRegion(numberOfPuts));
    vm4.invoke(() -> putOrderPartitionedRegion(numberOfPuts));
    vm4.invoke(() -> putShipmentPartitionedRegion(numberOfPuts));

    vm6.invoke(
        () -> createCustomerOrderShipmentPartitionedRegion("ln", 1, numberOfBuckets, isOffHeap()));

    AsyncInvocation<?> asyncInvocation = vm4.invokeAsync(this::doRebalance);

    if (!blackboard.isGateSignaled(DEPOSE_PRIMARY_MESSAGE_RECEIVED)) {
      blackboard.waitForGate(DEPOSE_PRIMARY_MESSAGE_RECEIVED);
    }
    vm4.invokeAsync(() -> putInShipmentRegion(newId));

    if (!blackboard.isGateSignaled(CONTINUE_PUT_OP)) {
      blackboard.waitForGate(CONTINUE_PUT_OP);
    }
    vm4.invokeAsync(() -> putInCustomerRegion(newId));
    vm4.invokeAsync(() -> putInOrderRegion(newId));

    asyncInvocation.get();

    vm2.invoke(() -> verifyContent(newId));
    vm3.invoke(() -> verifyContent(newId));
  }

  private void addDistributionMessageObserver() {
    DistributionMessageObserver.setInstance(new TestDistributionMessageObserver());
  }

  private RebalanceResults doRebalance() throws Exception {
    ResourceManager manager = cache.getResourceManager();
    return manager.createRebalanceFactory().includeRegions(null).start().getResults();
  }

  private void putInCustomerRegion(int id) {
    CustId custid = new CustId(id);
    Customer customer = new Customer("name" + id, "Address" + id);
    customerRegion.put(custid, customer);
  }

  private void putInOrderRegion(int id) {
    CustId custid = new CustId(id);
    int oid = id + 1;
    OrderId orderId = new OrderId(oid, custid);
    Order order = new Order("ORDER" + oid);
    orderRegion.put(orderId, order);
  }

  private void putInShipmentRegion(int id) {
    CustId custid = new CustId(id);
    int oid = id + 1;
    OrderId orderId = new OrderId(oid, custid);
    int sid = oid + 1;
    ShipmentId shipmentId = new ShipmentId(sid, orderId);
    Shipment shipment = new Shipment("Shipment" + sid);
    shipmentRegion.put(shipmentId, shipment);
  }

  private void verifyContent(int id) {
    CustId custid = new CustId(id);
    int oid = id + 1;
    OrderId orderId = new OrderId(oid, custid);
    int sid = oid + 1;
    ShipmentId shipmentId = new ShipmentId(sid, orderId);

    await().untilAsserted(() -> assertThat(orderRegion.get(orderId)).isNotNull());
    await().untilAsserted(() -> assertThat(shipmentRegion.get(shipmentId)).isNotNull());
    await().untilAsserted(() -> assertThat(customerRegion.get(custid)).isNotNull());
  }

  protected void validateColocatedRegionContents(Map<?, ?> custKeyValues, Map<?, ?> orderKeyValues,
      Map<?, ?> shipmentKeyValues) {
    vm2.invoke(() -> validateRegionSize(WANTestBase.customerRegionName, custKeyValues.size()));
    vm2.invoke(() -> validateRegionSize(WANTestBase.orderRegionName, orderKeyValues.size()));
    vm2.invoke(() -> validateRegionSize(WANTestBase.shipmentRegionName, shipmentKeyValues.size()));

    vm2.invoke(() -> validateRegionContents(WANTestBase.customerRegionName, custKeyValues));
    vm2.invoke(() -> validateRegionContents(WANTestBase.orderRegionName, orderKeyValues));
    vm2.invoke(() -> validateRegionContents(WANTestBase.shipmentRegionName, shipmentKeyValues));

    vm3.invoke(() -> validateRegionSize(WANTestBase.customerRegionName, custKeyValues.size()));
    vm3.invoke(() -> validateRegionSize(WANTestBase.orderRegionName, orderKeyValues.size()));
    vm3.invoke(() -> validateRegionSize(WANTestBase.shipmentRegionName, shipmentKeyValues.size()));

    vm3.invoke(() -> validateRegionContents(WANTestBase.customerRegionName, custKeyValues));
    vm3.invoke(() -> validateRegionContents(WANTestBase.orderRegionName, orderKeyValues));
    vm3.invoke(() -> validateRegionContents(WANTestBase.shipmentRegionName, shipmentKeyValues));
  }

  protected void createOrderShipmentOnReceivers() {
    vm2.invoke(() -> createCustomerOrderShipmentPartitionedRegion(null, 1, 8, isOffHeap()));
    vm3.invoke(() -> createCustomerOrderShipmentPartitionedRegion(null, 1, 8, isOffHeap()));
  }

  protected void createOrderShipmentOnSenders() {
    vm4.invoke(() -> createCustomerOrderShipmentPartitionedRegion("ln", 0, 8, isOffHeap()));
    vm5.invoke(() -> createCustomerOrderShipmentPartitionedRegion("ln", 0, 8, isOffHeap()));
    vm6.invoke(() -> createCustomerOrderShipmentPartitionedRegion("ln", 0, 8, isOffHeap()));
    vm7.invoke(() -> createCustomerOrderShipmentPartitionedRegion("ln", 0, 8, isOffHeap()));
  }

  protected Map<Integer, Object> updateKeyValues() {
    final Map<Integer, Object> updateKeyValues = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), updateKeyValues));
    return updateKeyValues;
  }

  protected Map<Integer, Object> putKeyValues() {
    final Map<Integer, Object> keyValues = new HashMap<>();
    for (int i = 0; i < 20; i++) {
      keyValues.put(i, i);
    }


    vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), keyValues));
    return keyValues;
  }

  protected <K, V> void validateReceiverRegionSize(final Map<K, V> keyValues) {
    vm2.invoke(() -> validateRegionSize(getTestMethodName(), keyValues.size()));
    vm3.invoke(() -> validateRegionSize(getTestMethodName(), keyValues.size()));

    vm2.invoke(() -> validateRegionContents(getTestMethodName(), keyValues));
    vm3.invoke(() -> validateRegionContents(getTestMethodName(), keyValues));
  }

  protected void resumeSenders() {
    vm4.invoke(() -> resumeSender("ln"));
    vm5.invoke(() -> resumeSender("ln"));
    vm6.invoke(() -> resumeSender("ln"));
    vm7.invoke(() -> resumeSender("ln"));
  }

  protected void createReceiverPrs() {
    vm2.invoke(() -> createPartitionedRegion(getTestMethodName(), null, 1, 8, isOffHeap()));
    vm3.invoke(() -> createPartitionedRegion(getTestMethodName(), null, 1, 8, isOffHeap()));
  }

  protected void startPausedSenders() {
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    pauseSenders();
  }

  protected void pauseSenders() {
    vm4.invoke(() -> pauseSender("ln"));
    vm5.invoke(() -> pauseSender("ln"));
    vm6.invoke(() -> pauseSender("ln"));
    vm7.invoke(() -> pauseSender("ln"));
  }

  protected void createSenderPRs() {
    createSenderPRs(0);
  }

  protected void createSenderPRs(int redundancy) {
    vm4.invoke(
        () -> createPartitionedRegion(getTestMethodName(), "ln", redundancy, 8, isOffHeap()));
    vm5.invoke(
        () -> createPartitionedRegion(getTestMethodName(), "ln", redundancy, 8, isOffHeap()));
    vm6.invoke(
        () -> createPartitionedRegion(getTestMethodName(), "ln", redundancy, 8, isOffHeap()));
    vm7.invoke(
        () -> createPartitionedRegion(getTestMethodName(), "ln", redundancy, 8, isOffHeap()));
  }

  protected void initialSetUp() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
  }

  protected void createSendersNoConflation() {
    vm4.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
  }

  protected void createSendersWithConflation() {
    vm4.invoke(() -> createSender("ln", 2, true, 100, 2, true, false, null, true));
    vm5.invoke(() -> createSender("ln", 2, true, 100, 2, true, false, null, true));
    vm6.invoke(() -> createSender("ln", 2, true, 100, 2, true, false, null, true));
    vm7.invoke(() -> createSender("ln", 2, true, 100, 2, true, false, null, true));
  }

  private class TestDistributionMessageObserver extends DistributionMessageObserver {
    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof DeposePrimaryBucketMessage) {
        logger.info(
            "TestDistributionMessageObserver.beforeProcessMessage about to signal received_deposePrimaryMessage gate",
            new Exception());
        blackboard.signalGate(DEPOSE_PRIMARY_MESSAGE_RECEIVED);
        logger.info(
            "TestDistributionMessageObserver.beforeProcessMessage done signal received_deposePrimaryMessage gate");
      } else if (message instanceof DLockReleaseProcessor.DLockReleaseReplyMessage
          && blackboard.isGateSignaled(DEPOSE_PRIMARY_MESSAGE_RECEIVED)) {
        try {
          logger.info(
              "TestDistributionMessageObserver.beforeSendMessage about to signal on continue_put_op");
          blackboard.signalGate(CONTINUE_PUT_OP);

          logger.info(
              "TestDistributionMessageObserver.beforeSendMessage waits for signal on "
                  + CONTINUE_DEPOSE_PRIMARY);
          blackboard.waitForGate(CONTINUE_DEPOSE_PRIMARY);
          logger.info(
              "TestDistributionMessageObserver.beforeSendMessage done wait for signal on "
                  + CONTINUE_DEPOSE_PRIMARY);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (blackboard.isGateSignaled(DEPOSE_PRIMARY_MESSAGE_RECEIVED)) {
        if (message instanceof UpdateOperation.UpdateMessage) {
          logger.info(
              "TestDistributionMessageObserver.beforeSendMessage sending signal to stop wait on "
                  + CONTINUE_DEPOSE_PRIMARY);
          blackboard.signalGate(CONTINUE_DEPOSE_PRIMARY);
        }
      }
    }
  }
}
