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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ParallelWANConflationDUnitTest extends WANTestBase {
  private static final long serialVersionUID = 1L;

  public ParallelWANConflationDUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() throws Exception {
    IgnoredException.addIgnoredException("java.net.ConnectException");
  }

  @Test
  public void testParallelPropagationConflationDisabled() throws Exception {
    initialSetUp();

    createSendersNoConflation();

    createSenderPRs();

    startPausedSenders();

    createReceiverPrs();

    final Map keyValues = putKeyValues();

    vm4.invoke(() -> checkQueueSize("ln", keyValues.size()));

    final Map updateKeyValues = updateKeyValues();

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
  public void testParallelPropagationBatchConflation() throws Exception {
    initialSetUp();

    vm4.invoke(() -> createSender("ln", 2, true, 100, 50, false, false, null, true));
    vm5.invoke(() -> createSender("ln", 2, true, 100, 50, false, false, null, true));
    vm6.invoke(() -> createSender("ln", 2, true, 100, 50, false, false, null, true));
    vm7.invoke(() -> createSender("ln", 2, true, 100, 50, false, false, null, true));

    createSenderPRs(1);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    pauseSenders();

    createReceiverPrs();

    final Map keyValues = new HashMap();

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

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 100));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 100));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 100));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 100));
    assertTrue("Event in secondary queue should be 100",
        (v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10)) == 100);

    resumeSenders();

    v4List = (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    v5List = (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    v6List = (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    v7List = (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    assertTrue("No events conflated in batch",
        (v4List.get(8) + v5List.get(8) + v6List.get(8) + v7List.get(8)) > 0);

    verifySecondaryEventQueuesDrained("ln");
    vm2.invoke(() -> validateRegionSize(getTestMethodName(), 10));
    validateEventsProcessedByPQRM(100, 1);
  }

  private void verifySecondaryEventQueuesDrained(final String senderId) {
    await().untilAsserted(() -> {
      int vm4SecondarySize = vm4.invoke(() -> getSecondaryQueueSizeInStats("ln"));
      int vm5SecondarySize = vm5.invoke(() -> getSecondaryQueueSizeInStats("ln"));
      int vm6SecondarySize = vm6.invoke(() -> getSecondaryQueueSizeInStats("ln"));
      int vm7SecondarySize = vm7.invoke(() -> getSecondaryQueueSizeInStats("ln"));

      assertEquals(
          "Event in secondary queue should be 0 after dispatched, but actual is " + vm4SecondarySize
              + ":" + vm5SecondarySize + ":" + vm6SecondarySize + ":" + vm7SecondarySize,
          0, vm4SecondarySize + vm5SecondarySize + vm6SecondarySize + vm7SecondarySize);
    });
  }

  @Test
  public void testParallelPropagationConflation() throws Exception {
    doTestParallelPropagationConflation(0);
  }

  @Test
  public void testParallelPropagationConflationRedundancy2() throws Exception {
    doTestParallelPropagationConflation(2);
  }

  public void doTestParallelPropagationConflation(int redundancy) throws Exception {
    initialSetUp();

    createSendersWithConflation();

    createSenderPRs(redundancy);

    startPausedSenders();

    createReceiverPrs();

    final Map keyValues = putKeyValues();

    vm4.invoke(() -> checkQueueSize("ln", keyValues.size()));
    final Map updateKeyValues = updateKeyValues();

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
    verifySecondaryEventQueuesDrained("ln");
    validateSecondaryEventQueueSize(0, redundancy);
    validateEventsProcessedByPQRM(totalEventsProcessedByPQRM, redundancy);
  }

  private void validateSecondaryEventQueueSize(int expectedNum, int redundancy) {
    ArrayList<Integer> vm4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", expectedNum));
    ArrayList<Integer> vm5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", expectedNum));
    ArrayList<Integer> vm6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", expectedNum));
    ArrayList<Integer> vm7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", expectedNum));
    assertTrue(
        "Event in secondary queue should be " + (expectedNum * redundancy) + ", but is "
            + (vm4List.get(10) + vm5List.get(10) + vm6List.get(10) + vm7List.get(10)),
        (vm4List.get(10) + vm5List.get(10) + vm6List.get(10) + vm7List.get(10)) == expectedNum
            * redundancy);
  }

  private void validateEventsProcessedByPQRM(int expectedNum, int redundancy) {
    ArrayList<Integer> vm4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> vm5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> vm6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> vm7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    assertTrue(
        "Event processed by queue removal message should be " + (expectedNum * redundancy)
            + ", but is " + (vm4List.get(11) + vm5List.get(11) + vm6List.get(11) + vm7List.get(11)),
        (vm4List.get(11) + vm5List.get(11) + vm6List.get(11) + vm7List.get(11)) == expectedNum
            * redundancy);
  }

  @Test
  public void testParallelPropagationConflationOfRandomKeys() throws Exception {
    initialSetUp();

    createSendersWithConflation();

    createSenderPRs();

    startPausedSenders();

    createReceiverPrs();

    final Map keyValues = putKeyValues();

    vm4.invoke(() -> checkQueueSize("ln", keyValues.size()));

    final Map updateKeyValues = new HashMap();
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
  public void testParallelPropagationColocatedRegionConflation() throws Exception {
    initialSetUp();

    createSendersWithConflation();

    createOrderShipmentOnSenders();

    startPausedSenders();

    createOrderShipmentOnReceivers();

    Map custKeyValues = (Map) vm4.invoke(() -> putCustomerPartitionedRegion(20));
    Map orderKeyValues = (Map) vm4.invoke(() -> putOrderPartitionedRegion(20));
    Map shipmentKeyValues = (Map) vm4.invoke(() -> putShipmentPartitionedRegion(20));

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln",
        (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues.size())));

    Map updatedCustKeyValues = (Map) vm4.invoke(() -> updateCustomerPartitionedRegion(10));
    Map updatedOrderKeyValues = (Map) vm4.invoke(() -> updateOrderPartitionedRegion(10));
    Map updatedShipmentKeyValues = (Map) vm4.invoke(() -> updateShipmentPartitionedRegion(10));
    int sum = (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues.size())
        + updatedCustKeyValues.size() + updatedOrderKeyValues.size()
        + updatedShipmentKeyValues.size();
    vm4.invoke(() -> WANTestBase.checkQueueSize("ln", sum));


    updatedCustKeyValues = (Map) vm4.invoke(() -> updateCustomerPartitionedRegion(10));
    updatedOrderKeyValues = (Map) vm4.invoke(() -> updateOrderPartitionedRegion(10));
    updatedShipmentKeyValues = (Map) vm4.invoke(() -> updateShipmentPartitionedRegion(10));
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
  public void testParallelPropagationColocatedRegionConflationSameKey() throws Exception {
    initialSetUp();

    createSendersWithConflation();

    createOrderShipmentOnSenders();

    startPausedSenders();

    createOrderShipmentOnReceivers();

    Map custKeyValues = (Map) vm4.invoke(() -> putCustomerPartitionedRegion(20));
    Map orderKeyValues = (Map) vm4.invoke(() -> putOrderPartitionedRegionUsingCustId(20));
    Map shipmentKeyValues = (Map) vm4.invoke(() -> putShipmentPartitionedRegionUsingCustId(20));

    vm4.invoke(() -> checkQueueSize("ln",
        (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues.size())));

    Map updatedCustKeyValues = (Map) vm4.invoke(() -> updateCustomerPartitionedRegion(10));
    Map updatedOrderKeyValues = (Map) vm4.invoke(() -> updateOrderPartitionedRegionUsingCustId(10));
    Map updatedShipmentKeyValues =
        (Map) vm4.invoke(() -> updateShipmentPartitionedRegionUsingCustId(10));
    int sum = (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues.size())
        + updatedCustKeyValues.size() + updatedOrderKeyValues.size()
        + updatedShipmentKeyValues.size();

    vm4.invoke(() -> checkQueueSize("ln", sum));

    updatedCustKeyValues = (Map) vm4.invoke(() -> updateCustomerPartitionedRegion(10));
    updatedOrderKeyValues = (Map) vm4.invoke(() -> updateOrderPartitionedRegionUsingCustId(10));
    updatedShipmentKeyValues =
        (Map) vm4.invoke(() -> updateShipmentPartitionedRegionUsingCustId(10));

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

  protected void validateColocatedRegionContents(Map custKeyValues, Map orderKeyValues,
      Map shipmentKeyValues) {
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

  protected Map updateKeyValues() {
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 10; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), updateKeyValues));
    return updateKeyValues;
  }

  protected Map putKeyValues() {
    final Map keyValues = new HashMap();
    for (int i = 0; i < 20; i++) {
      keyValues.put(i, i);
    }


    vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), keyValues));
    return keyValues;
  }

  protected void validateReceiverRegionSize(final Map keyValues) {
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
    Integer lnPort = (Integer) vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

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

}
