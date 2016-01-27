/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.wan.parallel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;

/**
 * @author skumar
 * 
 */
public class ParallelWANConflationDUnitTest extends WANTestBase {
  private static final long serialVersionUID = 1L;

  public ParallelWANConflationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    addExpectedException("java.net.ConnectException");
  }

  public void testParallelPropagationConflationDisabled() throws Exception {
    initialSetUp();

    createSendersNoConflation();

    createSenderPRs();

    startPausedSenders();

    createReceiverPrs();

    final Map keyValues = putKeyValues();

    vm4.invoke(() ->checkQueueSize( "ln", keyValues.size() ));
    
    final Map updateKeyValues = updateKeyValues();

    vm4.invoke(() ->checkQueueSize( "ln", (keyValues.size() + updateKeyValues.size()) ));

    vm2.invoke(() ->validateRegionSize(
        testName, 0 ));

    resumeSenders();

    keyValues.putAll(updateKeyValues);
    validateReceiverRegionSize(keyValues);
    
  }

  /**
   * This test is desabled as it is not guaranteed to pass it everytime. This
   * test is related to the conflation in batch. yet did find any way to
   * ascertain that the vents in the batch will always be conflated.
   * 
   * @throws Exception
   */
  public void testParallelPropagationBatchConflation() throws Exception {
    initialSetUp();
    
    vm4.invoke(() ->createSender( "ln", 2,
        true, 100, 50, false, false, null, true ));
    vm5.invoke(() ->createSender( "ln", 2,
      true, 100, 50, false, false, null, true ));
    vm6.invoke(() ->createSender( "ln", 2,
      true, 100, 50, false, false, null, true ));
    vm7.invoke(() ->createSender( "ln", 2,
    true, 100, 50, false, false, null, true ));
  
    createSenderPRs();
    
    startSenders();
    
    pauseSenders();
    
    createReceiverPrs();

    final Map keyValues = new HashMap();
    
    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 10; j++) {
        keyValues.put(j, i) ;
      }
      vm4.invoke(() ->putGivenKeyValue(
        testName, keyValues ));
    }
    
    vm4.invoke(() ->enableConflation( "ln" ));
    vm5.invoke(() ->enableConflation( "ln" ));
    vm6.invoke(() ->enableConflation( "ln" ));
    vm7.invoke(() ->enableConflation( "ln" ));
    
    resumeSenders();
    
    ArrayList<Integer> v4List = (ArrayList<Integer>)vm4.invoke(() -> 
        WANTestBase.getSenderStats( "ln", 0 ));
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(() -> 
        WANTestBase.getSenderStats( "ln", 0 ));
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(() -> 
        WANTestBase.getSenderStats( "ln", 0 ));
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(() -> 
        WANTestBase.getSenderStats( "ln", 0 ));
    
    assertTrue("No events conflated in batch", (v4List.get(8) + v5List.get(8) + v6List.get(8) + v7List.get(8)) > 0);
    
    vm2.invoke(() ->validateRegionSize(
      testName, 10 ));

  }
  
  public void testParallelPropagationConflation() throws Exception {
    doTestParallelPropagationConflation(0);
  }
  
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

    vm4.invoke(() ->checkQueueSize( "ln", keyValues.size() ));
    final Map updateKeyValues = updateKeyValues();

    vm4.invoke(() ->checkQueueSize( "ln", keyValues.size() + updateKeyValues.size() )); // creates aren't conflated
    
    vm4.invoke(() ->putGivenKeyValue( testName, updateKeyValues ));

    vm4.invoke(() ->checkQueueSize( "ln", keyValues.size() + updateKeyValues.size() )); // creates aren't conflated

    vm2.invoke(() ->validateRegionSize(
        testName, 0 ));

    resumeSenders();

    keyValues.putAll(updateKeyValues);
    validateReceiverRegionSize(keyValues);
  }
  
  public void testParallelPropagationConflationOfRandomKeys() throws Exception {
    initialSetUp();

    createSendersWithConflation();

    createSenderPRs();

    startPausedSenders();

    createReceiverPrs();

    final Map keyValues = putKeyValues();

    vm4.invoke(() ->checkQueueSize( "ln", keyValues.size() ));
    
    final Map updateKeyValues = new HashMap();
    while(updateKeyValues.size()!=10) {
      int key = (new Random()).nextInt(keyValues.size());
      updateKeyValues.put(key, key+"_updated");
    }
    vm4.invoke(() ->putGivenKeyValue( testName, updateKeyValues ));

    vm4.invoke(() ->checkQueueSize( "ln", keyValues.size() + updateKeyValues.size() ));

    vm4.invoke(() ->putGivenKeyValue( testName, updateKeyValues ));

    vm4.invoke(() ->checkQueueSize( "ln", keyValues.size() + updateKeyValues.size() ));

    vm2.invoke(() ->validateRegionSize(
        testName, 0 ));

    resumeSenders();

    
    keyValues.putAll(updateKeyValues);
    validateReceiverRegionSize(keyValues);
    
  }
  
  public void testParallelPropagationColocatedRegionConflation()
      throws Exception {
    initialSetUp();

    createSendersWithConflation();

    createOrderShipmentOnSenders();

    startPausedSenders();

    createOrderShipmentOnReceivers();

    Map custKeyValues = (Map)vm4.invoke(() ->putCustomerPartitionedRegion( 20 ));
    Map orderKeyValues = (Map)vm4.invoke(() ->putOrderPartitionedRegion( 20 ));
    Map shipmentKeyValues = (Map)vm4.invoke(() ->putShipmentPartitionedRegion( 20 ));

    vm4.invoke(() -> 
        WANTestBase.checkQueueSize(
            "ln",
            (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
                .size()) ));

    Map updatedCustKeyValues = (Map)vm4.invoke(() ->updateCustomerPartitionedRegion( 10 ));
    Map updatedOrderKeyValues = (Map)vm4.invoke(() ->updateOrderPartitionedRegion( 10 ));
    Map updatedShipmentKeyValues = (Map)vm4.invoke(() ->updateShipmentPartitionedRegion( 10 ));
    int sum = (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
        .size())
        + updatedCustKeyValues.size()
        + updatedOrderKeyValues.size()
        + updatedShipmentKeyValues.size();
    vm4.invoke(() -> 
        WANTestBase.checkQueueSize(
            "ln",
            sum));

    
    updatedCustKeyValues = (Map)vm4.invoke(() ->updateCustomerPartitionedRegion( 10 ));
    updatedOrderKeyValues = (Map)vm4.invoke(() ->updateOrderPartitionedRegion( 10 ));
    updatedShipmentKeyValues = (Map)vm4.invoke(() ->updateShipmentPartitionedRegion( 10 ));
    int sum2 = (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
        .size())
        + updatedCustKeyValues.size()
        + updatedOrderKeyValues.size()
        + updatedShipmentKeyValues.size();
    vm4.invoke(() -> 
        WANTestBase.checkQueueSize(
            "ln",
            sum2));

    vm2.invoke(() ->validateRegionSize(
        WANTestBase.customerRegionName, 0 ));
    vm2.invoke(() ->validateRegionSize(
        WANTestBase.orderRegionName, 0 ));
    vm2.invoke(() ->validateRegionSize(
        WANTestBase.shipmentRegionName, 0 ));

    resumeSenders();
    
    custKeyValues.putAll(updatedCustKeyValues);
    orderKeyValues.putAll(updatedOrderKeyValues);
    shipmentKeyValues.putAll(updatedShipmentKeyValues);
    
    validateColocatedRegionContents(custKeyValues, orderKeyValues,
        shipmentKeyValues);
    
  }
  
  //
  //This is the same as the previous test, except for the UsingCustId methods
  public void testParallelPropagationColoatedRegionConflationSameKey()
      throws Exception {
    initialSetUp();

    createSendersWithConflation();

    createOrderShipmentOnSenders();

    startPausedSenders();

    createOrderShipmentOnReceivers();

    Map custKeyValues = (Map)vm4.invoke(() ->putCustomerPartitionedRegion( 20 ));
    Map orderKeyValues = (Map)vm4.invoke(() ->putOrderPartitionedRegionUsingCustId( 20 ));
    Map shipmentKeyValues = (Map)vm4.invoke(() ->putShipmentPartitionedRegionUsingCustId( 20 ));

    vm4.invoke(() ->checkQueueSize( "ln", (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
      .size()) ));

    Map updatedCustKeyValues = (Map)vm4.invoke(() ->updateCustomerPartitionedRegion( 10 ));
    Map updatedOrderKeyValues = (Map)vm4.invoke(() ->updateOrderPartitionedRegionUsingCustId( 10 ));
    Map updatedShipmentKeyValues = (Map)vm4.invoke(() ->updateShipmentPartitionedRegionUsingCustId( 10 ));
    int sum = (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
        .size()) + updatedCustKeyValues.size() + updatedOrderKeyValues.size() + updatedShipmentKeyValues.size() ;

    vm4.invoke(() ->checkQueueSize( "ln", sum));

    updatedCustKeyValues = (Map)vm4.invoke(() ->updateCustomerPartitionedRegion( 10 ));
    updatedOrderKeyValues = (Map)vm4.invoke(() ->updateOrderPartitionedRegionUsingCustId( 10 ));
    updatedShipmentKeyValues = (Map)vm4.invoke(() ->updateShipmentPartitionedRegionUsingCustId( 10 ));

    int sum2 = (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
        .size()) + updatedCustKeyValues.size() + updatedOrderKeyValues.size() + updatedShipmentKeyValues.size();
    vm4.invoke(() ->checkQueueSize( "ln", sum2));

    vm2.invoke(() ->validateRegionSize(
        WANTestBase.customerRegionName, 0 ));
    vm2.invoke(() ->validateRegionSize(
        WANTestBase.orderRegionName, 0 ));
    vm2.invoke(() ->validateRegionSize(
        WANTestBase.shipmentRegionName, 0 ));

    resumeSenders();

    custKeyValues.putAll(updatedCustKeyValues);
    orderKeyValues.putAll(updatedOrderKeyValues);
    shipmentKeyValues.putAll(updatedShipmentKeyValues);
    
    validateColocatedRegionContents(custKeyValues, orderKeyValues,
        shipmentKeyValues);
  }
  
  protected void validateColocatedRegionContents(Map custKeyValues,
      Map orderKeyValues, Map shipmentKeyValues) {
    vm2.invoke(() ->validateRegionSize(
        WANTestBase.customerRegionName, custKeyValues.size() ));
    vm2.invoke(() ->validateRegionSize(
        WANTestBase.orderRegionName, orderKeyValues.size() ));
    vm2.invoke(() ->validateRegionSize(
        WANTestBase.shipmentRegionName, shipmentKeyValues.size() ));

    vm2.invoke(() ->validateRegionContents(
        WANTestBase.customerRegionName, custKeyValues ));
    vm2.invoke(() ->validateRegionContents(
        WANTestBase.orderRegionName, orderKeyValues ));
    vm2.invoke(() ->validateRegionContents(
        WANTestBase.shipmentRegionName, shipmentKeyValues ));
    
    vm3.invoke(() ->validateRegionSize(
        WANTestBase.customerRegionName, custKeyValues.size() ));
    vm3.invoke(() ->validateRegionSize(
        WANTestBase.orderRegionName, orderKeyValues.size() ));
    vm3.invoke(() ->validateRegionSize(
        WANTestBase.shipmentRegionName, shipmentKeyValues.size() ));

    vm3.invoke(() ->validateRegionContents(
        WANTestBase.customerRegionName, custKeyValues ));
    vm3.invoke(() ->validateRegionContents(
        WANTestBase.orderRegionName, orderKeyValues ));
    vm3.invoke(() ->validateRegionContents(
        WANTestBase.shipmentRegionName, shipmentKeyValues ));
  }

  protected void createOrderShipmentOnReceivers() {
    vm2.invoke(() ->createCustomerOrderShipmentPartitionedRegion(
            testName, null, 1, 8, isOffHeap() ));
    vm3.invoke(() ->createCustomerOrderShipmentPartitionedRegion(
            testName, null, 1, 8, isOffHeap() ));
  }

  protected void createOrderShipmentOnSenders() {
    vm4.invoke(() ->createCustomerOrderShipmentPartitionedRegion(
            testName, "ln", 0, 8, isOffHeap() ));
    vm5.invoke(() ->createCustomerOrderShipmentPartitionedRegion(
            testName, "ln", 0, 8, isOffHeap() ));
    vm6.invoke(() ->createCustomerOrderShipmentPartitionedRegion(
            testName, "ln", 0, 8, isOffHeap() ));
    vm7.invoke(() ->createCustomerOrderShipmentPartitionedRegion(
            testName, "ln", 0, 8, isOffHeap() ));
  }
  
  protected Map updateKeyValues() {
    final Map updateKeyValues = new HashMap();
    for(int i=0;i<10;i++) {
      updateKeyValues.put(i, i+"_updated");
    }
    
    vm4.invoke(() ->putGivenKeyValue( testName, updateKeyValues ));
    return updateKeyValues;
  }

  protected Map putKeyValues() {
    final Map keyValues = new HashMap();
    for(int i=0; i< 20; i++) {
      keyValues.put(i, i);
    }
    
    
    vm4.invoke(() ->putGivenKeyValue( testName, keyValues ));
    return keyValues;
  }

  protected void validateReceiverRegionSize(final Map keyValues) {
    vm2.invoke(() ->validateRegionSize(
        testName, keyValues.size() ));
    vm3.invoke(() ->validateRegionSize(
      testName, keyValues.size() ));
    
    vm2.invoke(() ->validateRegionContents(
        testName, keyValues ));
    vm3.invoke(() ->validateRegionContents(
        testName, keyValues ));
  }

  protected void resumeSenders() {
    vm4.invoke(() ->resumeSender( "ln" ));
    vm5.invoke(() ->resumeSender( "ln" ));
    vm6.invoke(() ->resumeSender( "ln" ));
    vm7.invoke(() ->resumeSender( "ln" ));
  }

  protected void createReceiverPrs() {
    vm2.invoke(() ->createPartitionedRegion(
        testName, null, 1, 8, isOffHeap() ));
    vm3.invoke(() ->createPartitionedRegion(
        testName, null, 1, 8, isOffHeap() ));
  }

  protected void startPausedSenders() {
    startSenders();

    pauseSenders();
  }

  protected void pauseSenders() {
    vm4.invoke(() ->pauseSenderAndWaitForDispatcherToPause( "ln" ));
    vm5.invoke(() ->pauseSenderAndWaitForDispatcherToPause( "ln" ));
    vm6.invoke(() ->pauseSenderAndWaitForDispatcherToPause( "ln" ));
    vm7.invoke(() ->pauseSenderAndWaitForDispatcherToPause( "ln" ));
  }

  protected void startSenders() {
    vm4.invoke(() ->startSender( "ln" ));
    vm5.invoke(() ->startSender( "ln" ));
    vm6.invoke(() ->startSender( "ln" ));
    vm7.invoke(() ->startSender( "ln" ));
  }
  
  protected void createSenderPRs() {
    createSenderPRs(0);
  }

  protected void createSenderPRs(int redundancy) {
    vm4.invoke(() ->createPartitionedRegion(
        testName, "ln", redundancy, 8, isOffHeap() ));
    vm5.invoke(() ->createPartitionedRegion(
        testName, "ln", redundancy, 8, isOffHeap() ));
    vm6.invoke(() ->createPartitionedRegion(
        testName, "ln", redundancy, 8, isOffHeap() ));
    vm7.invoke(() ->createPartitionedRegion(
        testName, "ln", redundancy, 8, isOffHeap() ));
  }

  protected void initialSetUp() {
    Integer lnPort = (Integer)vm0.invoke(() ->createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() ->createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() ->createReceiver( nyPort ));
    vm3.invoke(() ->createReceiver( nyPort ));

    vm4.invoke(() ->createCache(lnPort ));
    vm5.invoke(() ->createCache(lnPort ));
    vm6.invoke(() ->createCache(lnPort ));
    vm7.invoke(() ->createCache(lnPort ));
  }
  
  protected void createSendersNoConflation() {
    vm4.invoke(() ->createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));
    vm5.invoke(() ->createSender( "ln", 2,
        true, 100, 10, false, false, null, true  ));
    vm6.invoke(() ->createSender( "ln", 2,
        true, 100, 10, false, false, null, true  ));
    vm7.invoke(() ->createSender( "ln", 2,
        true, 100, 10, false, false, null, true  ));
  }
  
  protected void createSendersWithConflation() {
    vm4.invoke(() ->createSender( "ln", 2,
        true, 100, 2, true, false, null, true ));
    vm5.invoke(() ->createSender( "ln", 2,
        true, 100, 2, true, false, null, true ));
    vm6.invoke(() ->createSender( "ln", 2,
        true, 100, 2, true, false, null, true ));
    vm7.invoke(() ->createSender( "ln", 2,
        true, 100, 2, true, false, null, true ));
  }
  
}
