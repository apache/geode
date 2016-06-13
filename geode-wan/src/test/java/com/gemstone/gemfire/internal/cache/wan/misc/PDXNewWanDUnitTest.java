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
package com.gemstone.gemfire.internal.cache.wan.misc;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Wait;

@Category(DistributedTest.class)
public class PDXNewWanDUnitTest extends WANTestBase{

  private static final long serialVersionUID = 1L;
  
  public PDXNewWanDUnitTest() {
    super();
  }

  /**
   * Test
   *   1> Site 1 : 1 locator, 1 member
   *   2> Site 2 : 1 locator, 1 member
   *   3> DR is defined on  member 1 on site1
   *   4> Serial GatewaySender is defined on member 1 on site1
   *   5> Same DR is defined on site2 member 1
   *   6> Put is done with value which is PDXSerializable
   *   7> Validate whether other sites member receive this put operation.    
   */
  @Test
  public void testWANPDX_RR_SerialSender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));
    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));

    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_RR",
        1 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_RR", 1 ));
  }
  
  /**
   * Test
   *   1> Site 1 : 1 locator, 1 member
   *   2> Site 2 : 1 locator, 1 member
   *   3> DR is defined on  member 1 on site1
   *   4> Serial GatewaySender is defined on member 1 on site1
   *   5> Same DR is defined on site2 member 1
   *   6> Put is done with value which is PDXSerializable
   *   7> Validate whether other sites member receive this put operation.
   *   8> Bounce site 1 and delete all of it's data
   *   9> Make sure that site 1 get the the PDX types along with entries
   *   and can deserialize entries.     
   */
  @Test
  public void testWANPDX_RemoveRomoteData() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX( nyPort ));

    vm3.invoke(() -> WANTestBase.createCache_PDX( lnPort ));

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));

    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_RR",
        1 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_RR", 1 ));
    
    
    //bounce vm2
    vm2.invoke(() -> WANTestBase.closeCache());
    
    vm2.invoke(() -> WANTestBase.deletePDXDir());
    
    vm2.invoke(() -> WANTestBase.createReceiver_PDX( nyPort ));
    
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", null, isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_RR",
      2 ));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
      getTestMethodName() + "_RR", 2 ));
  }
  
  /**
   * Test
   *   1> Site 1 : 1 locator, 1 member
   *   2> Site 2 : 1 locator, 1 member
   *   3> DR is defined on  member 1 on site1
   *   4> Serial GatewaySender is defined on member 1 on site1
   *   5> Same DR is defined on site2 member 1
   *   6> Put is done with value which is PDXSerializable
   *   7> Validate whether other sites member receive this put operation.
   *   8> Bounce site 1 and delete all of it's data
   *   9> Make some conflicting PDX registries in site 1 before the reconnect
   *   10> Make sure we flag a warning about the conflicting updates.     
   */
  @Test
  public void testWANPDX_ConflictingData() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX( nyPort ));

    vm3.invoke(() -> WANTestBase.createCache_PDX( lnPort ));

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));

    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_RR",
        1 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_RR", 1 ));
    
    //bounce vm3
    vm3.invoke(() -> WANTestBase.closeCache());
    
    IgnoredException ex1 = IgnoredException.addIgnoredException("Trying to add a PDXType with the same id");
    IgnoredException ex2 = IgnoredException.addIgnoredException("CacheWriterException");
    IgnoredException ex3 = IgnoredException.addIgnoredException("does match the existing PDX type");
    IgnoredException ex4 = IgnoredException.addIgnoredException("ServerOperationException");
    IgnoredException ex5 = IgnoredException.addIgnoredException("Stopping the processor");
    
    try {
    //blow away vm3's PDX data
    vm3.invoke(() -> WANTestBase.deletePDXDir());
    
    vm3.invoke(() -> WANTestBase.createCache_PDX( lnPort ));

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));

    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    
    //Define a different type from vm3
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable2( getTestMethodName() + "_RR",
      2 ));
    
    //Give the updates some time to make it over the WAN
    Wait.pause(10000);
    
    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
      getTestMethodName() + "_RR", 1 ));
    
    vm3.invoke(() -> WANTestBase.closeCache());
    } finally {
      ex1.remove();
      ex2.remove();
      ex3.remove();
      ex4.remove();
      ex5.remove();
    }
  }
  
  /**
   * Test
   *   1> Site 1 : 1 locator, 1 member
   *   2> Site 2 : 1 locator, 1 member
   *   3> Site 3 : 1 locator, 1 member
   *   3> DR is defined on  member 1 on site1
   *   4> Serial GatewaySender is defined on member 1 on site1
   *   5> Same DR is defined on site2 member 1
   *   6> Put is done with value which is PDXSerializable
   *   7> Validate whether other sites member receive this put operation.    
   */
  @Test
  public void testWANPDX_RR_SerialSender3Sites() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));
    
    Integer tkPort = (Integer)vm2.invoke(() -> WANTestBase.createFirstRemoteLocator( 3, lnPort ));

    createCacheInVMs(lnPort, vm3);
    createCacheInVMs(nyPort, vm4);
    createCacheInVMs(tkPort, vm5);
    vm3.invoke(() -> WANTestBase.createReceiver());
    vm4.invoke(() -> WANTestBase.createReceiver());
    vm5.invoke(() -> WANTestBase.createReceiver());


    //Create all of our gateway senders
    vm3.invoke(() -> WANTestBase.createSender( "ny", 2,
        false, 100, 10, false, false, null, true ));
    vm3.invoke(() -> WANTestBase.createSender( "tk", 3,
      false, 100, 10, false, false, null, true ));
    vm4.invoke(() -> WANTestBase.createSender( "ln", 1,
      false, 100, 10, false, false, null, true ));
    vm4.invoke(() -> WANTestBase.createSender( "tk", 3,
    false, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 1,
      false, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ny", 2,
    false, 100, 10, false, false, null, true ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", "ny,tk", isOffHeap() ));
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", "ln,tk", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", "ln,ny", isOffHeap() ));
    
    //Start all of the senders
    vm3.invoke(() -> WANTestBase.startSender( "ny" ));
    vm3.invoke(() -> WANTestBase.startSender( "tk" ));
    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm4.invoke(() -> WANTestBase.startSender( "tk" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ny" ));
    
    //Pause ln to ny. This means the PDX type will not be dispatched
    //to ny from ln
    vm3.invoke(() -> WANTestBase.pauseSender( "ny" ));
    
    Wait.pause(5000);
    
    //Do some puts that define a PDX type in ln
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_RR",
      1 ));
    
    //Make sure that tk received the update
    vm5.invoke(() -> WANTestBase.validateRegionSize_PDX(
      getTestMethodName() + "_RR", 1 ));
    
    //Make ny didn't receive the update because the sender is paused 
    vm4.invoke(() -> WANTestBase.validateRegionSize_PDX(
      getTestMethodName() + "_RR", 0 ));
    
    //Now, do a put from tk. This serialized object will be distributed
    //to ny from tk, using the type defined by ln.
    vm5.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_RR",
      2 ));
    
    //Verify the ny can read the object
    vm4.invoke(() -> WANTestBase.validateRegionSize_PDX(
      getTestMethodName() + "_RR", 2 ));
    
    //Wait for vm3 to receive the update (prevents a broken pipe suspect string) 
    vm3.invoke(() -> WANTestBase.validateRegionSize_PDX(
      getTestMethodName() + "_RR", 2 ));
  }
  
  @Test
  public void testWANPDX_RR_SerialSender_StartedLater() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_RR",
        10 ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_RR",
      40 ));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_RR", 40 ));
  }
  
  /**
   * Test
   *   1> Site 1 : 1 locator, 1 member
   *   2> Site 2 : 1 locator, 1 member
   *   3> PR is defined on  member 1 on site1
   *   4> Serial GatewaySender is defined on member 1 on site1
   *   5> Same PR is defined on site2 member 1
   *   6> Put is done with value which is PDXSerializable
   *   7> Validate whether other sites member receive this put operation.    
   */
  
  @Test
  public void testWANPDX_PR_SerialSender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 0, 2, isOffHeap() ));
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));


    vm3.invoke(() -> WANTestBase.startSender( "ln" ));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 0, 2, isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
        1 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_PR", 1 ));
  }
  
  @Test
  public void testWANPDX_PR_SerialSender_StartedLater() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX( nyPort ));

    vm3.invoke(() -> WANTestBase.createCache_PDX( lnPort ));

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 0, 2, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 0, 2, isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
        20 ));
    
    vm3.invoke(() -> WANTestBase.startSender( "ln" ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
      40 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_PR", 40 ));
  }
  
  /**
   * Test
   *   1> Site 1 : 1 locator, 2 member
   *   2> Site 2 : 1 locator, 2 member
   *   3> PR is defined on  member 1, 2 on site1
   *   4> Serial GatewaySender is defined on member 1 on site1
   *   5> Same PR is defined on site2 member 1, 2
   *   6> Put is done with value which is PDXSerializable
   *   7> Validate whether other sites member receive this put operation.    
   */
  
  @Test
  public void testWANPDX_PR_MultipleVM_SerialSender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null,1, 5, isOffHeap() ));
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3, vm4);

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 5, isOffHeap() ));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", "ln", 1, 5, isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
        10 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_PR", 10 ));
  }
  
  @Test
  public void testWANPDX_PR_MultipleVM_SerialSender_StartedLater() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX( nyPort ));

    vm3.invoke(() -> WANTestBase.createCache_PDX( lnPort ));
    vm4.invoke(() -> WANTestBase.createCache_PDX( lnPort ));
    
    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 5, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 5, isOffHeap() ));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", "ln", 1, 5, isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
        10 ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));
    
    vm4.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
      40 ));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_PR", 40 ));
  }
  
  /**
   * Test
   *   1> Site 1 : 1 locator, 1 member
   *   2> Site 2 : 1 locator, 1 member
   *   3> PR is defined on  member 1 on site1
   *   4> Parallel GatewaySender is defined on member 1 on site1
   *   5> Same PR is defined on site2 member 1
   *   6> Put is done with value which is PDXSerializable
   *   7> Validate whether other sites member receive this put operation.    
   */
  
  @Test
  public void testWANPDX_PR_ParallelSender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    vm3.invoke(() -> WANTestBase.createCache( lnPort ));

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 0, 1, isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", "ln", 0, 1, isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.startSender( "ln" ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
        1 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_PR", 1));
  }
  
  @Test
  public void testWANPDX_PR_ParallelSender_47826() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 0, 1, isOffHeap() ));
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2, true,
        100, 10, false, false, null, true ));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 0, 1, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(
        getTestMethodName() + "_PR", 1 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_PR", 1 ));
  }
  
  @Test
  public void testWANPDX_PR_ParallelSender_StartedLater() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX( nyPort ));

    vm3.invoke(() -> WANTestBase.createCache_PDX( lnPort ));

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 0, 2, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", "ln", 0, 2, isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
        10 ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
      40 ));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_PR", 40 ));
  }
  
  
  @Test
  public void testWANPDX_PR_MultipleVM_ParallelSender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3, vm4);

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));
    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 0, 2, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", "ln", 0, 2, isOffHeap() ));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", "ln", 0, 2, isOffHeap() ));

    startSenderInVMs("ln", vm3, vm4);

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
        10 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_PR", 10 ));
  }
  
  @Test
  public void testWANPDX_PR_MultipleVM_ParallelSender_StartedLater() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX( nyPort ));

    vm3.invoke(() -> WANTestBase.createCache_PDX( lnPort ));
    vm4.invoke(() -> WANTestBase.createCache_PDX( lnPort ));

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));
   vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 0, 2, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", "ln", 0, 2, isOffHeap() ));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", "ln", 0, 2, isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
        10 ));

    startSenderInVMsAsync("ln", vm3, vm4);

    vm4.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
      40 ));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_PR", 40 ));
  }
  
  
  @Test
  public void testWANPDX_RR_SerialSenderWithFilter() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, new PDXGatewayEventFilter(), true ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));

    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_RR",
        1 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_RR", 1 ));
    
    vm3.invoke(() -> PDXNewWanDUnitTest.verifyFilterInvocation( 1));
  }
  
  
  @Test
  public void testWANPDX_PR_MultipleVM_ParallelSenderWithFilter() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3, vm4);

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, new PDXGatewayEventFilter(), true ));
    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, new PDXGatewayEventFilter(), true ));
    
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 0, 2, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", "ln", 0, 2, isOffHeap() ));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", "ln", 0, 2, isOffHeap() ));

    startSenderInVMs("ln", vm3, vm4);

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
        10 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_PR", 10 ));
    
    vm3.invoke(() -> PDXNewWanDUnitTest.verifyFilterInvocation( 5));
    vm4.invoke(() -> PDXNewWanDUnitTest.verifyFilterInvocation( 5));
  }
  
  
  /**
   * When remote site bounces then we should send pdx event again.
   */
  
  public void Bug_testWANPDX_PR_SerialSender_RemoteSite_Bounce() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 0, 2, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 0, 2, isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
        1 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
        getTestMethodName() + "_PR", 1 ));
    
    vm2.invoke(() -> WANTestBase.killSender());

    createReceiverInVMs(vm2, vm4);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 2, isOffHeap() ));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 2, isOffHeap() ));
    
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable( getTestMethodName() + "_PR",
      1 ));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(
      getTestMethodName() + "_PR", 1 ));
  }
  

  
  
  public static void verifyFilterInvocation(int invocation) {
    assertEquals(((PDXGatewayEventFilter)eventFilter).beforeEnqueueInvoked, invocation);
    assertEquals(((PDXGatewayEventFilter)eventFilter).beforeTransmitInvoked, invocation);
    assertEquals(((PDXGatewayEventFilter)eventFilter).afterAckInvoked, invocation);
  }

  
  
}
