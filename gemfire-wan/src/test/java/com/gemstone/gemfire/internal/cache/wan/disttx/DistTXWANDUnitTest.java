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
package com.gemstone.gemfire.internal.cache.wan.disttx;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;

public class DistTXWANDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public DistTXWANDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    
    Invoke.invokeInEveryVM(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        System.setProperty("gemfire.log-level", LogWriterUtils.getDUnitLogLevel());
        return null;
      }
    }); 
  }
  

  /*
   * Disabled because it hangs with current implementation of notifying
   * adjunct receivers by sending DistTXAdjunctCommitMessage from primary at the
   * time of commit.
   */
  public void DISABLED_testPartitionedSerialPropagation_SenderSameAsCoordinator() throws Exception {

    
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.doDistTXPuts( getTestMethodName() + "_PR",
        50 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_PR", 50 ));
  }

  public void testPartitionedSerialPropagation_SenderNotSameAsCoordinator() throws Exception {

    
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    vm6.invoke(() -> WANTestBase.doDistTXPuts( getTestMethodName() + "_PR",
        50 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_PR", 50 ));
  }

  
  public void testPartitionedRegionParallelPropagation() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));
    vm6.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));
    vm7.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap()  ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap()  ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap()  ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap()  ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    vm7.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap()  ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap()  ));

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    vm4.invoke(() -> WANTestBase.doDistTXPuts( getTestMethodName() + "_PR",
        5 ));
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_PR", 5 ));
  }

  public void testDummy() {
  }
  
}

