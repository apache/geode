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
package com.gemstone.gemfire.internal.cache.wan.asyncqueue;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.wan.AsyncEventQueueTestBase;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class AsyncEventListenerDUnitTest extends AsyncEventQueueTestBase {

  public AsyncEventListenerDUnitTest() {
    super();
  }

  /**
   * Test to verify that AsyncEventQueue can not be created when null listener
   * is passed.
   */
  @Test
  public void testCreateAsyncEventQueueWithNullListener() {
    AsyncEventQueueTestBase test = new AsyncEventQueueTestBase();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    AsyncEventQueueFactory asyncQueueFactory = cache
        .createAsyncEventQueueFactory();
    try {
      asyncQueueFactory.create("testId", null);
      fail("AsyncQueueFactory should not allow to create AsyncEventQueue with null listener");
    }
    catch (IllegalArgumentException e) {
      // expected
    }

  }

  @Test
  public void testSerialAsyncEventQueueAttributes() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 150, true, true, "testDS", true ));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventQueueAttributes( "ln", 100, 150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true ));
  }
  
  @Test
  public void testSerialAsyncEventQueueSize() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(createAsyncEventQueueRunnable());
    vm2.invoke(createAsyncEventQueueRunnable());
    vm3.invoke(createAsyncEventQueueRunnable());
    vm4.invoke(createAsyncEventQueueRunnable());

    vm1.invoke(createReplicatedRegionRunnable());
    vm2.invoke(createReplicatedRegionRunnable());
    vm3.invoke(createReplicatedRegionRunnable());
    vm4.invoke(createReplicatedRegionRunnable());

    vm1
        .invoke(pauseAsyncEventQueueRunnable());
    vm2
        .invoke(pauseAsyncEventQueueRunnable());
    vm3
        .invoke(pauseAsyncEventQueueRunnable());
    vm4
        .invoke(pauseAsyncEventQueueRunnable());
    Wait.pause(1000);// pause at least for the batchTimeInterval

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));

    int vm1size = (Integer)vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventQueueSize( "ln" ));
    int vm2size = (Integer)vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventQueueSize( "ln" ));
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm1size);
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm2size);
  }

  @Test
  public void testParallelAsyncEventQueueWithFixedPartition() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
      true, 100, 100, false, false, null, false ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
      true, 100, 100, false, false, null, false ));

    List<String> allPartitions = Arrays.asList("part1", "part2");
    vm1.invoke(() -> AsyncEventQueueTestBase.createFixedPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", "part1", allPartitions, isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase.createFixedPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", "part2", allPartitions, isOffHeap()));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR",
      256 ));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));

    int vm1size = (Integer)vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm2size = (Integer)vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));

    assertEquals(vm1size + vm2size, 256);
  }

  protected SerializableRunnableIF pauseAsyncEventQueueRunnable() {
    return () -> AsyncEventQueueTestBase.pauseAsyncEventQueue( "ln" );
  }

  protected SerializableRunnableIF createReplicatedRegionRunnable() {
    return () -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue( getTestMethodName() + "_RR", "ln", isOffHeap() );
  }

  protected SerializableRunnableIF createAsyncEventQueueRunnable() {
    return () -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false );
  }

  protected SerializableRunnableIF createCacheRunnable(Integer lnPort) {
    return () -> AsyncEventQueueTestBase.createCache( lnPort );
  }
  
  /**
   * Added to reproduce defect #50366: 
   * NullPointerException with AsyncEventQueue#size() when number of dispatchers is more than 1
   */
  @Test
  public void testConcurrentSerialAsyncEventQueueSize() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 150, true, false, null, false, 2, OrderPolicy.KEY ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 150, true, false, null, false, 2, OrderPolicy.KEY ));

    vm1.invoke(createReplicatedRegionRunnable());
    vm2.invoke(createReplicatedRegionRunnable());
    vm3.invoke(createReplicatedRegionRunnable());
    vm4.invoke(createReplicatedRegionRunnable());

    vm1
      .invoke(pauseAsyncEventQueueRunnable());
    vm2
      .invoke(pauseAsyncEventQueueRunnable());

    Wait.pause(1000);// pause at least for the batchTimeInterval

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR",
      1000 ));

    int vm1size = (Integer)vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventQueueSize( "ln" ));
    int vm2size = (Integer)vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventQueueSize( "ln" ));
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm1size);
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm2size);
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false
   */
  @Test
  public void testReplicatedSerialAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(createAsyncEventQueueRunnable());
    vm2.invoke(createAsyncEventQueueRunnable());
    vm3.invoke(createAsyncEventQueueRunnable());
    vm4.invoke(createAsyncEventQueueRunnable());

    vm1.invoke(createReplicatedRegionRunnable());
    vm2.invoke(createReplicatedRegionRunnable());
    vm3.invoke(createReplicatedRegionRunnable());
    vm4.invoke(createReplicatedRegionRunnable());

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 1000 ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
  }
  
  /**
   * Verify that the events loaded by CacheLoader reach the AsyncEventListener
   * with correct operation detail (added for defect #50237).
   */
  @Test
  public void testReplicatedSerialAsyncEventQueueWithCacheLoader() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithCacheLoaderAndAsyncEventQueue( getTestMethodName() + "_RR", "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithCacheLoaderAndAsyncEventQueue( getTestMethodName() + "_RR", "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithCacheLoaderAndAsyncEventQueue( getTestMethodName() + "_RR", "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithCacheLoaderAndAsyncEventQueue( getTestMethodName() + "_RR", "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.doGets( getTestMethodName() + "_RR",
        10 ));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventForOperationDetail( "ln", 10, true, false ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventForOperationDetail( "ln", 0, true, false ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventForOperationDetail( "ln", 0, true, false ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventForOperationDetail( "ln", 0, true, false ));// secondary
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated 
   * WAN: Serial 
   * Region persistence enabled: false 
   * Async queue persistence enabled: false
   * 
   * Error is thrown from AsyncEventListener implementation while processing the batch.
   * Added to test the fix done for defect #45152.
   */
  @Test
  public void testReplicatedSerialAsyncEventQueue_ExceptionScenario() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithCustomListener( "ln",
        false, 100, 100, false, false, null, false, 1 ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithCustomListener( "ln",
        false, 100, 100, false, false, null, false, 1 ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithCustomListener( "ln",
        false, 100, 100, false, false, null, false, 1 ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithCustomListener( "ln",
        false, 100, 100, false, false, null, false, 1 ));

    vm1.invoke(createReplicatedRegionRunnable());
    vm2.invoke(createReplicatedRegionRunnable());
    vm3.invoke(createReplicatedRegionRunnable());
    vm4.invoke(createReplicatedRegionRunnable());
    
    vm1
        .invoke(pauseAsyncEventQueueRunnable());
    vm2
        .invoke(pauseAsyncEventQueueRunnable());
    vm3
        .invoke(pauseAsyncEventQueueRunnable());
    vm4
        .invoke(pauseAsyncEventQueueRunnable());
    Wait.pause(2000);// pause at least for the batchTimeInterval

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR",
        100 ));
    
    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateCustomAsyncEventListener( "ln", 100 ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateCustomAsyncEventListener( "ln", 0 ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateCustomAsyncEventListener( "ln", 0 ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateCustomAsyncEventListener( "ln", 0 ));// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false AsyncEventQueue conflation enabled: true
   */
  @Test
  public void testReplicatedSerialAsyncEventQueueWithConflationEnabled() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));

    vm1.invoke(createReplicatedRegionRunnable());
    vm2.invoke(createReplicatedRegionRunnable());
    vm3.invoke(createReplicatedRegionRunnable());
    vm4.invoke(createReplicatedRegionRunnable());

    vm1
        .invoke(pauseAsyncEventQueueRunnable());
    vm2
        .invoke(pauseAsyncEventQueueRunnable());
    vm3
        .invoke(pauseAsyncEventQueueRunnable());
    vm4
        .invoke(pauseAsyncEventQueueRunnable());
    Wait.pause(1000);// pause at least for the batchTimeInterval

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
        getTestMethodName() + "_RR", keyValues ));

    Wait.pause(1000);
    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize(
        "ln", keyValues.size() ));

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    // Put the update events and check the queue size.
    // There should be no conflation with the previous create events.
    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
        getTestMethodName() + "_RR", updateKeyValues ));

    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize(
        "ln", keyValues.size() + updateKeyValues.size() ));

    // Put the update events again and check the queue size.
    // There should be conflation with the previous update events.
    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
        getTestMethodName() + "_RR", updateKeyValues ));

    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize(
        "ln", keyValues.size() + updateKeyValues.size() ));

    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 1000 ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * event queue persistence enabled: false
   * 
   * Note: The test doesn't create a locator but uses MCAST port instead.
   */
  @Ignore("TODO: Disabled until I can sort out the hydra dependencies - see bug 52214")
  @Test
  public void testReplicatedSerialAsyncEventQueueWithoutLocator() {
    int mPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    vm1.invoke(() -> AsyncEventQueueTestBase.createCacheWithoutLocator( mPort ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCacheWithoutLocator( mPort ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createCacheWithoutLocator( mPort ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createCacheWithoutLocator( mPort ));

    vm1.invoke(createAsyncEventQueueRunnable());
    vm2.invoke(createAsyncEventQueueRunnable());
    vm3.invoke(createAsyncEventQueueRunnable());
    vm4.invoke(createAsyncEventQueueRunnable());

    vm1.invoke(createReplicatedRegionRunnable());
    vm2.invoke(createReplicatedRegionRunnable());
    vm3.invoke(createReplicatedRegionRunnable());
    vm4.invoke(createReplicatedRegionRunnable());

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 1000 ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * No VM is restarted.
   */

  @Test
  public void testReplicatedSerialAsyncEventQueueWithPeristenceEnabled() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));

    vm1.invoke(createReplicatedRegionRunnable());
    vm2.invoke(createReplicatedRegionRunnable());
    vm3.invoke(createReplicatedRegionRunnable());
    vm4.invoke(createReplicatedRegionRunnable());

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));
    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 1000 ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * There is only one vm in the site and that vm is restarted
   */
  @Ignore("TODO: Disabled for 52351")
  @Test
  public void testReplicatedSerialAsyncEventQueueWithPeristenceEnabled_Restart() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    String firstDStore = (String)vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithDiskStore( "ln", false, 100,
            100, true, null ));

    vm1.invoke(createReplicatedRegionRunnable());

    // pause async channel and then do the puts
    vm1
        .invoke(pauseAsyncEventQueueRunnable());
    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));

    // ------------------ KILL vm1 AND REBUILD
    // ------------------------------------------
    vm1.invoke(() -> AsyncEventQueueTestBase.killSender());

    vm1.invoke(createCacheRunnable(lnPort));
    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithDiskStore( "ln", false, 100, 100, true, firstDStore ));
    vm1.invoke(createReplicatedRegionRunnable());
    // -----------------------------------------------------------------------------------

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 1000 ));// primary sender
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * There are 3 VMs in the site and the VM with primary sender is shut down.
   */
  @Ignore("TODO: Disabled for 52351")
  @Test
  public void testReplicatedSerialAsyncEventQueueWithPeristenceEnabled_Restart2() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithDiskStore( "ln", false, 100, 100, true, null ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithDiskStore( "ln", false, 100, 100, true, null ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithDiskStore( "ln", false, 100, 100, true, null ));

    vm1.invoke(createReplicatedRegionRunnable());
    vm1.invoke(() -> AsyncEventQueueTestBase.addCacheListenerAndCloseCache( getTestMethodName() + "_RR" ));
    vm2.invoke(createReplicatedRegionRunnable());
    vm3.invoke(createReplicatedRegionRunnable());

    vm2.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR", 2000 ));

    // -----------------------------------------------------------------------------------
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForSenderToBecomePrimary( AsyncEventQueueImpl
            .getSenderIdFromAsyncEventQueueId("ln") ));
    
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));

    int vm1size = (Integer)vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln" ));
    int vm2size = (Integer)vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln" ));

    LogWriterUtils.getLogWriter().info("vm1 size is: " + vm1size);
    LogWriterUtils.getLogWriter().info("vm2 size is: " + vm2size);
    // verify that there is no event loss
    assertTrue(
        "Total number of entries in events map on vm1 and vm2 should be at least 2000",
        (vm1size + vm2size) >= 2000);
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated 
   * WAN: Serial 
   * Dispatcher threads: more than 1
   * Order policy: key based ordering
   */
  @Test
  public void testConcurrentSerialAsyncEventQueueWithReplicatedRegion() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY ));

    vm1.invoke(createReplicatedRegionRunnable());
    vm2.invoke(createReplicatedRegionRunnable());
    vm3.invoke(createReplicatedRegionRunnable());
    vm4.invoke(createReplicatedRegionRunnable());

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));
    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 1000 ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0 ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0 ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0 ));// secondary
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated 
   * WAN: Serial 
   * Region persistence enabled: false 
   * Async queue persistence enabled: false
   */
  @Test
  public void testConcurrentSerialAsyncEventQueueWithReplicatedRegion_2() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD ));

    vm1.invoke(createReplicatedRegionRunnable());
    vm2.invoke(createReplicatedRegionRunnable());
    vm3.invoke(createReplicatedRegionRunnable());
    vm4.invoke(createReplicatedRegionRunnable());

    vm1.invokeAsync(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR",
        500 ));
    vm1.invokeAsync(() -> AsyncEventQueueTestBase.doNextPuts( getTestMethodName() + "_RR",
      500, 1000 ));
    //Async invocation which was bound to fail
//    vm1.invokeAsync(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR",
//      1000, 1500 ));
    
    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 1000 ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0 ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0 ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0 ));// secondary
  }
  
  /**
   * Dispatcher threads set to more than 1 but no order policy set.
   * Added for defect #50514.
   */
  @Test
  public void testConcurrentSerialAsyncEventQueueWithoutOrderPolicy() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, null ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, null ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, null ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false, 3, null ));

    vm1.invoke(createReplicatedRegionRunnable());
    vm2.invoke(createReplicatedRegionRunnable());
    vm3.invoke(createReplicatedRegionRunnable());
    vm4.invoke(createReplicatedRegionRunnable());

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));
    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 1000 ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0 ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0 ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0 ));// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false
   */
  @Test
  public void testPartitionedSerialAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(createAsyncEventQueueRunnable());
    vm2.invoke(createAsyncEventQueueRunnable());
    vm3.invoke(createAsyncEventQueueRunnable());
    vm4.invoke(createAsyncEventQueueRunnable());

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR",
        500 ));
    vm2.invoke(() -> AsyncEventQueueTestBase.doPutsFrom(
        getTestMethodName() + "_PR", 500, 1000 ));
    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 1000 ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false AsyncEventQueue conflation enabled: true
   */
  @Test
  public void testPartitionedSerialAsyncEventQueueWithConflationEnabled() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, true, false, null, false ));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

    vm1
        .invoke(pauseAsyncEventQueueRunnable());
    vm2
        .invoke(pauseAsyncEventQueueRunnable());
    vm3
        .invoke(pauseAsyncEventQueueRunnable());
    vm4
        .invoke(pauseAsyncEventQueueRunnable());
    
    Wait.pause(2000);

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
        getTestMethodName() + "_PR", keyValues ));

    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize(
        "ln", keyValues.size() ));

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    // Put the update events and check the queue size.
    // There should be no conflation with the previous create events.
    vm2.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
        getTestMethodName() + "_PR", updateKeyValues ));

    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize(
        "ln", keyValues.size() + updateKeyValues.size() ));

    // Put the update events again and check the queue size.
    // There should be conflation with the previous update events.
    vm2.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
      getTestMethodName() + "_PR", updateKeyValues ));

    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize(
      "ln", keyValues.size() + updateKeyValues.size() ));

    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 1000 ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * No VM is restarted.
   */
  @Test
  public void testPartitionedSerialAsyncEventQueueWithPeristenceEnabled() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, true, null, false ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, true, null, false ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, true, null, false ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, true, null, false ));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR",
        500 ));
    vm2.invoke(() -> AsyncEventQueueTestBase.doPutsFrom(
        getTestMethodName() + "_PR", 500, 1000 ));
    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 1000 ));// primary sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * There is only one vm in the site and that vm is restarted
   */
  @Test
  public void testPartitionedSerialAsyncEventQueueWithPeristenceEnabled_Restart() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    String firstDStore = (String)vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithDiskStore( "ln", false, 100,
            100, true, null ));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

    // pause async channel and then do the puts
    vm1
        .invoke(() -> AsyncEventQueueTestBase.pauseAsyncEventQueueAndWaitForDispatcherToPause( "ln" ));
  
    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR",
        1000 ));

    // ------------------ KILL vm1 AND REBUILD
    // ------------------------------------------
    vm1.invoke(() -> AsyncEventQueueTestBase.killSender());

    vm1.invoke(createCacheRunnable(lnPort));
    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithDiskStore( "ln", false, 100, 100, true, firstDStore ));
    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    // -----------------------------------------------------------------------------------

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 1000 ));// primary sender
  }

  @Test
  public void testParallelAsyncEventQueueWithReplicatedRegion() {
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

      vm1.invoke(createCacheRunnable(lnPort));
      vm2.invoke(createCacheRunnable(lnPort));
      vm3.invoke(createCacheRunnable(lnPort));
      vm4.invoke(createCacheRunnable(lnPort));

      vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue(
          "ln", true, 100, 100, true, false, null, false ));
      vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue(
          "ln", true, 100, 100, true, false, null, false ));
      vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue(
          "ln", true, 100, 100, true, false, null, false ));
      vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue(
          "ln", true, 100, 100, true, false, null, false ));

      vm1.invoke(createReplicatedRegionRunnable());
      fail("Expected GatewaySenderConfigException where parallel async event queue can not be used with replicated region");
    }
    catch (Exception e) {
      if (!e.getCause().getMessage()
          .contains("can not be used with replicated region")) {
        fail("Expected GatewaySenderConfigException where parallel async event queue can not be used with replicated region");
      }
    }
  }

  @Test
  public void testParallelAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR",
        256 ));
    
    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    
    int vm1size = (Integer)vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm2size = (Integer)vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm3size = (Integer)vm3.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm4size = (Integer)vm4.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    
    assertEquals(vm1size + vm2size + vm3size + vm4size, 256);
  }

  @Test
  public void testParallelAsyncEventQueueWithSubstitutionFilter() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false, "MyAsyncEventListener", "MyGatewayEventSubstitutionFilter" ));

    String regionName = getTestMethodName() + "_PR";
    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( regionName, "ln", isOffHeap() ));

    int numPuts = 10;
    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( regionName, numPuts ));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));

    vm1.invoke(() -> verifySubstitutionFilterInvocations( "ln" ,numPuts ));
  }

  @Test
  public void testParallelAsyncEventQueueWithSubstitutionFilterNoSubstituteValueToDataInvocations() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false, "MyAsyncEventListener", "SizeableGatewayEventSubstitutionFilter" ));

    String regionName = getTestMethodName() + "_PR";
    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( regionName, "ln", isOffHeap() ));

    int numPuts = 10;
    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( regionName, numPuts ));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));

    vm1.invoke(() -> verifySubstitutionFilterToDataInvocations( "ln" ,0 ));
  }

  /**
   * Verify that the events reaching the AsyncEventListener have correct operation detail.
   * (added for defect #50237).
   */
  @Test
  public void testParallelAsyncEventQueueWithCacheLoader() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
    	true, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
    	true, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
    	true, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
    	true, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithCacheLoaderAndAsyncQueue( getTestMethodName() + "_PR", "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithCacheLoaderAndAsyncQueue( getTestMethodName() + "_PR", "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithCacheLoaderAndAsyncQueue( getTestMethodName() + "_PR", "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithCacheLoaderAndAsyncQueue( getTestMethodName() + "_PR", "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPutAll( getTestMethodName() + "_PR",
    	100, 10 ));
    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventForOperationDetail( "ln", 250, false, true ));
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventForOperationDetail( "ln", 250, false, true ));
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventForOperationDetail( "ln", 250, false, true ));
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventForOperationDetail( "ln", 250, false, true ));
  }
  
  @Test
  public void testParallelAsyncEventQueueSize() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

    vm1
        .invoke(pauseAsyncEventQueueRunnable());
    vm2
        .invoke(pauseAsyncEventQueueRunnable());
    vm3
        .invoke(pauseAsyncEventQueueRunnable());
    vm4
        .invoke(pauseAsyncEventQueueRunnable());
    Wait.pause(1000);// pause at least for the batchTimeInterval

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR",
        1000 ));

    int vm1size = (Integer)vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventQueueSize( "ln" ));
    int vm2size = (Integer)vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventQueueSize( "ln" ));
    
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm1size);
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm2size);
  }
  
  /**
   * Added to reproduce defect #50366: 
   * NullPointerException with AsyncEventQueue#size() when number of dispatchers is more than 1
   */
  @Test
  public void testConcurrentParallelAsyncEventQueueSize() {
	Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

	vm1.invoke(createCacheRunnable(lnPort));
	vm2.invoke(createCacheRunnable(lnPort));
	vm3.invoke(createCacheRunnable(lnPort));
	vm4.invoke(createCacheRunnable(lnPort));

	vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
	  true, 100, 100, false, false, null, false, 2, OrderPolicy.KEY ));
	vm2.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
	  true, 100, 100, false, false, null, false, 2, OrderPolicy.KEY ));
	vm3.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
	  true, 100, 100, false, false, null, false, 2, OrderPolicy.KEY ));
	vm4.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue( "ln",
	  true, 100, 100, false, false, null, false, 2, OrderPolicy.KEY ));

	vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
	vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
	vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
	vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

	vm1
	  .invoke(pauseAsyncEventQueueRunnable());
	vm2
	  .invoke(pauseAsyncEventQueueRunnable());
	vm3
	  .invoke(pauseAsyncEventQueueRunnable());
	vm4
	  .invoke(pauseAsyncEventQueueRunnable());
	Wait.pause(1000);// pause at least for the batchTimeInterval

	vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR",
	  1000 ));

	int vm1size = (Integer)vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventQueueSize( "ln" ));
	int vm2size = (Integer)vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventQueueSize( "ln" ));
	    
	assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm1size);
	assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm2size);
  }
  
  @Test
  public void testParallelAsyncEventQueueWithConflationEnabled() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, true, false, null, false ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, true, false, null, false ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, true, false, null, false ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, true, false, null, false ));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

    vm1
        .invoke(pauseAsyncEventQueueRunnable());
    vm2
        .invoke(pauseAsyncEventQueueRunnable());
    vm3
        .invoke(pauseAsyncEventQueueRunnable());
    vm4
        .invoke(pauseAsyncEventQueueRunnable());

    Wait.pause(2000);// pause for the batchTimeInterval to ensure that all the
    // senders are paused

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
        getTestMethodName() + "_PR", keyValues ));

    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize(
        "ln", keyValues.size() ));

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
        getTestMethodName() + "_PR", updateKeyValues ));

 
    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncEventQueueSize(
        "ln", keyValues.size() + updateKeyValues.size() )); // no conflation of creates

    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
        getTestMethodName() + "_PR", updateKeyValues ));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncEventQueueSize(
        "ln", keyValues.size() + updateKeyValues.size() )); // conflation of updates

    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    
    int vm1size = (Integer)vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm2size = (Integer)vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm3size = (Integer)vm3.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm4size = (Integer)vm4.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    
    assertEquals(vm1size + vm2size + vm3size + vm4size, keyValues.size());
  }

  /**
   * Added to reproduce defect #47213
   */
  @Test
  public void testParallelAsyncEventQueueWithConflationEnabled_bug47213() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, true, false, null, false ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, true, false, null, false ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, true, false, null, false ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, true, false, null, false ));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

    vm1.invoke(pauseAsyncEventQueueRunnable());
    vm2.invoke(pauseAsyncEventQueueRunnable());
    vm3.invoke(pauseAsyncEventQueueRunnable());
    vm4.invoke(pauseAsyncEventQueueRunnable());

    Wait.pause(2000);// pause for the batchTimeInterval to ensure that all the
    // senders are paused

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
        getTestMethodName() + "_PR", keyValues ));

    Wait.pause(2000);
    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize(
        "ln", keyValues.size() ));

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
        getTestMethodName() + "_PR", updateKeyValues ));

    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(
        getTestMethodName() + "_PR", updateKeyValues ));

    // pause to ensure that events have been conflated.
    Wait.pause(2000);
    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize(
        "ln", keyValues.size() + updateKeyValues.size() ));

    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    
    int vm1size = (Integer)vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm2size = (Integer)vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm3size = (Integer)vm3.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm4size = (Integer)vm4.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    
    assertEquals(vm1size + vm2size + vm3size + vm4size, keyValues.size());
    
  }

  @Test
  public void testParallelAsyncEventQueueWithOneAccessor() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionAccessorWithAsyncEventQueue(
            getTestMethodName() + "_PR", "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR",
        256 ));
    
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener( "ln", 0 ));
    
    int vm2size = (Integer)vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm3size = (Integer)vm3.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm4size = (Integer)vm4.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));

    assertEquals(vm2size + vm3size + vm4size, 256);

  }

  @Test
  public void testParallelAsyncEventQueueWithPersistence() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    vm3.invoke(createCacheRunnable(lnPort));
    vm4.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, true, null, false ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, true, null, false ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, true, null, false ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, true, null, false ));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR",
        256 ));
    
    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm4.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    
    int vm1size = (Integer)vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm2size = (Integer)vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm3size = (Integer)vm3.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm4size = (Integer)vm4.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln"));
    
    assertEquals(vm1size + vm2size + vm3size + vm4size, 256);
  }
  
  /**
   * Test case to test possibleDuplicates. vm1 & vm2 are hosting the PR. vm2 is
   * killed so the buckets hosted by it are shifted to vm1.
   */
  @Ignore("TODO: Disabled for 52349")
  @Test
  public void testParallelAsyncEventQueueHA_Scenario1() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));
    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    LogWriterUtils.getLogWriter().info("Created the cache");

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithListener2( "ln", true, 100, 5, false, null ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithListener2( "ln", true, 100, 5, false, null ));

    LogWriterUtils.getLogWriter().info("Created the AsyncEventQueue");

    vm1.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue(
            getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue(
            getTestMethodName() + "_PR", "ln", isOffHeap() ));

    LogWriterUtils.getLogWriter().info("Created PR with AsyncEventQueue");

    vm1
        .invoke(pauseAsyncEventQueueRunnable());
    vm2
        .invoke(pauseAsyncEventQueueRunnable());
    Wait.pause(1000);// pause for the batchTimeInterval to make sure the AsyncQueue
                // is paused

    LogWriterUtils.getLogWriter().info("Paused the AsyncEventQueue");

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR", 80 ));

    LogWriterUtils.getLogWriter().info("Done puts");

    Set<Integer> primaryBucketsvm2 = (Set<Integer>)vm2.invoke(() -> AsyncEventQueueTestBase.getAllPrimaryBucketsOnTheNode( getTestMethodName() + "_PR" ));

    LogWriterUtils.getLogWriter().info("Primary buckets on vm2: " + primaryBucketsvm2);
    // ---------------------------- Kill vm2 --------------------------
    vm2.invoke(() -> AsyncEventQueueTestBase.killSender());

    Wait.pause(1000);// give some time for rebalancing to happen
    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.verifyAsyncEventListenerForPossibleDuplicates( "ln",
            primaryBucketsvm2, 5 ));
  }

  /**
   * Test case to test possibleDuplicates. vm1 & vm2 are hosting the PR. vm2 is
   * killed and subsequently vm3 is brought up. Buckets are now rebalanced
   * between vm1 & vm3.
   */
  @Category(FlakyTest.class) // GEODE-688 & GEODE-713: random ports, thread sleeps, async actions
  @Test
  public void testParallelAsyncEventQueueHA_Scenario2() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    LogWriterUtils.getLogWriter().info("Created the cache");

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithListener2( "ln", true, 100, 5, false, null ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithListener2( "ln", true, 100, 5, false, null ));

    LogWriterUtils.getLogWriter().info("Created the AsyncEventQueue");

    vm1.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue(
            getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue(
            getTestMethodName() + "_PR", "ln", isOffHeap() ));

    LogWriterUtils.getLogWriter().info("Created PR with AsyncEventQueue");

    vm1
        .invoke(pauseAsyncEventQueueRunnable());
    vm2
        .invoke(pauseAsyncEventQueueRunnable());
    Wait.pause(1000);// pause for the batchTimeInterval to make sure the AsyncQueue
                // is paused

    LogWriterUtils.getLogWriter().info("Paused the AsyncEventQueue");

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR", 80 ));

    LogWriterUtils.getLogWriter().info("Done puts");

    Set<Integer> primaryBucketsvm2 = (Set<Integer>)vm2.invoke(() -> AsyncEventQueueTestBase.getAllPrimaryBucketsOnTheNode( getTestMethodName() + "_PR" ));

    LogWriterUtils.getLogWriter().info("Primary buckets on vm2: " + primaryBucketsvm2);
    // ---------------------------- Kill vm2 --------------------------
    vm2.invoke(() -> AsyncEventQueueTestBase.killSender());
    // ----------------------------------------------------------------

    // ---------------------------- start vm3 --------------------------
    vm3.invoke(createCacheRunnable(lnPort));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithListener2( "ln", true, 100, 5, false, null ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue(
            getTestMethodName() + "_PR", "ln", isOffHeap() ));

    // ------------------------------------------------------------------

    Wait.pause(1000);// give some time for rebalancing to happen
    Set<Integer> primaryBucketsvm3 = (Set<Integer>)vm3.invoke(() -> AsyncEventQueueTestBase.getAllPrimaryBucketsOnTheNode( getTestMethodName() + "_PR" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));

    vm3.invoke(() -> AsyncEventQueueTestBase.verifyAsyncEventListenerForPossibleDuplicates( "ln",
            primaryBucketsvm3, 5 ));
  }

  /**
   * Test case to test possibleDuplicates. vm1 & vm2 are hosting the PR. vm3 is
   * brought up and rebalancing is triggered so the buckets get balanced among
   * vm1, vm2 & vm3.
   */
  @Ignore("Depends on hydra code. See bug ")
  @Test
  public void testParallelAsyncEventQueueHA_Scenario3() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    LogWriterUtils.getLogWriter().info("Created the cache");

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithListener2( "ln", true, 100, 5, false, null ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithListener2( "ln", true, 100, 5, false, null ));

    LogWriterUtils.getLogWriter().info("Created the AsyncEventQueue");

    vm1.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue(
            getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue(
            getTestMethodName() + "_PR", "ln", isOffHeap() ));

    LogWriterUtils.getLogWriter().info("Created PR with AsyncEventQueue");

    vm1
        .invoke(pauseAsyncEventQueueRunnable());
    vm2
        .invoke(pauseAsyncEventQueueRunnable());
    Wait.pause(1000);// pause for the batchTimeInterval to make sure the AsyncQueue
                // is paused

    LogWriterUtils.getLogWriter().info("Paused the AsyncEventQueue");

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR", 80 ));

    LogWriterUtils.getLogWriter().info("Done puts");

    // ---------------------------- start vm3 --------------------------
    vm3.invoke(createCacheRunnable(lnPort));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueueWithListener2( "ln", true, 100, 5, false, null ));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPRWithRedundantCopyWithAsyncEventQueue(
            getTestMethodName() + "_PR", "ln", isOffHeap() ));

    // ------------------------------------------------------------------
    vm1.invoke(() -> AsyncEventQueueTestBase.doRebalance());

    Set<Integer> primaryBucketsvm3 = (Set<Integer>)vm3.invoke(() -> AsyncEventQueueTestBase.getAllPrimaryBucketsOnTheNode( getTestMethodName() + "_PR" ));
    LogWriterUtils.getLogWriter().info("Primary buckets on vm3: " + primaryBucketsvm3);
    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue( "ln" ));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));

    vm3.invoke(() -> AsyncEventQueueTestBase.verifyAsyncEventListenerForPossibleDuplicates( "ln",
            primaryBucketsvm3, 5 ));
  }
  
  /**
   * Added for defect #50364 Can't colocate region that has AEQ with a region that does not have that same AEQ
   */
  @Test
  public void testParallelAsyncEventQueueAttachedToChildRegionButNotToParentRegion() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    // create cache on node
    vm3.invoke(createCacheRunnable(lnPort));

    // create AsyncEventQueue on node
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln", true, 100, 10, false, false, null, true ));

    // create leader (parent) PR on node
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegion( getTestMethodName() + "PARENT_PR", null, 0, 100 ));
    String parentRegionFullPath = (String)vm3.invoke(() -> AsyncEventQueueTestBase.getRegionFullPath( getTestMethodName() + "PARENT_PR" ));

    // create colocated (child) PR on node
    vm3.invoke(() -> AsyncEventQueueTestBase.createColocatedPartitionedRegionWithAsyncEventQueue(
            getTestMethodName() + "CHILD_PR", "ln", 100, parentRegionFullPath ));

    // do puts in colocated (child) PR on node
    vm3.invoke(() -> AsyncEventQueueTestBase.doPuts(
        getTestMethodName() + "CHILD_PR", 1000 ));

    // wait for AsyncEventQueue to get empty on node
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));

    // verify the events in listener
    int vm3size = (Integer)vm3.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize( "ln" ));
    assertEquals(vm3size, 1000);
  }

  @Test
  public void testParallelAsyncEventQueueMoveBucketAndMoveItBackDuringDispatching() {
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));
    final DistributedMember member1 = vm1.invoke(() -> cache.getDistributedSystem().getDistributedMember());
    final DistributedMember member2 = vm2.invoke(() -> cache.getDistributedSystem().getDistributedMember());

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln",
        true, 100, 10, false, false, null, false, new BucketMovingAsyncEventListener(member2)));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));

    vm1.invoke(() -> AsyncEventQueueTestBase.pauseAsyncEventQueue("ln"));
    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts( getTestMethodName() + "_PR",
      113 ));

    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln",
      true, 100, 10, false, false, null, false, new BucketMovingAsyncEventListener(member1)));

    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR", "ln", isOffHeap() ));
    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue("ln"));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty( "ln" ));

    Set<Object> allKeys = new HashSet<Object>();
    allKeys.addAll(getKeysSeen(vm1, "ln"));
    allKeys.addAll(getKeysSeen(vm2, "ln"));

    final Set<Long> expectedKeys = LongStream.range(0, 113).mapToObj(Long::valueOf).collect(Collectors.toSet());
    assertEquals(expectedKeys, allKeys);

    assertTrue(getBucketMoved(vm1, "ln"));
    assertTrue(getBucketMoved(vm2, "ln"));
  }

  private static Set<Object> getKeysSeen(VM vm, String asyncEventQueueId) {
    return vm.invoke(() -> {
      final BucketMovingAsyncEventListener listener = (BucketMovingAsyncEventListener) getAsyncEventListener(asyncEventQueueId);
      return listener.keysSeen;
    });
  }

  private static boolean getBucketMoved(VM vm, String asyncEventQueueId) {
    return vm.invoke(() -> {
      final BucketMovingAsyncEventListener listener = (BucketMovingAsyncEventListener) getAsyncEventListener(asyncEventQueueId);
      return listener.moved;
    });
  }

  private static final class BucketMovingAsyncEventListener implements AsyncEventListener {
    private final DistributedMember destination;
    private boolean moved;
    private Set<Object> keysSeen = new HashSet<Object>();

    public BucketMovingAsyncEventListener(final DistributedMember destination) {
      this.destination = destination;
    }

    @Override public boolean processEvents(final List<AsyncEvent> events) {
      if(!moved) {

        AsyncEvent event1 = events.get(0);
        moveBucket(destination, event1.getKey());
        moved = true;
        return false;
      }

      events.stream().map(AsyncEvent::getKey).forEach(keysSeen::add);
      return true;
    }

    @Override public void close() {

    }
    private static void moveBucket(final DistributedMember destination, final Object key) {
      Region<Object, Object> region = cache.getRegion(getTestMethodName() + "_PR");
      DistributedMember source = cache.getDistributedSystem().getDistributedMember();
      PartitionRegionHelper.moveBucketByKey(region, source, destination, key);
    }
  }


}
