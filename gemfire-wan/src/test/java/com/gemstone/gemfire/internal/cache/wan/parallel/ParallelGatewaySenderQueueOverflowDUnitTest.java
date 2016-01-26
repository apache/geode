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

import java.io.File;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.cache30.MyGatewayEventFilter1;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter1;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter2;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * DUnit for ParallelSenderQueue overflow operations.
 * 
 * @author pdeole
 *
 */
public class ParallelGatewaySenderQueueOverflowDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;
  
  public ParallelGatewaySenderQueueOverflowDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
  }
  
  public void testParallelSenderQueueEventsOverflow_NoDiskStoreSpecified() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSenderWithoutDiskStore", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSenderWithoutDiskStore", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSenderWithoutDiskStore", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSenderWithoutDiskStore", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    //give some time for the senders to pause
    pause(1000);
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });

    int numEventPuts = 50;
    vm4.invoke(WANTestBase.class, "doHeavyPuts", new Object[] { testName, numEventPuts });
    
    long numOvVm4 = (Long) vm4.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm5 = (Long) vm5.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm6 = (Long) vm6.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm7 = (Long) vm7.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    
    long numMemVm4 = (Long) vm4.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm5 = (Long) vm5.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm6 = (Long) vm6.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm7 = (Long) vm7.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    
    getLogWriter().info("Entries overflown to disk: " + numOvVm4 + "," + numOvVm5 + "," + numOvVm6 + "," + numOvVm7);
    getLogWriter().info("Entries in VM: " + numMemVm4 + "," + numMemVm5 + "," + numMemVm6 + "," + numMemVm7);
    
    long totalOverflown = numOvVm4 + numOvVm5 + numOvVm6 + numOvVm7; 
    //considering a memory limit of 40 MB, maximum of 40 events can be in memory. Rest should be on disk.
    assertTrue("Total number of entries overflown to disk should be at least greater than 55", (totalOverflown > 55));
    
    long totalInMemory = numMemVm4 + numMemVm5 + numMemVm6 + numMemVm7;
    //expected is twice the number of events put due to redundancy level of 1  
    assertEquals("Total number of entries on disk and in VM is incorrect", (numEventPuts*2), (totalOverflown + totalInMemory));
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] { testName, 50 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] { testName, 50 });
  }
  
  /**
   * Keep same max memory limit for all the VMs
   *   
   * @throws Exception
   */
  public void _testParallelSenderQueueEventsOverflow() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    //give some time for the senders to pause
    pause(1000);
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });

    int numEventPuts = 50;
    vm4.invoke(WANTestBase.class, "doHeavyPuts", new Object[] { testName, numEventPuts });
    
    long numOvVm4 = (Long) vm4.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm5 = (Long) vm5.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm6 = (Long) vm6.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm7 = (Long) vm7.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    
    long numMemVm4 = (Long) vm4.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm5 = (Long) vm5.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm6 = (Long) vm6.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm7 = (Long) vm7.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    
    getLogWriter().info("Entries overflown to disk: " + numOvVm4 + "," + numOvVm5 + "," + numOvVm6 + "," + numOvVm7);
    getLogWriter().info("Entries in VM: " + numMemVm4 + "," + numMemVm5 + "," + numMemVm6 + "," + numMemVm7);
    
    long totalOverflown = numOvVm4 + numOvVm5 + numOvVm6 + numOvVm7; 
    //considering a memory limit of 40 MB, maximum of 40 events can be in memory. Rest should be on disk.
    assertTrue("Total number of entries overflown to disk should be at least greater than 55", (totalOverflown > 55));
    
    long totalInMemory = numMemVm4 + numMemVm5 + numMemVm6 + numMemVm7;
    //expected is twice the number of events put due to redundancy level of 1  
    assertEquals("Total number of entries on disk and in VM is incorrect", (numEventPuts*2), (totalOverflown + totalInMemory));
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] { testName, 50 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] { testName, 50 });
  }

  /**
   * Set a different memory limit for each VM and make sure that all the VMs are utilized to
   * full extent of available memory.
   * 
   * @throws Exception
   */
  public void _testParallelSenderQueueEventsOverflow_2() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 5, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 5, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 20, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    //give some time for the senders to pause
    pause(1000);
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });

    int numEventPuts = 50;
    vm4.invoke(WANTestBase.class, "doHeavyPuts", new Object[] { testName, numEventPuts });
    
    long numOvVm4 = (Long) vm4.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm5 = (Long) vm5.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm6 = (Long) vm6.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm7 = (Long) vm7.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    
    long numMemVm4 = (Long) vm4.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm5 = (Long) vm5.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm6 = (Long) vm6.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm7 = (Long) vm7.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    
    getLogWriter().info("Entries overflown to disk: " + numOvVm4 + "," + numOvVm5 + "," + numOvVm6 + "," + numOvVm7);
    getLogWriter().info("Entries in VM: " + numMemVm4 + "," + numMemVm5 + "," + numMemVm6 + "," + numMemVm7);
    
    long totalOverflown = numOvVm4 + numOvVm5 + numOvVm6 + numOvVm7; 
    //considering a memory limit of 40 MB, maximum of 40 events can be in memory. Rest should be on disk.
    assertTrue("Total number of entries overflown to disk should be at least greater than 55", (totalOverflown > 55));
    
    long totalInMemory = numMemVm4 + numMemVm5 + numMemVm6 + numMemVm7;
    //expected is twice the number of events put due to redundancy level of 1  
    assertEquals("Total number of entries on disk and in VM is incorrect", (numEventPuts*2), (totalOverflown + totalInMemory));
    
    //assert the numbers for each VM
    assertTrue("Number of entries in memory VM4 is incorrect. Should be less than 10", (numMemVm4 < 10));
    assertTrue("Number of entries in memory VM5 is incorrect. Should be less than 5", (numMemVm5 < 5));
    assertTrue("Number of entries in memory VM6 is incorrect. Should be less than 5", (numMemVm6 < 5));
    assertTrue("Number of entries in memory VM7 is incorrect. Should be less than 20", (numMemVm7 < 20));
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] { testName, 50 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] { testName, 50 });
  }

  public void _testParallelSenderQueueNoEventsOverflow() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 10, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    //give some time for the senders to pause
    pause(1000);
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });

    int numEventPuts = 15;
    vm4.invoke(WANTestBase.class, "doHeavyPuts", new Object[] { testName, numEventPuts });
    
    long numOvVm4 = (Long) vm4.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm5 = (Long) vm5.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm6 = (Long) vm6.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    long numOvVm7 = (Long) vm7.invoke(WANTestBase.class, "getNumberOfEntriesOverflownToDisk", new Object[] { "ln" });
    
    long numMemVm4 = (Long) vm4.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm5 = (Long) vm5.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm6 = (Long) vm6.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    long numMemVm7 = (Long) vm7.invoke(WANTestBase.class, "getNumberOfEntriesInVM", new Object[] { "ln" });
    
    getLogWriter().info("Entries overflown to disk: " + numOvVm4 + "," + numOvVm5 + "," + numOvVm6 + "," + numOvVm7);
    getLogWriter().info("Entries in VM: " + numMemVm4 + "," + numMemVm5 + "," + numMemVm6 + "," + numMemVm7);
    
    long totalOverflown = numOvVm4 + numOvVm5 + numOvVm6 + numOvVm7; 
    //all 30 (considering redundant copies) events should accommodate in 40 MB space given to 4 senders
    assertEquals("Total number of entries overflown to disk is incorrect", 0, totalOverflown);
    
    long totalInMemory = numMemVm4 + numMemVm5 + numMemVm6 + numMemVm7;
    //expected is twice the number of events put due to redundancy level of 1  
    assertEquals("Total number of entries on disk and in VM is incorrect", (numEventPuts*2), (totalOverflown + totalInMemory));
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] { testName, 15 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] { testName, 15 });
  }
  
  /**
   * Test to validate that ParallelGatewaySenderQueue diskSynchronous attribute
   * when persistence of sender is enabled. 
   */
  public void _test_ValidateParallelGatewaySenderQueueAttributes_1() {
    Integer localLocPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    
    Integer remoteLocPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, localLocPort });
    
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + localLocPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);  

    File directory = new File("TKSender" + "_disk_"
        + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    directory.mkdir();
    File[] dirs1 = new File[] { directory };
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setDiskDirs(dirs1);
    DiskStore diskStore = dsf.create("FORNY");
    
    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setParallel(true);//set parallel to true
    fact.setBatchConflationEnabled(true);
    fact.setBatchSize(200);
    fact.setBatchTimeInterval(300);
    fact.setPersistenceEnabled(true);//enable the persistence
    fact.setDiskSynchronous(true);
    fact.setDiskStoreName("FORNY");
    fact.setMaximumQueueMemory(200);
    fact.setAlertThreshold(1200);
    GatewayEventFilter myeventfilter1 = new MyGatewayEventFilter1();
    fact.addGatewayEventFilter(myeventfilter1);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    fact.addGatewayTransportFilter(myStreamfilter2);
    final ExpectedException exTKSender = addExpectedException("Could not connect");
    try {
      GatewaySender sender1 = fact.create("TKSender", 2);

      AttributesFactory factory = new AttributesFactory();
      factory.addGatewaySenderId(sender1.getId());
      factory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      Region region = cache.createRegionFactory(factory.create()).create(
          "test_ValidateGatewaySenderAttributes");
      Set<GatewaySender> senders = cache.getGatewaySenders();
      assertEquals(senders.size(), 1);
      GatewaySender gatewaySender = senders.iterator().next();
      Set<RegionQueue> regionQueues = ((AbstractGatewaySender) gatewaySender)
          .getQueues();
      assertEquals(regionQueues.size(), 1);
      RegionQueue regionQueue = regionQueues.iterator().next();
      assertEquals(true, regionQueue.getRegion().getAttributes()
          .isDiskSynchronous());
    } finally {
      exTKSender.remove();
    }
  }
  
  /**
   * Test to validate that ParallelGatewaySenderQueue diskSynchronous attribute
   * when persistence of sender is not enabled. 
   */
  public void _test_ValidateParallelGatewaySenderQueueAttributes_2() {
    Integer localLocPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    
    Integer remoteLocPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, localLocPort });
    
    WANTestBase test = new WANTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + localLocPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);  

    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setParallel(true);//set parallel to true
    fact.setBatchConflationEnabled(true);
    fact.setBatchSize(200);
    fact.setBatchTimeInterval(300);
    fact.setPersistenceEnabled(false);//set persistence to false
    fact.setDiskSynchronous(true);
    fact.setMaximumQueueMemory(200);
    fact.setAlertThreshold(1200);
    GatewayEventFilter myeventfilter1 = new MyGatewayEventFilter1();
    fact.addGatewayEventFilter(myeventfilter1);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    fact.addGatewayTransportFilter(myStreamfilter2);
    final ExpectedException ex = addExpectedException("Could not connect");
    try {
      GatewaySender sender1 = fact.create("TKSender", 2);
      AttributesFactory factory = new AttributesFactory();
      factory.addGatewaySenderId(sender1.getId());
      factory.setDataPolicy(DataPolicy.PARTITION);
      Region region = cache.createRegionFactory(factory.create()).create(
          "test_ValidateGatewaySenderAttributes");
      Set<GatewaySender> senders = cache.getGatewaySenders();
      assertEquals(senders.size(), 1);
      GatewaySender gatewaySender = senders.iterator().next();
      Set<RegionQueue> regionQueues = ((AbstractGatewaySender) gatewaySender)
          .getQueues();
      assertEquals(regionQueues.size(), 1);
      RegionQueue regionQueue = regionQueues.iterator().next();
      assertEquals(false, regionQueue.getRegion().getAttributes()
          .isDiskSynchronous());
    } finally {
      ex.remove();
    }
  }


}
