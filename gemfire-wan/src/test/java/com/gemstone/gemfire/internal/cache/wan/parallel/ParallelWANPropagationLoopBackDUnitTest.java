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
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;

public class ParallelWANPropagationLoopBackDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;
  
  public ParallelWANPropagationLoopBackDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
  }
  
  /**
   * Test loop back issue between 2 WAN sites (LN & NY). LN -> NY -> LN.
   * Site1 (LN): vm2, vm4, vm5
   * Site2 (NY): vm3, vm6, vm7
   */
  public void testParallelPropagationLoopBack() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    //create receiver on site1 and site2
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    //create cache on site1 and site2
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });

    //create senders on site1
    vm2.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    
    //create senders on site2
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 1,
      true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 1,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 1,
      true, 100, 10, false, false, null, true });
    
    //create PR on site1
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 100, isOffHeap()  });
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 100, isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 100, isOffHeap()  });
    
    //create PR on site2
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny", 0, 100, isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny", 0, 100, isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny", 0, 100, isOffHeap()  });
    
    //start sender on site1
    vm2.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    //start sender on site2
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    
    //pause senders on site1
    vm2.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    //pause senders on site2
    vm3.invoke(WANTestBase.class, "pauseSender", new Object[] { "ny" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ny" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ny" });
    
    //this is required since sender pause doesn't take effect immediately
    pause(1000);
    
    //Do 100 puts on site1
    vm2.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
      100 });
    //do next 100 puts on site2
    vm3.invoke(WANTestBase.class, "doPutsFrom", new Object[] { testName + "_PR",
      100, 200 });
    //verify queue size on both sites
    vm2.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln", 100 });
    vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln", 100 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln", 100 });
    
    vm3.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny", 100 });
    vm6.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny", 100 });
    vm7.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny", 100 });
    
    //resume sender on site1
    vm2.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
     
    //validate events reached site2 from site1
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 200 });
    
    //on site2, verify queue size again
    //this ensures that loopback is not happening since the queue size is same as before
    //the event coming from site1 are not enqueued again
    vm3.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny", 100 });
    vm6.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny", 100 });
    vm7.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny", 100 });
    
    //resume sender on site2
    vm3.invoke(WANTestBase.class, "resumeSender", new Object[] { "ny" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ny" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ny" });
    
    //validate region size on both the sites
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 200 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 200 });
  }
  
  /**
   * Test loop back issue among 3 WAN sites with Ring topology i.e. LN -> NY -> TK -> LN
   * Site1 (LN): vm3, vm6
   * Site2 (NY): vm4, vm7
   * Site3 (TK): vm5
   */
  public void testParallelPropagationLoopBack3Sites() {
    //Create locators
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });
    
    //create cache and receivers on all the 3 sites
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    
    //create senders on all the 3 sites
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 3,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 3,
      true, 100, 10, false, false, null, true });
    
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk", 1,
      true, 100, 10, false, false, null, true });
    
    //create PR on the 3 sites
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 100, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny", 0, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny", 0, 100, isOffHeap() });
    
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "tk", 0, 100, isOffHeap() });
    
    //start senders on all the sites 
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk" });
    
    //pause senders on site1 and site3. Site2 has the sender running to pass along events
    vm3.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "tk" });
    
    //need to have this pause since pauseSender doesn't take effect immediately
    pause(1000);
    
    //do puts on site1
    vm3.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
      100 });
    
    //do more puts on site3
    vm5.invoke(WANTestBase.class, "doPutsFrom", new Object[] { testName + "_PR",
      100, 200 });
    
    //verify queue size on site1 and site3
    vm3.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln", 100 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "tk", 100 });

    //resume sender on site1 so that events reach site2 and from there to site3
    vm3.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    //validate region size on site2 (should have 100) and site3 (should have 200)
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 100 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 200 });
    
    //verify queue size remains same on site3 which means event loopback did not happen
    //this means events coming from site1 are not enqueued back into the sender
    vm5.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "tk", 100 });
    
    //resume sender on site3
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "tk" });
    
    //validate region size
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 200 });
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 200 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 200 });
  }
  
  /**
   * Test loop back issue among 3 WAN sites with N to N topology 
   * i.e. each site connected to all other sites.
   * Puts are done to only one DS.
   * LN site: vm3, vm6
   * NY site: vm4, vm7
   * TK site: vm5
   */
  public void testParallelPropagationLoopBack3SitesNtoNTopologyPutFromOneDS() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });
    
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    
    //site1
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
      true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
      true, 100, 10, false, false, null, true });

    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
      true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
      true, 100, 10, false, false, null, true });
    
    //site2
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny2", 3,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny2", 3,
      true, 100, 10, false, false, null, true });

    //site3
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk1", 1,
      true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk2", 2,
      true, 100, 10, false, false, null, true });

    //create PR
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln1,ln2", 0, 1, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln1,ln2", 0, 1, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny1,ny2", 0, 1, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ny1,ny2", 0, 1, isOffHeap() });
    
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "tk1,tk2", 0, 1, isOffHeap() });

    //start all the senders
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny2" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny2" });
    
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk1" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk2" });
    
    //pause senders on all the sites
    vm3.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln1" });
    vm3.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln2" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln1" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln2" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ny1" });
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ny2" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ny1" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ny2" });
    
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "tk1" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "tk2" });
    
    //this is required since sender pause doesn't take effect immediately
    pause(1000);

    //do puts on site1
    vm3.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
      100 });
    
    //verify queue size on site1 and site3
    vm3.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln1", 100 });
    vm3.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln2", 100 });
    
    //resume sender (from site1 to site2) on site1
    vm3.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln1" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln1" });

    //validate region size on site2
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 100 });

    //verify queue size on site2 (sender 2 to 1)
    //should remain at 0 as the events from site1 should not go back to site1
    vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny1", 0 });
    
    //verify queue size on site2 (sender 2 to 3)
    //should remain at 0 as events from site1 will reach site3 directly..site2 need not send to site3 again  
    vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny2", 0 });
    
    //do more puts on site3
    vm5.invoke(WANTestBase.class, "doPutsFrom", new Object[] { testName + "_PR",
      100, 200 });
    
    //resume sender (from site3 to site2) on site3
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "tk2" });
    
    //validate region size on site2
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 200 });
    
    //verify queue size on site2 (sender 2 to 3)
    //should remain at 0 as the events from site3 should not go back to site3
    vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny2", 0 });
    
    //verify queue size on site2 (sender 2 to 1)
    //should remain at 0 as events from site3 will reach site1 directly..site2 need not send to site1 again  
    vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ny1", 0 });
    
    //resume all senders
    vm3.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln2" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln2" });
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ny1" });
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ny2" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ny1" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ny2" });
    
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "tk1" });
    
    //validate region size on all sites
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 200 });
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 200 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 200 });
  }
  


}
