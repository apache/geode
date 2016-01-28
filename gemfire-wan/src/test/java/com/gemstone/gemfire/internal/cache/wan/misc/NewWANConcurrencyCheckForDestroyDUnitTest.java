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

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.DistributedCacheOperation;
import com.gemstone.gemfire.internal.cache.EntrySnapshot;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion.NonTXEntry;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.Token.Tombstone;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;

/**
 * @author shobhit
 *
 * Test verifies that version tag for destroyed entry is propagated back to
 * origin distributed system if the version tag is applied and replaces old
 * version information in destination distributed system.
 *
 * Version tag information which is relevant between multiple distributed
 * systems consistency check is basically dsid and timestamp.
 */
public class NewWANConcurrencyCheckForDestroyDUnitTest extends WANTestBase {

  //These fields are used as BlackBoard for test data verification.
  static long destroyTimeStamp;
  static int destroyingMember;
  
  public NewWANConcurrencyCheckForDestroyDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testVersionTagTimestampForDestroy() {
    
    
    // create three distributed systems with each having a cache containing
    // a Replicated Region with one entry and concurrency checks enabled.

    // Site 2 and Site 3 only know about Site 1 but Site 1 knows about both
    // Site 2 and Site 3.

    // Site 1
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer lnRecPort = (Integer) vm1.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    
    //Site 2
    Integer nyPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer nyRecPort = (Integer) vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    //Site 3
    Integer tkPort = (Integer)vm4.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });
    Integer tkRecPort = (Integer) vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });

    getLogWriter().info("Created locators and receivers in 3 distributed systems");
     
    //Site 1
    vm1.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
      true, 10, 1, false, false, null, true });
    vm1.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
      true, 10, 1, false, false, null, true });
    
    vm1.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {"repRegion", "ln1,ln2", 0, 1, isOffHeap() });
    vm1.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm1.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
    vm1.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln1" });
    vm1.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln2" });
    
    //Site 2
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      true, 10, 1, false, false, null, true });
    
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {"repRegion", "ny1", 0, 1, isOffHeap() });
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm3.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ny1" });
    
    //Site 3 which only knows about Site 1.
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk1", 1,
      true, 10, 1, false, false, null, true });
    
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {"repRegion", "tk1", 0, 1, isOffHeap() });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk1" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "tk1" });
    
    pause(2000);
    
    // Perform a put in vm1
    vm1.invoke(new CacheSerializableRunnable("Putting an entry in ds1") {
      
      @Override
      public void run2() throws CacheException {
        assertNotNull(cache);
        
        Region region = cache.getRegion("/repRegion");
        region.put("testKey", "testValue");
        
        assertEquals(1, region.size());
      }
    });

    //wait for vm1 to propagate put to vm3 and vm5
    pause(2000); 

    destroyTimeStamp = (Long) vm3.invoke(NewWANConcurrencyCheckForDestroyDUnitTest.class, "getVersionTimestampAfterOp");
    
    //wait for vm1 to propagate destroyed entry's new version tag to vm5
    pause(2000); 

    vm5.invoke(NewWANConcurrencyCheckForDestroyDUnitTest.class, "verifyTimestampAfterOp", 
          new Object[] {destroyTimeStamp, 1 /* ds 3 receives gatway event only from ds 1*/});
  }

  /**
   * Test creates two sites and one Replicated Region on each with Serial
   * GatewaySender on each. Test checks for sequence of events being sent from
   * site1 to site2 for PUTALL and PUT and finally checks for final timestamp in
   * version for RegionEntry with key "testKey". If timestamp on both site is
   * same that means events were transferred in correct sequence.
   */
  public void testPutAllEventSequenceOnSerialGatewaySenderWithRR() {
    
    // create two distributed systems with each having a cache containing
    // a Replicated Region with one entry and concurrency checks enabled.

    // Site 1
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer lnRecPort = (Integer) vm1.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    
    //Site 2
    Integer nyPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer nyRecPort = (Integer) vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    getLogWriter().info("Created locators and receivers in 2 distributed systems");
     
    //Site 1
    vm1.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
      false, 10, 1, false, false, null, true });
    
    vm1.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {"repRegion", "ln1", isOffHeap() });
    vm1.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm1.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln1" });
    
    //Site 2
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      false, 10, 1, false, false, null, true });
    
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {"repRegion", "ny1", isOffHeap() });
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm3.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ny1" });
    
    pause(2000);
    
    // Perform a put in vm1
    AsyncInvocation asynch1 = vm1.invokeAsync(new CacheSerializableRunnable("Putting an entry in ds1") {
      
      @Override
      public void run2() throws CacheException {
        assertNotNull(cache);
        // Test hook to make put wait after RE lock is released but before Gateway events are sent.
        DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 2000; 
        
        Region region = cache.getRegion("/repRegion");
        Map testMap = new HashMap();
        testMap.put("testKey", "testValue1");
        region.putAll(testMap);
        
        assertEquals(1, region.size());
        assertEquals("testValue2", region.get("testKey"));
      }
    });

    //wait for vm1 to propagate put to vm3
    pause(1000); 

    AsyncInvocation asynch2 = vm1.invokeAsync(new CacheSerializableRunnable("Putting an entry in ds1") {
      
      @Override
      public void run2() throws CacheException {
        assertNotNull(cache);
        Region region = cache.getRegion("/repRegion");
        
        while (!region.containsKey("testKey")) {
          pause(10);
        }
        // Test hook to make put wait after RE lock is released but before Gateway events are sent.
        DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 0; 
        
        region.put("testKey", "testValue2");
        
        assertEquals(1, region.size());
        assertEquals("testValue2", region.get("testKey"));
      }
    });

    try {
      asynch1.join(5000);
      asynch2.join(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      vm1.invoke(new CacheSerializableRunnable("Reset Test Hook") {
        
        @Override
        public void run2() throws CacheException {
          DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 0;
        }
      });
    }

    //Wait for all Gateway events be received by vm3.
    pause(1000);

    long putAllTimeStampVm1 = (Long) vm1.invoke(NewWANConcurrencyCheckForDestroyDUnitTest.class, "getVersionTimestampAfterPutAllOp");
    
    long putAllTimeStampVm3 = (Long) vm3.invoke(NewWANConcurrencyCheckForDestroyDUnitTest.class, "getVersionTimestampAfterPutAllOp");
    
    assertEquals(putAllTimeStampVm1, putAllTimeStampVm3);
  }

/**
 * This is similar to above test but for PartitionedRegion.
 */
public void testPutAllEventSequenceOnSerialGatewaySenderWithPR() {
    
    // create two distributed systems with each having a cache containing
    // a Replicated Region with one entry and concurrency checks enabled.

    // Site 1
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer lnRecPort = (Integer) vm1.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    
    //Site 2
    Integer nyPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer nyRecPort = (Integer) vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    getLogWriter().info("Created locators and receivers in 2 distributed systems");
     
    //Site 1
    vm1.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
      false, 10, 1, false, false, null, true });
    
    vm1.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {"repRegion", "ln1", 0, 1, isOffHeap() });
    vm1.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm1.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln1" });
    
    //Site 2
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      false, 10, 1, false, false, null, true });
    
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {"repRegion", "ny1", 0, 1, isOffHeap() });
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm3.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ny1" });
    
    pause(2000);
    
    // Perform a put in vm1
    AsyncInvocation asynch1 = vm1.invokeAsync(new CacheSerializableRunnable("Putting an entry in ds1") {
      
      @Override
      public void run2() throws CacheException {
        assertNotNull(cache);
        // Test hook to make put wait after RE lock is released but before Gateway events are sent.
        DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 2000; 
        
        Region region = cache.getRegion("/repRegion");
        Map testMap = new HashMap();
        testMap.put("testKey", "testValue1");
        region.putAll(testMap);
        
        assertEquals(1, region.size());
        assertEquals("testValue2", region.get("testKey"));
      }
    });

    //wait for vm1 to propagate put to vm3
    pause(1000); 

    AsyncInvocation asynch2 = vm1.invokeAsync(new CacheSerializableRunnable("Putting an entry in ds1") {
      
      @Override
      public void run2() throws CacheException {
        assertNotNull(cache);
        Region region = cache.getRegion("/repRegion");
        
        while (!region.containsKey("testKey")) {
          pause(10);
        }
        // Test hook to make put wait after RE lock is released but before Gateway events are sent.
        DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 0; 
        
        region.put("testKey", "testValue2");
        
        assertEquals(1, region.size());
        assertEquals("testValue2", region.get("testKey"));
      }
    });

    try {
      asynch1.join(5000);
      asynch2.join(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      vm1.invoke(new CacheSerializableRunnable("Reset Test Hook") {
        
        @Override
        public void run2() throws CacheException {
          DistributedCacheOperation.SLOW_DISTRIBUTION_MS = 0;
        }
      });
    }

    //Wait for all Gateway events be received by vm3.
    pause(1000);

    long putAllTimeStampVm1 = (Long) vm1.invoke(NewWANConcurrencyCheckForDestroyDUnitTest.class, "getVersionTimestampAfterPutAllOp");
    
    long putAllTimeStampVm3 = (Long) vm3.invoke(NewWANConcurrencyCheckForDestroyDUnitTest.class, "getVersionTimestampAfterPutAllOp");
    
    assertEquals(putAllTimeStampVm1, putAllTimeStampVm3);
  }

  /**
   * Tests if conflict checks are happening based on DSID and timestamp even if
   * version tag is generated in local distributed system.
   */
  public void testConflicChecksBasedOnDsidAndTimeStamp() {

    
    // create two distributed systems with each having a cache containing
    // a Replicated Region with one entry and concurrency checks enabled.

    // Site 1
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer lnRecPort = (Integer) vm1.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    
    //Site 2
    Integer nyPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer nyRecPort = (Integer) vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    
    getLogWriter().info("Created locators and receivers in 2 distributed systems");

    //Site 1
    vm1.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
      false, 10, 1, false, false, null, true });
    
    vm1.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {"repRegion", "ln1", isOffHeap() });
    vm1.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm1.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln1" });
    
    //Site 2
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {"repRegion", "ny1", isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 1,
      false, 10, 1, false, false, null, true });
    
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {"repRegion", "ny1", isOffHeap() });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ny1" });
    
    pause(2000);
    
    // Perform a put in vm1
    vm1.invoke(new CacheSerializableRunnable("Putting an entry in ds1") {
      
      @Override
      public void run2() throws CacheException {
        assertNotNull(cache);
        
        Region region = cache.getRegion("/repRegion");
        region.put("testKey", "testValue1");
        
        assertEquals(1, region.size());
      }
    });

    //wait for vm4 to have later timestamp before sending operation to vm1
    pause(300); 

    AsyncInvocation asynch = vm4.invokeAsync(new CacheSerializableRunnable("Putting an entry in ds2 in vm4") {
      
      @Override
      public void run2() throws CacheException {
        assertNotNull(cache);
        Region region = cache.getRegion("/repRegion");
        
        region.put("testKey", "testValue2");
        
        assertEquals(1, region.size());
        assertEquals("testValue2", region.get("testKey"));
      }
    });

    try {
      asynch.join(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    //Wait for all local ds events be received by vm3.
    pause(1000);

    vm3.invoke(new CacheSerializableRunnable("Check dsid") {
      
      @Override
      public void run2() throws CacheException {
        Region region = cache.getRegion("repRegion");
        
        Region.Entry entry = ((LocalRegion)region).getEntry("testKey", /*null,*/
            true); //commented while merging revision 43582
        RegionEntry re = null;
        if (entry instanceof EntrySnapshot) {
          re = ((EntrySnapshot)entry).getRegionEntry();
        } else if (entry instanceof NonTXEntry) {
          re = ((NonTXEntry)entry).getRegionEntry();
        }
        VersionTag tag = re.getVersionStamp().asVersionTag();
        assertEquals(2, tag.getDistributedSystemId());
      }
    });

    // Check vm3 has latest timestamp from vm4.
    long putAllTimeStampVm1 = (Long) vm4.invoke(NewWANConcurrencyCheckForDestroyDUnitTest.class, "getVersionTimestampAfterPutAllOp");
    
    long putAllTimeStampVm3 = (Long) vm3.invoke(NewWANConcurrencyCheckForDestroyDUnitTest.class, "getVersionTimestampAfterPutAllOp");
    
    assertEquals(putAllTimeStampVm1, putAllTimeStampVm3);
  }
  
  /*
   * For VM1 in ds 1. Used in testPutAllEventSequenceOnSerialGatewaySender.
   */
  public static long getVersionTimestampAfterPutAllOp() {
    Region region = cache.getRegion("repRegion");
    
    while (!(region.containsKey("testKey") /*&& region.get("testKey").equals("testValue2") */)) {
      pause(10);
    }
    assertEquals(1, region.size());

    Region.Entry entry = ((LocalRegion)region).getEntry("testKey", /*null,*/ true);
    RegionEntry re = null;
    if (entry instanceof EntrySnapshot) {
      re = ((EntrySnapshot)entry).getRegionEntry();
    } else if (entry instanceof NonTXEntry) {
      re = ((NonTXEntry)entry).getRegionEntry();
    }
    if (re != null) {
      getLogWriter().fine("RegionEntry for testKey: " + re.getKey() + " " + re.getValueInVM((LocalRegion) region));
      
      VersionTag tag = re.getVersionStamp().asVersionTag();
      return tag.getVersionTimeStamp();
    } else {
      return -1;
    }
  }

  /*
   * For VM3 in ds 2.
   */
  public static long getVersionTimestampAfterOp() {
    Region region = cache.getRegion("repRegion");
    assertEquals(1, region.size());
    
    region.destroy("testKey");

    Region.Entry entry = ((LocalRegion)region).getEntry("testKey", /*null,*/ true);
    RegionEntry re = ((EntrySnapshot)entry).getRegionEntry();
    getLogWriter().fine("RegionEntry for testKey: " + re.getKey() + " " + re.getValueInVM((LocalRegion) region));
    assertTrue(re.getValueInVM((LocalRegion) region) instanceof Tombstone);
    
    VersionTag tag = re.getVersionStamp().asVersionTag();
    return tag.getVersionTimeStamp();
  }

  /*
   * For VM 5 in ds 3.
   */
  public static void verifyTimestampAfterOp(long timestamp, int memberid) {
    Region region = cache.getRegion("repRegion");
    assertEquals(0, region.size());

    Region.Entry entry = ((LocalRegion)region).getEntry("testKey", /*null,*/ true);
    RegionEntry re = ((EntrySnapshot)entry).getRegionEntry();
    assertTrue(re.getValueInVM((LocalRegion) region) instanceof Tombstone);
    
    VersionTag tag = re.getVersionStamp().asVersionTag();
    assertEquals(timestamp, tag.getVersionTimeStamp());
    assertEquals(memberid, tag.getDistributedSystemId());
  }
}
