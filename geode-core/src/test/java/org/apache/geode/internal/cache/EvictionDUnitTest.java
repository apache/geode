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
package org.apache.geode.internal.cache;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.internal.cache.lru.HeapEvictor;
import org.apache.geode.internal.cache.lru.MemLRUCapacityController;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@Ignore("TODO: Test was disabled by renaming to DisabledTest")
public class EvictionDUnitTest extends EvictionTestBase {

  @Test
  public void testDummyInlineNCentralizedEviction() {
    prepareScenario1(EvictionAlgorithm.LRU_HEAP,0);
    putData("PR1", 50, 1);
    
    final int expectedEviction1 = getExpectedEvictionRatioOnVm(dataStore1);
    final int expectedEviction2 = getExpectedEvictionRatioOnVm(dataStore2);
    
    raiseFakeNotification(dataStore1, "PR1", expectedEviction1);
    raiseFakeNotification(dataStore2, "PR1", expectedEviction2);
    validateNoOfEvictions("PR1", expectedEviction1 + expectedEviction2);

    putData("PR1", 4, 1);
    validateNoOfEvictions("PR1", 4 + expectedEviction1 + expectedEviction2);
  }
  
  @Test
  public void testThreadPoolSize() {
    prepareScenario1(EvictionAlgorithm.LRU_HEAP,0);
    putData("PR1", 50, 1);
    raiseFakeNotification(dataStore1, "PR1", getExpectedEvictionRatioOnVm(dataStore1));
    verifyThreadPoolTaskCount(HeapEvictor.MAX_EVICTOR_THREADS);
  }
  
  @Test
  public void testCentralizedEvictionnForDistributedRegionWithDummyEvent() {
    prepareScenario1(EvictionAlgorithm.LRU_HEAP,0);
    createDistributedRegion();
    putDataInDistributedRegion(50, 1);
    raiseFakeNotification(dataStore1, "DR1", getExpectedEvictionRatioOnVm(dataStore1));
  }

  /**
   * Test Case Description: 2 VM's. 2 PR's. 4 buckets each PR. PR1 has action
   * -Local destroy and PR2 has action - Overflow To Disk.
   * 
   * Test Case verifies:If naturally Eviction up and eviction Down events are
   * raised. Centralized and Inline eviction are happening.All this verificatio
   * is done thorugh logs. It also verifies that during eviction, if one node
   * goes down and then comes up again causing GII to take place, the system
   * doesnot throw an OOME.
   */
  @Test
  public void testEvictionWithNodeDown() {
    prepareScenario2(EvictionAlgorithm.LRU_HEAP, "PR3", "PR4");
    putDataInDataStore3("PR3", 100, 1);
    fakeNotification();
    print("PR3");
    killVm();
    bringVMBackToLife();
    assertEquals(100, getPRSize("PR3"));
    assertEquals(0, getPRSize("PR4"));
  }
  
  @Test
  public void testEntryLruEvictions() {
    int extraEntries=1;
    createCache();
    maxEnteries=3;
    createPartitionedRegion(true, EvictionAlgorithm.LRU_ENTRY, "PR1", 4, 1, 1000,maxEnteries);
    
    final PartitionedRegion pr = (PartitionedRegion)cache.getRegion("PR1");
    LogWriterUtils.getLogWriter().info(
        "PR- " +pr.getEvictionAttributes().getMaximum());
    
    for (int counter = 1; counter <= maxEnteries+extraEntries; counter++) {
      pr.put(new Integer(counter), new byte[1 * 1024 * 1024]);
    }
     
    assertEquals(extraEntries,((AbstractLRURegionMap)pr.entries)._getLruList().stats().getEvictions());
  }

  @Test
  public void testEntryLru() {
    createCache();
    maxEnteries=12;
    createPartitionedRegion(true, EvictionAlgorithm.LRU_ENTRY, "PR1", 4, 1, 1000,maxEnteries);
    
    final PartitionedRegion pr = (PartitionedRegion)cache.getRegion("PR1");
    LogWriterUtils.getLogWriter().info(
        "PR- " +pr.getEvictionAttributes().getMaximum());
    for (int i = 0; i < 3; i++) {
      // assume mod-based hashing for bucket creation
      pr.put(new Integer(i), "value0");
      pr.put(new Integer(i
          + pr.getPartitionAttributes().getTotalNumBuckets()), "value1");
      pr.put(new Integer(i
          + (pr.getPartitionAttributes().getTotalNumBuckets()) * 2),
          "value2");
    }
    pr.put(new Integer(3), "value0");
    
    for (int i = 0; i < 2; i++) {
      pr.put(new Integer(i
          + pr.getPartitionAttributes().getTotalNumBuckets())*3, "value1");
    }
   assertEquals(0,((AbstractLRURegionMap)pr.entries)._getLruList().stats().getEvictions());
  }

  @Test
  public void testCheckEntryLruEvictionsIn1DataStore() {
    int extraEntries=10;
    createCache();
    maxEnteries=20;
    createPartitionedRegion(true, EvictionAlgorithm.LRU_ENTRY, "PR1", 5, 1, 1000,maxEnteries);
    
    final PartitionedRegion pr = (PartitionedRegion)cache.getRegion("PR1");
    LogWriterUtils.getLogWriter().info(
        "PR- " +pr.getEvictionAttributes().getMaximum());
    
    for (int counter = 1; counter <= maxEnteries+extraEntries; counter++) {
      pr.put(new Integer(counter), new byte[1 * 1024 * 1024]);
    }
     
    assertEquals(extraEntries,((AbstractLRURegionMap)pr.entries)._getLruList().stats().getEvictions());
    
    for (final Iterator i = pr.getDataStore().getAllLocalBuckets().iterator(); i
        .hasNext();) {
      final Map.Entry entry = (Map.Entry)i.next();
      final BucketRegion bucketRegion = (BucketRegion)entry.getValue();
      if (bucketRegion == null) {
        continue;
      }
      LogWriterUtils.getLogWriter().info(
          "FINAL bucket= " + bucketRegion.getFullPath() + "size= "
              + bucketRegion.size() + "  count= "+bucketRegion.entryCount());
      assertEquals(4,bucketRegion.size());
    }
  }
  
  @Test
  public void testCheckEntryLruEvictionsIn2DataStore() {
    maxEnteries=20;
    prepareScenario1(EvictionAlgorithm.LRU_ENTRY,maxEnteries);
    putData("PR1", 60, 1);
    validateNoOfEvictions("PR1", 20);
  }

  @Test
  public void testMemLruForPRAndDR() {
    createCache();
    createPartitionedRegion(true, EvictionAlgorithm.LRU_MEMORY, "PR1", 4, 1, 1000,40);
    createDistRegionWithMemEvictionAttr();
    PartitionedRegion pr = (PartitionedRegion)cache.getRegion("PR1");
    DistributedRegion dr = (DistributedRegion)cache.getRegion("DR1");
    
    assertEquals(pr.getLocalMaxMemory(), pr.getEvictionAttributes().getMaximum());
    assertEquals(MemLRUCapacityController.DEFAULT_MAXIMUM_MEGABYTES, dr.getEvictionAttributes().getMaximum());
   
   for (int i = 0; i < 41; i++) {
     pr.put(new Integer(i), new byte[1 * 1024 * 1024]);
    }
   
   assertTrue(1<=((AbstractLRURegionMap)pr.entries)._getLruList().stats().getEvictions());
   assertTrue(((AbstractLRURegionMap)pr.entries)._getLruList().stats().getEvictions()<=2);
   
   for (int i = 0; i < 11; i++) {
     dr.put(new Integer(i), new byte[1 * 1024 * 1024]);
    }
  
   assertTrue(1<=((AbstractLRURegionMap)dr.entries)._getLruList().stats().getEvictions());
   assertTrue(((AbstractLRURegionMap)dr.entries)._getLruList().stats().getEvictions()<=2);
  }
  
  @Test
  public void testEachTaskSize() {
    createCache();
    createPartitionedRegion(true, EvictionAlgorithm.LRU_HEAP, "PR1", 6, 1,
        1000, 40);
    createPartitionedRegion(true, EvictionAlgorithm.LRU_HEAP, "PR2", 10, 1,
        1000, 40);
    createPartitionedRegion(true, EvictionAlgorithm.LRU_HEAP, "PR3", 15, 1,
        1000, 40);
    createDistRegion();

    ArrayList<Integer> taskSetSizes = getTestTaskSetSizes();
    if (taskSetSizes != null) {
      for (Integer size : taskSetSizes) {
        assertEquals(8, size.intValue());
      }
    }
  }
}
