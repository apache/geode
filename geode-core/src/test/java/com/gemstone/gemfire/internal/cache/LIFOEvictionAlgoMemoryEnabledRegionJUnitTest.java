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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.lru.EnableLRU;
import com.gemstone.gemfire.internal.cache.lru.LRUClockNode;
import com.gemstone.gemfire.internal.cache.lru.LRUStatistics;
import com.gemstone.gemfire.internal.cache.lru.NewLRUClockHand;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

/**
 * This is a test verifies region is LIFO enabled by MEMORY verifies correct
 * stats updating and faultin is not evicting another entry - not strict LIFO
 * 
 * @since GemFire 5.7
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(IntegrationTest.class)
public class LIFOEvictionAlgoMemoryEnabledRegionJUnitTest {

  /** The cache instance */
  private static Cache cache = null;

  /** Stores LIFO Related Statistics */
  private static LRUStatistics lifoStats = null;

  /** The distributedSystem instance */
  private static DistributedSystem distributedSystem = null;

  private static String regionName = "LIFOMemoryEvictionEnabledRegion";

  private static int maximumMegabytes = 1;
  
  private static int byteArraySize = 20480;
  
  private static long memEntryCountForFirstPutOperation ;
  
  private int deltaSize = 20738;
  
  private static NewLRUClockHand lifoClockHand = null;


  @Before
  public void setUp() throws Exception {
    initializeVM();
  }

  @After
  public void tearDown() throws Exception {
    assertNotNull(cache);
    Region rgn = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(rgn);
    rgn.localDestroyRegion();
    cache.close();
     
  }

  /**
   * Method for intializing the VM and create region with LIFO attached
   * 
   * @throws Exception
   */
  private static void initializeVM() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, "info"); // to keep diskPerf logs smaller
    distributedSystem = DistributedSystem.connect(props);
    cache = CacheFactory.create(distributedSystem);
    assertNotNull(cache);
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);

    File dir = new File("testingDirectoryDefault");
    dir.mkdir();
    dir.deleteOnExit();
    File[] dirs = { dir };
    dsf.setDiskDirsAndSizes(dirs, new int[] { Integer.MAX_VALUE });

    dsf.setAutoCompact(false);
    DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = true;
    try {
      factory.setDiskStoreName(dsf.create(regionName).getName());
    } finally {
      DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = false;
    }
    factory.setDiskSynchronous(true);
    factory.setDataPolicy(DataPolicy.NORMAL);

    /* setting LIFO MEMORY related eviction attributes */

    factory.setEvictionAttributes(EvictionAttributesImpl.createLIFOMemoryAttributes(
        maximumMegabytes, EvictionAction.OVERFLOW_TO_DISK));
    RegionAttributes attr = factory.create();
    
    ((GemFireCacheImpl)cache).createRegion(regionName, attr);
    /*
     * NewLIFOClockHand extends NewLRUClockHand to hold on to the list reference
     */
    lifoClockHand = ((VMLRURegionMap)((LocalRegion)cache.getRegion(Region.SEPARATOR + regionName)).entries)
        ._getLruList();

    /* storing stats reference */
    lifoStats = lifoClockHand.stats();

  }

  /**
   * This test does the following :<br>
   * 1)Perform put operation <br>
   * 2)Verify count of faultin entries <br>
   */
  @Test
  public void test000EntryFaultinCount() {
    try {
      assertNotNull(cache);
      LocalRegion rgn = (LocalRegion)cache.getRegion(Region.SEPARATOR + regionName);
      assertNotNull(rgn);

      DiskRegionStats diskRegionStats = rgn.getDiskRegion().getStats();
      assertTrue("Entry count not 0 ", new Long(0).equals(new Long(lifoStats
          .getCounter())));

      // put 60 entries into the region
      for (long i = 0L; i < 60L; i++) {
        rgn.put("key" + i, newDummyObject(i));
      }

      assertEquals(
          "LRU eviction entry count and entries overflown to disk count from diskstats is not equal ",
          lifoStats.getEvictions(), diskRegionStats.getNumOverflowOnDisk());
      assertNull("Entry value in VM is not null", rgn.getValueInVM("key59"));
      //used to get number of entries required to reach the limit of memory assign
      memEntryCountForFirstPutOperation = diskRegionStats.getNumEntriesInVM();
      rgn.get("key59");
      assertEquals("Not equal to number of entries present in VM : ", 51L,
          diskRegionStats.getNumEntriesInVM());
      assertEquals("Not equal to number of entries present on disk : ", 9L,
          diskRegionStats.getNumOverflowOnDisk());

    }
    catch (Exception ex) {
      ex.printStackTrace(); 
      fail("Test failed");
    }
  }
  
  /**
   * This test does the following :<br>
   * 1)Varify region is LIFO Enabled <br>
   * 2)Perform put operation <br>
   * 3)perform get operation <br>
   * 4)Varify value retrived <br>
   * 5)Verify count (entries present in memory) after put operations <br>
   * 6)Verify count (entries present in memory) after get (performs faultin)
   * operation <br>
   * 7)Verify count (entries present in memory) after remove operation <br>
   */
  @Test
  public void test001LIFOStatsUpdation() {
    try {
      assertNotNull(cache);
      LocalRegion rgn = (LocalRegion)cache.getRegion(Region.SEPARATOR + regionName);
      assertNotNull(rgn);

      // check for is LIFO Enable
      assertTrue(
          "Eviction Algorithm is not LIFO",
          (((EvictionAttributesImpl)rgn.getAttributes().getEvictionAttributes())
              .isLIFO()));

//    put 60 entries into the region
      for (long i = 0L; i < 60L; i++) {
        rgn.put(new Long(i), newDummyObject(i));
      }
      
      // verifies evicted entry values are null in memory
      assertTrue("In memory ",
          rgn.entries.getEntry(new Long(51)).isValueNull());
      assertTrue("In memory ",
          rgn.entries.getEntry(new Long(52)).isValueNull());
      assertTrue("In memory ",
          rgn.entries.getEntry(new Long(53)).isValueNull());

      // get an entry back
      rgn.get(new Long(46));
      rgn.get(new Long(51));
      rgn.get(new Long(56));
      // gets stuck in while loop
      rgn.put(new Long(60), newDummyObject(60));
      rgn.put(new Long(61), newDummyObject(61));
      assertNull("Entry value in VM is not null", rgn.getValueInVM(new Long(58)));
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("Test failed");
    }
   
  }

  /**
   * This test does the following :<br>
   * 1)Perform put operation <br>
   * 2)Verify entry evicted is LIFO Entry and is not present in vm<br>
   */
  @Test
  public void test002LIFOEntryEviction() {
    try {
      assertNotNull(cache);
      LocalRegion rgn = (LocalRegion)cache.getRegion(Region.SEPARATOR + regionName);
      assertNotNull(rgn);

      assertEquals("Region is not properly cleared ", 0, rgn.size());
      assertTrue("Entry count not 0 ", new Long(0).equals(new Long(lifoStats
          .getCounter())));
      // put sixty entries into the region
      for (long i = 0L; i < 60L; i++) {
        rgn.put(new Long(i), newDummyObject(i));
        if (i < memEntryCountForFirstPutOperation ) {
          // entries are in memory  
            assertNotNull("Entry is not in VM ", rgn.getValueInVM(new Long(i)));
        }
        else {
          /*assertTrue("LIFO Entry is not evicted", lifoClockHand.getLRUEntry()
              .testEvicted());*/
            assertTrue("Entry is not null ", rgn.entries.getEntry(new Long(i)).isValueNull());
        }
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("Test failed");
    }
  }

  /**
   * This test does the following :<br>
   * 1)Perform put operation <br>
   * 2)Verify count of evicted entries <br>
   */
  @Test
  public void test003EntryEvictionCount() {
    try {
      assertNotNull(cache);
      Region rgn = cache.getRegion(Region.SEPARATOR + regionName);
      assertNotNull(rgn);

      assertTrue("Entry count not 0 ", new Long(0).equals(new Long(lifoStats
          .getCounter())));
//    put 60 entries into the region
      for (long i = 0L; i < 60L; i++) {
        rgn.put(new Long(i), newDummyObject(i));
      }

      assertTrue("1)Total eviction count is not correct ", new Long(10)
          .equals(new Long(lifoStats.getEvictions())));
      rgn.put(new Long(60), newDummyObject(60));
      rgn.get(new Long(55));
      assertTrue("2)Total eviction count is not correct ", new Long(11)
          .equals(new Long(lifoStats.getEvictions())));
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("Test failed");
    }
  }
  
//Basic checks to validate lifo queue implementation works as expected
    @Test
    public void testLIFOQueue() {
        try {
            assertNotNull(cache);
            Region rgn = cache.getRegion(Region.SEPARATOR + regionName);
            assertNotNull(rgn);
            //insert data
            lifoClockHand.appendEntry(new TestLRUNode(1));
            lifoClockHand.appendEntry(new TestLRUNode(2));
            lifoClockHand.appendEntry(new TestLRUNode(3));
            assertTrue(lifoClockHand.size() == 3);
            //make sure data is removed in LIFO fashion
            TestLRUNode tailValue = (TestLRUNode) lifoClockHand.getLRUEntry();
            assertTrue("Value = " + tailValue.getValue(), tailValue.getValue() == 3);
            assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 2);
            tailValue = (TestLRUNode) lifoClockHand.getLRUEntry();
            assertTrue("Value = " + tailValue.getValue(), tailValue.getValue() == 2);
            assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 1);
            tailValue = (TestLRUNode) lifoClockHand.getLRUEntry();
            assertTrue("Value = " + tailValue.getValue(), tailValue.getValue() == 1);
            assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 0);
            tailValue = (TestLRUNode) lifoClockHand.getLRUEntry();
            assertTrue("No Value - null", tailValue == null);
            assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 0);
            //check that entries not available or already evicted are skipped and removed
            TestLRUNode testlrunode = new TestLRUNode(1);
            lifoClockHand.appendEntry(testlrunode);
            testlrunode = new TestLRUNode(2);
            testlrunode.setEvicted();
            lifoClockHand.appendEntry(testlrunode);
            testlrunode = new TestLRUNode(3);
            testlrunode.setEvicted();
            lifoClockHand.appendEntry(testlrunode);
            tailValue = (TestLRUNode) lifoClockHand.getLRUEntry();
            assertTrue("Value = " + tailValue.getValue(), tailValue.getValue() == 1);
            assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 0);
            tailValue = (TestLRUNode) lifoClockHand.getLRUEntry();
            assertTrue("No Value - null", tailValue == null);
            assertTrue("LIFO Queue Size = " + lifoClockHand.size(), lifoClockHand.size() == 0);
            //TODO : need tests for data still part of transaction 
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
    }

  
  // purpose to create object ,size of byteArraySize 
  private Object newDummyObject(long i){
    byte[] value = new byte[byteArraySize];
    Arrays.fill(value,(byte)i);
    return value;
  }
}

//test class for validating LIFO queue
class TestLRUNode implements LRUClockNode{
  
    LRUClockNode next=null;
    LRUClockNode prev=null;
    boolean evicted=false;
    boolean recentlyUsed=false;
    int value=0;
    
  public TestLRUNode(int value){
      this.value=value;
  }
  
  public int getValue(){ 
      return value;
  }
  
  public void setNextLRUNode( LRUClockNode next ){
      this.next=next;
  }
  
  public void setPrevLRUNode( LRUClockNode prev ){
      this.prev=prev;
  }
  
  public LRUClockNode nextLRUNode(){
      return next;
  }
  
  public LRUClockNode prevLRUNode(){
      return prev;
  }

  public int updateEntrySize(EnableLRU ccHelper){
      return 0;
  }
 
  public int updateEntrySize(EnableLRU ccHelper, Object value){
      return 0;
  }
  
  public int getEntrySize(){
      return 0;
  }
  
  public boolean testRecentlyUsed(){
      return recentlyUsed;
  }
  
  public void setRecentlyUsed(){
      recentlyUsed=true;
  }
  
  public void unsetRecentlyUsed(){
      recentlyUsed=false;
  }
  
  public void setEvicted(){
      evicted=true;
  }
  
  public void unsetEvicted(){
      evicted=false;
  }
  
  public boolean testEvicted(){
      return evicted;
  } 
}

