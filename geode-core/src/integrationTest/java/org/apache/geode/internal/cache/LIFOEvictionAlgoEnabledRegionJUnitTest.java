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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.eviction.EvictionList;

/**
 * This is a test verifies region is LIFO enabled by ENTRY COUNT verifies correct stats updating and
 * faultin is not evicting another entry - not strict LIFO
 *
 * @since GemFire 5.7
 */
public class LIFOEvictionAlgoEnabledRegionJUnitTest {

  /** The cache instance */
  private static Cache cache = null;

  /** Stores LIFO Related Statistics */
  private static EvictionCounters lifoStats = null;

  /** The distributedSystem instance */
  private static DistributedSystem distributedSystem = null;

  private static final String regionName = "LIFOEntryCountEvictionEnabledRegion";

  private static final int capacity = 5;

  private static EvictionList lifoClockHand = null;

  @Before
  public void setUp() throws Exception {
    initializeVM();
  }

  @After
  public void tearDown() throws Exception {
    assertNotNull(cache);
    Region rgn = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(rgn);
    rgn.localDestroyRegion();
    cache.close();

  }

  /**
   * Method for intializing the VM and create region with LIFO attached
   *
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
    File[] dirs = {dir};
    dsf.setDiskDirsAndSizes(dirs, new int[] {Integer.MAX_VALUE});

    dsf.setAutoCompact(false);
    DiskStore ds = dsf.create(regionName);
    factory.setDiskStoreName(ds.getName());
    factory.setDiskSynchronous(true);
    factory.setDataPolicy(DataPolicy.NORMAL);

    /* setting LIFO related eviction attributes */

    factory.setEvictionAttributes(EvictionAttributesImpl.createLIFOEntryAttributes(capacity,
        EvictionAction.OVERFLOW_TO_DISK));
    RegionAttributes attr = factory.create();

    ((GemFireCacheImpl) cache).createRegion(regionName, attr);
    lifoClockHand =
        ((VMLRURegionMap) ((LocalRegion) cache.getRegion(SEPARATOR + regionName)).entries)
            .getEvictionList();

    /* storing stats reference */
    lifoStats = lifoClockHand.getStatistics();

  }

  /**
   * This test does the following :<br>
   * 1)Verify region is LIFO Enabled <br>
   * 2)Perform put operation <br>
   * 3)perform get operation <br>
   * 4)Verify value retrieved <br>
   * 5)Verify count (entries present in memory) after put operations <br>
   * 6)Verify count (entries present in memory) after get (performs faultin) operation <br>
   * 7)Verify count (entries present in memory) after remove operation <br>
   */
  @Test
  public void testLIFOStatsUpdation() {
    try {
      assertNotNull(cache);
      LocalRegion rgn = (LocalRegion) cache.getRegion(SEPARATOR + regionName);
      assertNotNull(rgn);

      // check for is LIFO Enable
      assertTrue("Eviction Algorithm is not LIFO",
          (((EvictionAttributesImpl) rgn.getAttributes().getEvictionAttributes()).isLIFO()));

      // put four entries into the region
      for (int i = 0; i < 8; i++) {
        rgn.put(new Long(i), new Long(i));
      }

      assertTrue("In Memory entry count not 5 ",
          new Long(5).equals(new Long(lifoStats.getCounter())));

      // varifies evicted entry values are null in memory
      assertTrue("In memory ", rgn.entries.getEntry(new Long(5)).isValueNull());
      assertTrue("In memory ", rgn.entries.getEntry(new Long(6)).isValueNull());
      assertTrue("In memory ", rgn.entries.getEntry(new Long(7)).isValueNull());

      // get an entry back
      Long value = (Long) rgn.get(new Long(4));
      value = (Long) rgn.get(new Long(5));
      value = (Long) rgn.get(new Long(6));

      // check for entry value
      assertTrue("Value not matched ", value.equals(new Long(6)));
      assertNull("Entry value in VM is not null", rgn.getValueInVM(new Long(7)));

      assertTrue("Entry count not 7 ", new Long(7).equals(new Long(lifoStats.getCounter())));
      // check for destory
      rgn.destroy(new Long(3));
      assertTrue("Entry count not 6 ", new Long(6).equals(new Long(lifoStats.getCounter())));
      // check for invalidate
      rgn.invalidate(new Long(1));
      assertTrue("Entry count not 5 ", new Long(5).equals(new Long(lifoStats.getCounter())));
      // check for remove
      rgn.put(new Long(8), new Long(8));
      rgn.remove(new Long(2));
      assertTrue("Entry count not 4 ", new Long(4).equals(new Long(lifoStats.getCounter())));
    } catch (Exception ex) {
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
  public void testLIFOEntryEviction() {
    try {
      assertNotNull(cache);
      LocalRegion rgn = (LocalRegion) cache.getRegion(SEPARATOR + regionName);
      assertNotNull(rgn);

      assertEquals("Region is not properly cleared ", 0, rgn.size());
      assertTrue("Entry count not 0 ", new Long(0).equals(new Long(lifoStats.getCounter())));
      // put eight entries into the region
      for (int i = 0; i < 8; i++) {
        rgn.put(new Long(i), new Long(i));
        if (i < capacity) {
          // entries are in memory
          assertNotNull("Entry is not in VM ", rgn.getValueInVM(new Long(i)));
        } else {
          /*
           * assertTrue("LIFO Entry is not evicted", lifoClockHand.getLRUEntry() .testEvicted());
           */
          assertTrue("Entry is not null ", rgn.entries.getEntry(new Long(i)).isValueNull());
        }
      }

    } catch (Exception ex) {
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
  public void testEntryEvictionCount() {
    try {
      assertNotNull(cache);
      Region rgn = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(rgn);

      assertTrue("Entry count not 0 ", new Long(0).equals(new Long(lifoStats.getCounter())));
      // put four entries into the region
      for (int i = 0; i < 8; i++) {
        rgn.put(new Long(i), new Long(i));
      }

      assertTrue("1)Total eviction count is not correct ",
          new Long(3).equals(new Long(lifoStats.getEvictions())));
      rgn.put(new Long(8), new Long(8));
      rgn.get(new Long(5));
      assertTrue("2)Total eviction count is not correct ",
          new Long(4).equals(new Long(lifoStats.getEvictions())));
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Test failed");
    }
  }

  /**
   * This test does the following :<br>
   * 1)Perform put operation <br>
   * 2)Verify count of faultin entries <br>
   */
  @Test
  public void testEntryFaultinCount() {
    try {
      assertNotNull(cache);
      LocalRegion rgn = (LocalRegion) cache.getRegion(SEPARATOR + regionName);
      assertNotNull(rgn);

      DiskRegionStats diskRegionStats = rgn.getDiskRegion().getStats();
      assertTrue("Entry count not 0 ", new Long(0).equals(new Long(lifoStats.getCounter())));

      // put five entries into the region
      for (int i = 0; i < 8; i++) {
        rgn.put("key" + i, "value" + i);
      }

      assertEquals(
          "LRU eviction entry count and entries overflown to disk count from diskstats is not equal ",
          lifoStats.getEvictions(), diskRegionStats.getNumOverflowOnDisk());
      assertNull("Entry value in VM is not null", rgn.getValueInVM("key6"));
      rgn.get("key6");
      assertEquals("Not equal to number of entries present in VM : ", 6L,
          diskRegionStats.getNumEntriesInVM());
      assertEquals("Not equal to number of entries present on disk : ", 2L,
          diskRegionStats.getNumOverflowOnDisk());

    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Test failed");
    }
  }

  /**
   * This test does the following :<br>
   * 1)Verify Entry value after faultin should be byte []<br>
   */
  // not using it. added as may needed if functionality gets added
  @Ignore
  @Test
  public void testFaultInEntryValueShouldbeSerialized() {
    try {
      assertNotNull(cache);
      LocalRegion rgn = (LocalRegion) cache.getRegion(SEPARATOR + regionName);
      assertNotNull(rgn);

      assertEquals("Region is not properly cleared ", 0, rgn.size());
      assertTrue("Entry count not 0 ", new Long(0).equals(new Long(lifoStats.getCounter())));
      // put eight entries into the region
      for (int i = 0; i < 8; i++) {
        rgn.put(new Long(i), new Long(i));
      }

      // assert for value should be Byte Array
      // here value is not delivered to client and should be get deserialized
      // value in region should be serialized form
      assertTrue("FaultIn Value in not a byte Array ", rgn.get(new Long(6)) instanceof byte[]);

    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Test failed");
    }
  }
}
