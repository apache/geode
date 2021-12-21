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
package org.apache.geode.cache30;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.DiskRegionStats;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.eviction.EvictionCounters;

/**
 * A little test program for testing (and debugging) disk regions.
 *
 *
 * @since GemFire 3.2
 */
public class TestDiskRegion {

  /**
   * Returns the <code>EvictionStatistics</code> for the given region
   */
  private static EvictionCounters getLRUStats(Region region) {
    final LocalRegion l = (LocalRegion) region;
    return l.getEvictionController().getCounters();
  }

  public static void main(String[] args) throws Exception {
    DistributedSystem system = DistributedSystem.connect(new java.util.Properties());
    Cache cache = CacheFactory.create(system);
    AttributesFactory factory = new AttributesFactory();
    factory.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(2,
        null, EvictionAction.OVERFLOW_TO_DISK));
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File user_dir = new File(System.getProperty("user.dir"));
    if (!user_dir.exists()) {
      user_dir.mkdir();
    }
    File[] dirs1 = new File[] {user_dir};
    DiskStore ds1 = dsf.setDiskDirs(dirs1).create("TestDiskRegion");
    factory.setDiskStoreName("TestDiskRegion");
    LocalRegion region = (LocalRegion) cache.createRegion("TestDiskRegion", factory.create());
    DiskRegion dr = region.getDiskRegion();
    Assert.assertTrue(dr != null);
    DiskRegionStats diskStats = dr.getStats();
    EvictionCounters lruStats = getLRUStats(region);
    Assert.assertTrue(diskStats != null);
    Assert.assertTrue(lruStats != null);

    // Put some small stuff
    for (int i = 0; i < 10; i++) {
      region.put(new Integer(i), String.valueOf(i));
    }

    Assert.assertTrue(diskStats.getWrites() == 0);
    Assert.assertTrue(diskStats.getReads() == 0);
    Assert.assertTrue(lruStats.getEvictions() == 0);

    // // Make sure we can get them back okay
    // for (int i = 0; i < 10; i++) {
    // Object value = region.get(new Integer(i));
    // Assert.assertTrue(value != null);
    // Assert.assertTrue(String.valueOf(i).equals(value));
    // }

    // Put in larger stuff until we start evicting
    int total;
    for (total = 0; lruStats.getEvictions() <= 0; total++) {
      System.out.println("total puts " + total + ", evictions " + lruStats.getEvictions()
          + ", total entry size " + lruStats.getCounter());
      int[] array = new int[250];
      array[0] = total;
      region.put(new Integer(total), array);
    }

    Assert.assertTrue(diskStats.getWrites() == 1);
    Assert.assertTrue(diskStats.getReads() == 0);
    Assert.assertTrue(lruStats.getEvictions() == 1);

    System.out.println("----------  Finished Putting -------------");

    Object value = region.get(new Integer(0));
    Assert.assertTrue(value != null);
    Assert.assertTrue(((int[]) value)[0] == 0);

    Assert.assertTrue(diskStats.getWrites() == 2, String.valueOf(diskStats.getWrites()));
    Assert.assertTrue(diskStats.getReads() == 1);
    Assert.assertTrue(lruStats.getEvictions() == 2, String.valueOf(lruStats.getEvictions()));

    System.out.println("----------  Getting ALL -------------");

    for (int i = 0; i < total; i++) {
      System.out.println("total gets " + i + ", evictions " + lruStats.getEvictions()
          + ", total entry size " + lruStats.getCounter());

      int[] array = (int[]) region.get(new Integer(i));
      Assert.assertTrue(array != null);
      Assert.assertTrue(i == array[0]);
    }

    System.out.println("---------  Updating  --------------");

    long startEvictions = lruStats.getEvictions();
    for (int i = 0; i < 10; i++) {
      region.put(new Integer(i), new int[251]);
      long expected = startEvictions + 1 + i;
      long actual = lruStats.getEvictions();
      Assert.assertTrue(expected == actual,
          "For " + i + " expected " + expected + ", got " + actual);
    }

    System.out.println("Done.  Waiting for stats to be written...");
    Thread.sleep(5 * 1000);
  }

  public static void main1(String[] args) throws Exception {
    DistributedSystem system = DistributedSystem.connect(new java.util.Properties());
    Cache cache = CacheFactory.create(system);
    AttributesFactory factory = new AttributesFactory();
    factory.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(2,
        null, EvictionAction.OVERFLOW_TO_DISK));
    factory.setCacheListener(new CacheListenerAdapter() {
      @Override
      public void afterUpdate(EntryEvent event) {
        System.out.println("UPDATE: " + event.getKey() + " -> (" + event.getOldValue() + " -> "
            + event.getNewValue() + ")");
      }
    });

    LocalRegion region = (LocalRegion) cache.createRegion("TestDiskRegion", factory.create());
    DiskRegion dr = region.getDiskRegion();
    DiskRegionStats diskStats = dr.getStats();
    EvictionCounters lruStats = getLRUStats(region);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Hit enter to perform action");
    for (int i = 0; true; i++) {
      br.readLine();
      // Thread.sleep(500);
      Object key = new Integer(i);
      Object value = new byte[200000];
      region.put(key, value);
      System.out.println(key + " -> " + value + " evictions = " + lruStats.getEvictions()
          + ", writes = " + diskStats.getWrites());
    }
  }

  /**
   * Byte arrays
   */
  public static void main4(String[] args) throws Exception {
    DistributedSystem system = DistributedSystem.connect(new java.util.Properties());
    Cache cache = CacheFactory.create(system);
    AttributesFactory factory = new AttributesFactory();
    factory.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(2,
        null, EvictionAction.OVERFLOW_TO_DISK));
    LocalRegion region = (LocalRegion) cache.createRegion("TestDiskRegion", factory.create());
    // DiskRegion dr = region.getDiskRegion();
    // DiskRegionStats diskStats = dr.getStats();
    // EvictionStatistics lruStats = getLRUStats(region);

    // int total;
    // for (total = 0; lruStats.getEvictions() > 100; total++) {
    // region.put(new Integer(total), String.valueOf(total).getBytes());
    // }

    // for (int i = 0; i < total; i++) {
    // byte[] bytes = (byte[]) region.get(new Integer(i));
    // Assert.assertTrue((new String(bytes)).equals(String.valueOf(i)));
    // }

    for (int i = 0; i < 100000; i++) {
      System.out.println(i);
      region.put(String.valueOf(i), String.valueOf(i).getBytes());
    }
  }

  /**
   * Filling up the region with keys and values
   */
  public static void main5(String[] args) throws Exception {
    DistributedSystem system = DistributedSystem.connect(new java.util.Properties());
    Cache cache = CacheFactory.create(system);
    AttributesFactory factory = new AttributesFactory();
    factory.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(2,
        null, EvictionAction.OVERFLOW_TO_DISK));
    LocalRegion region = (LocalRegion) cache.createRegion("TestDiskRegion", factory.create());
    // DiskRegion dr = region.getDiskRegion();
    // DiskRegionStats diskStats = dr.getStats();
    EvictionCounters lruStats = getLRUStats(region);

    for (int i = 0; i < 10000; i++) {
      int[] array = new int[1000];
      array[0] = i;
      try {
        region.put(array, new Integer(i));

      } catch (IllegalStateException ex) {
        System.out.println("Ran out of space: " + ex);
        return;
      }
    }

    String s = "Limit is " + lruStats.getLimit() + " evictions are " + lruStats.getEvictions();
    throw new RuntimeException(s);
  }

}
