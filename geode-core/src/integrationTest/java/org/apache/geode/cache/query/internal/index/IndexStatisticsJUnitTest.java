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
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Numbers;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexStatisticsJUnitTest {

  static QueryService qs;
  static boolean isInitDone = false;
  static Region region;
  static IndexProtocol keyIndex1;
  static IndexProtocol keyIndex2;
  static IndexProtocol keyIndex3;

  @Before
  public void setUp() throws Exception {
    try {
      CacheUtils.startCache();
      region = CacheUtils.createRegion("portfolio", Portfolio.class);
      Position.cnt = 0;
      if (region.size() == 0) {
        for (int i = 0; i < 100; i++) {
          region.put(Integer.toString(i), new Portfolio(i, i));
        }
      }
      assertEquals(100, region.size());
      qs = CacheUtils.getQueryService();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
    IndexManager.TEST_RANGEINDEX_ONLY = false;
  }

  /*
   * public static Test suite() { TestSuite suite = new TestSuite(IndexMaintenanceTest.class);
   * return suite; }
   */
  /**
   * Test RenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForRangeIndex() throws Exception {
    keyIndex1 = (IndexProtocol) qs.createIndex("multiKeyIndex1", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "portfolio p, p.positions.values pos");

    assertTrue(keyIndex1 instanceof RangeIndex);

    IndexStatistics keyIndex1Stats = keyIndex1.getStatistics();

    // Initial stats test (keys, values & updates)
    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(400, keyIndex1Stats.getNumUpdates());

    // IndexUsed stats test
    String queryStr = "select * from " + SEPARATOR
        + "portfolio p, p.positions.values pos where pos.secId = 'YHOO'";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    assertEquals(50, keyIndex1Stats.getTotalUses());
    assertEquals(0, keyIndex1Stats.getReadLockCount());

    // NumOfValues should be reduced.
    for (int i = 0; i < 50; i++) {
      region.invalidate(Integer.toString(i));
    }

    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());

    // Should not have any effect as invalidated values are destroyed
    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());

    // NumOfKeys should get zero as all values are destroyed
    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(500, keyIndex1Stats.getNumUpdates());

    assertEquals(0, keyIndex1Stats.getNumberOfKeys());

    qs.removeIndex(keyIndex1);
  }

  @Test
  public void testStatsForRangeIndexAfterRecreate() throws Exception {

    keyIndex2 = (IndexProtocol) qs.createIndex("multiKeyIndex2", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "portfolio p, p.positions.values pos");

    assertTrue(keyIndex2 instanceof RangeIndex);

    IndexStatistics keyIndex1Stats = keyIndex2.getStatistics();

    // Initial stats test (keys, values & updates)
    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    String queryStr = "select * from " + SEPARATOR
        + "portfolio p, p.positions.values pos where pos.secId = 'YHOO'";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(400, keyIndex1Stats.getNumUpdates());
    assertEquals(50, keyIndex1Stats.getTotalUses());

    region.clear();

    assertEquals(0, keyIndex1Stats.getNumberOfKeys());
    assertEquals(0, keyIndex1Stats.getNumberOfValues());
    assertEquals(800, keyIndex1Stats.getNumUpdates());
    assertEquals(50, keyIndex1Stats.getTotalUses());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(1000, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    assertEquals(100, keyIndex1Stats.getTotalUses());

    qs.removeIndex(keyIndex2);
  }

  /**
   * Test CompactRenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForCompactRangeIndex() throws Exception {

    keyIndex2 =
        (IndexProtocol) qs.createIndex("multiKeyIndex2", IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex2 instanceof CompactRangeIndex);

    IndexStatistics keyIndex1Stats = keyIndex2.getStatistics();

    // Initial stats test (keys, values & updates)
    assertEquals(100, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(100, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(100, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());

    // IndexUsed stats test
    String queryStr = "select * from " + SEPARATOR + "portfolio where ID > 0";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    assertEquals(0, keyIndex1Stats.getReadLockCount());
    assertEquals(50, keyIndex1Stats.getTotalUses());

    // NumOfValues should be reduced.
    for (int i = 0; i < 50; i++) {
      region.invalidate(Integer.toString(i));
    }

    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(250, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(250, keyIndex1Stats.getNumUpdates());

    // NumOfKeys should get zero as all values are destroyed
    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(300, keyIndex1Stats.getNumUpdates());

    assertEquals(0, keyIndex1Stats.getNumberOfKeys());

    qs.removeIndex(keyIndex2);
  }

  @Test
  public void testStatsForCompactRangeIndexAfterRecreate() throws Exception {

    keyIndex2 =
        (IndexProtocol) qs.createIndex("multiKeyIndex2", IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex2 instanceof CompactRangeIndex);

    IndexStatistics keyIndex1Stats = keyIndex2.getStatistics();

    // Initial stats test (keys, values & updates)
    assertEquals(100, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(100, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    String queryStr = "select * from " + SEPARATOR + "portfolio where ID > 0";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    assertEquals(100, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());
    assertEquals(50, keyIndex1Stats.getTotalUses());

    region.clear();

    assertEquals(0, keyIndex1Stats.getNumberOfKeys());
    assertEquals(0, keyIndex1Stats.getNumberOfValues());
    assertEquals(400, keyIndex1Stats.getNumUpdates());
    assertEquals(50, keyIndex1Stats.getTotalUses());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(100, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(500, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    assertEquals(100, keyIndex1Stats.getTotalUses());

    qs.removeIndex(keyIndex2);
  }

  /**
   * Test CompactMapRenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForCompactMapRangeIndex() throws Exception {
    keyIndex3 = (IndexProtocol) qs.createIndex("multiKeyIndex3", IndexType.FUNCTIONAL,
        "positions['DELL', 'YHOO']", SEPARATOR + "portfolio p");
    assertTrue(keyIndex3 instanceof CompactMapRangeIndex);

    Object[] indexes =
        ((CompactMapRangeIndex) keyIndex3).getRangeIndexHolderForTesting().values().toArray();
    assertTrue(indexes[0] instanceof CompactRangeIndex);
    assertTrue(indexes[1] instanceof CompactRangeIndex);

    IndexStatistics keyIndexStats = keyIndex3.getStatistics();

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, keyIndexStats.getNumberOfKeys());
    assertEquals(200, keyIndexStats.getNumberOfValues());
    assertEquals(200, keyIndexStats.getNumUpdates());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, keyIndexStats.getNumberOfKeys());
    assertEquals(200, keyIndexStats.getNumberOfValues());
    assertEquals(400, keyIndexStats.getNumUpdates());

    String queryStr =
        "select * from " + SEPARATOR
            + "portfolio where positions['DELL'] != NULL OR positions['YHOO'] != NULL";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    assertEquals(0, keyIndexStats.getReadLockCount());

    assertEquals(100, keyIndexStats.getTotalUses());

    for (int i = 0; i < 50; i++) {
      region.invalidate(Integer.toString(i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, keyIndexStats.getNumberOfKeys());
    assertEquals(100, keyIndexStats.getNumberOfValues());
    assertEquals(500, keyIndexStats.getNumUpdates());

    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, keyIndexStats.getNumberOfKeys());
    assertEquals(100, keyIndexStats.getNumberOfValues());
    assertEquals(500, keyIndexStats.getNumUpdates());

    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(600, keyIndexStats.getNumUpdates());

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(0, keyIndexStats.getNumberOfKeys());

    qs.removeIndex(keyIndex3);
    region.destroyRegion();
  }

  /**
   * Test MapRenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForMapRangeIndex() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    keyIndex3 = (IndexProtocol) qs.createIndex("multiKeyIndex3", IndexType.FUNCTIONAL,
        "positions['DELL', 'YHOO']", SEPARATOR + "portfolio");

    assertTrue(keyIndex3 instanceof MapRangeIndex);

    Object[] indexes =
        ((MapRangeIndex) keyIndex3).getRangeIndexHolderForTesting().values().toArray();
    assertTrue(indexes[0] instanceof RangeIndex);
    assertTrue(indexes[1] instanceof RangeIndex);

    IndexStatistics mapIndexStats = keyIndex3.getStatistics();

    assertEquals(2, mapIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, mapIndexStats.getNumberOfKeys());
    assertEquals(200, mapIndexStats.getNumberOfValues());
    assertEquals(200, mapIndexStats.getNumUpdates());


    Position.cnt = 0;
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(2, mapIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, mapIndexStats.getNumberOfKeys());
    assertEquals(200, mapIndexStats.getNumberOfValues());
    assertEquals(400, mapIndexStats.getNumUpdates());
    String queryStr =
        "select * from " + SEPARATOR
            + "portfolio where positions['DELL'] != NULL OR positions['YHOO'] != NULL";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }


    assertEquals(0, mapIndexStats.getReadLockCount());

    assertEquals(100, mapIndexStats.getTotalUses());

    for (int i = 0; i < 50; i++) {
      region.invalidate(Integer.toString(i));
    }

    assertEquals(2, mapIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, mapIndexStats.getNumberOfKeys());
    assertEquals(100, mapIndexStats.getNumberOfValues());
    assertEquals(600, mapIndexStats.getNumUpdates());

    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(2, mapIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, mapIndexStats.getNumberOfKeys());
    assertEquals(100, mapIndexStats.getNumberOfValues());
    assertEquals(600, mapIndexStats.getNumUpdates());

    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(800, mapIndexStats.getNumUpdates());

    assertEquals(2, mapIndexStats.getNumberOfMapIndexKeys());
    assertEquals(0, mapIndexStats.getNumberOfKeys());

    qs.removeIndex(keyIndex3);
  }

  /**
   * Test RenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForRangeIndexBeforeRegionCreation() throws Exception {
    // Destroy region
    region.clear();
    assertEquals(0, region.size());
    Position.cnt = 0;

    keyIndex1 = (IndexProtocol) qs.createIndex("multiKeyIndex4", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "portfolio p, p.positions.values pos");

    // Recreate all entries in the region
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertTrue(keyIndex1 instanceof RangeIndex);

    IndexStatistics keyIndex1Stats = keyIndex1.getStatistics();

    // Initial stats test (keys, values & updates)
    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(400, keyIndex1Stats.getNumUpdates());

    // IndexUsed stats test
    String queryStr = "select * from " + SEPARATOR
        + "portfolio p, p.positions.values pos where pos.secId = 'YHOO'";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    assertEquals(50, keyIndex1Stats.getTotalUses());

    // NumOfValues should be reduced.
    for (int i = 0; i < 50; i++) {
      region.invalidate(Integer.toString(i));
    }

    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());

    // Should not have any effect as invalidated values are destroyed
    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());

    // NumOfKeys should get zero as all values are destroyed
    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(500, keyIndex1Stats.getNumUpdates());

    assertEquals(0, keyIndex1Stats.getNumberOfKeys());

    qs.removeIndex(keyIndex1);
  }

  /**
   * Test CompactRenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForCompactRangeIndexBeforeRegionCreation() throws Exception {
    // Destroy region
    region.clear();
    assertEquals(0, region.size());
    Position.cnt = 0;

    keyIndex2 =
        (IndexProtocol) qs.createIndex("multiKeyIndex5", IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    // Recreate all entries in the region
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertTrue(keyIndex2 instanceof CompactRangeIndex);

    IndexStatistics keyIndex1Stats = keyIndex2.getStatistics();

    // Initial stats test (keys, values & updates)
    assertEquals(100, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(100, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(100, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());

    // IndexUsed stats test
    String queryStr = "select * from " + SEPARATOR + "portfolio where ID > 0";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    assertEquals(50, keyIndex1Stats.getTotalUses());

    // NumOfValues should be reduced.
    for (int i = 0; i < 50; i++) {
      region.invalidate(Integer.toString(i));
    }

    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(250, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(250, keyIndex1Stats.getNumUpdates());

    // NumOfKeys should get zero as all values are destroyed
    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(300, keyIndex1Stats.getNumUpdates());

    assertEquals(0, keyIndex1Stats.getNumberOfKeys());

    qs.removeIndex(keyIndex2);
  }

  /**
   * Test MapRenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForMapRangeIndexBeforeRegionCreation() throws Exception {
    // Destroy region
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    region.clear();
    assertEquals(0, region.size());
    Position.cnt = 0;

    keyIndex3 = (IndexProtocol) qs.createIndex("multiKeyIndex6", IndexType.FUNCTIONAL,
        "positions['DELL', 'YHOO']", SEPARATOR + "portfolio");
    Object[] indexes =
        ((MapRangeIndex) keyIndex3).getRangeIndexHolderForTesting().values().toArray();
    assertEquals(indexes.length, 0);

    // Recreate all entries in the region
    Position.cnt = 0;
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    assertTrue(keyIndex3 instanceof MapRangeIndex);
    indexes = ((MapRangeIndex) keyIndex3).getRangeIndexHolderForTesting().values().toArray();
    assertTrue(indexes[0] instanceof RangeIndex);
    assertTrue(indexes[1] instanceof RangeIndex);

    IndexStatistics keyIndex1Stats = ((RangeIndex) indexes[0]).getStatistics();
    IndexStatistics keyIndex2Stats = ((RangeIndex) indexes[1]).getStatistics();
    IndexStatistics mapIndexStats = keyIndex3.getStatistics();

    assertEquals(100, mapIndexStats.getNumberOfKeys());
    assertEquals(200, mapIndexStats.getNumberOfValues());
    assertEquals(200, mapIndexStats.getNumUpdates());


    Position.cnt = 0;
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(100, mapIndexStats.getNumberOfKeys());
    assertEquals(200, mapIndexStats.getNumberOfValues());
    assertEquals(400, mapIndexStats.getNumUpdates());

    String queryStr =
        "select * from " + SEPARATOR
            + "portfolio where positions['DELL'] != NULL OR positions['YHOO'] != NULL";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    assertEquals(100, mapIndexStats.getTotalUses());

    for (int i = 0; i < 50; i++) {
      region.invalidate(Integer.toString(i));
    }

    assertEquals(50, mapIndexStats.getNumberOfKeys());
    assertEquals(100, mapIndexStats.getNumberOfValues());
    assertEquals(600, mapIndexStats.getNumUpdates());


    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(50, mapIndexStats.getNumberOfKeys());
    assertEquals(100, mapIndexStats.getNumberOfValues());
    assertEquals(600, mapIndexStats.getNumUpdates());

    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(800, mapIndexStats.getNumUpdates());

    assertEquals(0, mapIndexStats.getNumberOfKeys());

    qs.removeIndex(keyIndex3);
  }


  @Test
  public void testCompactRangeIndexNumKeysStats() throws Exception {
    String regionName = "testCompactRegionIndexNumKeysStats_region";
    Region region = CacheUtils.createRegion(regionName, Numbers.class);

    Index index = qs.createIndex("idIndexName", "r.max1", SEPARATOR + regionName + " r");
    IndexStatistics stats = index.getStatistics();

    // Add an object and check stats
    Numbers obj1 = new Numbers(1);
    obj1.max1 = 20;
    region.put(1, obj1);
    assertEquals(1, stats.getNumberOfValues());
    assertEquals(1, stats.getNumberOfKeys());
    // assertIndexDetailsEquals(1, stats.getNumberOfValues(20f));
    assertEquals(1, stats.getNumUpdates());

    // add a second object with the same index key
    Numbers obj2 = new Numbers(1);
    obj2.max1 = 20;
    region.put(2, obj2);
    assertEquals(2, stats.getNumberOfValues());
    assertEquals(1, stats.getNumberOfKeys());
    // assertIndexDetailsEquals(2, stats.getNumberOfValues(20f));
    assertEquals(2, stats.getNumUpdates());

    // remove the second object and check that keys are 1
    region.remove(2);
    assertEquals(1, stats.getNumberOfValues());
    assertEquals(1, stats.getNumberOfKeys());
    // assertIndexDetailsEquals(1, stats.getNumberOfValues(20f));
    assertEquals(3, stats.getNumUpdates());

    // remove the first object and check that keys are 0
    region.remove(1);
    assertEquals(0, stats.getNumberOfValues());
    assertEquals(0, stats.getNumberOfKeys());
    // assertIndexDetailsEquals(0, stats.getNumberOfValues(20f));
    assertEquals(4, stats.getNumUpdates());

    // add object with a different key and check results
    obj2.max1 = 21;
    region.put(3, obj2);
    assertEquals(1, stats.getNumberOfValues());
    assertEquals(1, stats.getNumberOfKeys());
    // assertIndexDetailsEquals(0, stats.getNumberOfValues(20f));
    assertEquals(5, stats.getNumUpdates());

    // add object with original key and check that num keys are 2
    obj1.max1 = 20;
    region.put(1, obj1);
    assertEquals(2, stats.getNumberOfValues());
    assertEquals(2, stats.getNumberOfKeys());
    // assertIndexDetailsEquals(1, stats.getNumberOfValues(20f));
    assertEquals(6, stats.getNumUpdates());
  }

  @Test
  public void testReadLockCountStatsinRR() throws Exception {
    verifyReadLockCountStatsForCompactRangeIndex(false);
    verifyReadLockCountStatsForRangeIndex(false);
  }

  @Test
  public void testReadLockCountStatsinPR() throws Exception {
    verifyReadLockCountStatsForCompactRangeIndex(true);
    verifyReadLockCountStatsForRangeIndex(true);
  }

  public void verifyReadLockCountStatsForCompactRangeIndex(boolean isPr) throws Exception {
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserver old = QueryObserverHolder.setInstance(observer);

    String regionName = "exampleRegion";
    String name = SEPARATOR + regionName;

    final Cache cache = CacheUtils.getCache();
    Region r1 = null;
    if (isPr) {
      r1 = cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    } else {
      r1 = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    }

    QueryService qs = cache.getQueryService();
    Index statusIndex = qs.createIndex("status", "status", name);
    assertEquals(cache.getQueryService().getIndexes().size(), 1);

    for (int i = 0; i < 3; i++) {
      r1.put("key-" + i, new Portfolio(i));
    }

    String query = "select distinct * from " + name + " where status = 'active'";

    final Query q = cache.getQueryService().newQuery(query);
    SelectResults sr = (SelectResults) q.execute();

    assertEquals("Read locks should have been taken by the query ", 1, observer.readLockCount);
    assertEquals("Read lock count should have been released by the query ", 0,
        statusIndex.getStatistics().getReadLockCount());

    QueryObserverHolder.setInstance(old);
    qs.removeIndexes();
  }

  public void verifyReadLockCountStatsForRangeIndex(boolean isPr) throws Exception {
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserver old = QueryObserverHolder.setInstance(observer);

    String regionName = "exampleRegion";
    String name = SEPARATOR + regionName;

    final Cache cache = CacheUtils.getCache();
    Region r1 = cache.getRegion(regionName);

    QueryService qs = cache.getQueryService();
    Index secIdIndex = qs.createIndex("secId", "pos.secId", name + " p, p.positions.values pos");
    assertEquals(cache.getQueryService().getIndexes().size(), 1);

    for (int i = 0; i < 10; i++) {
      r1.put("key-" + i, new Portfolio(i));
    }

    String query =
        "select distinct * from " + name + " p, p.positions.values pos where pos.secId = 'IBM' ";

    final Query q = cache.getQueryService().newQuery(query);
    SelectResults sr = (SelectResults) q.execute();

    assertEquals("Read locks should have been taken by the query ", 1, observer.readLockCount);
    assertEquals("Read lock count should have been released by the query ", 0,
        secIdIndex.getStatistics().getReadLockCount());

    QueryObserverHolder.setInstance(old);
    qs.removeIndexes();
  }

  public static class QueryObserverImpl extends QueryObserverAdapter {
    int readLockCount = 0;

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      readLockCount = index.getStatistics().getReadLockCount();
    }
  }
}
