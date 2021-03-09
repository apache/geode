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
package org.apache.geode.cache.query.partitioned;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.cache.query.internal.index.PartitionedIndex;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class PRIndexStatisticsJUnitTest {

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

  private void createAndPopulateRegion() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    assertTrue(region instanceof PartitionedRegion);
    Position.cnt = 0;
    if (region.size() == 0) {
      for (int i = 0; i < 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
  }

  private void createRegion() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    assertTrue(region instanceof PartitionedRegion);
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
    createAndPopulateRegion();
    keyIndex1 = (IndexProtocol) qs.createIndex("multiKeyIndex1", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "portfolio p, p.positions.values pos");

    assertTrue(keyIndex1 instanceof PartitionedIndex);

    IndexStatistics keyIndex1Stats = keyIndex1.getStatistics();
    assertEquals(89, keyIndex1Stats.getNumberOfBucketIndexes());

    // Initial stats test (keys, values & updates)
    assertEquals(2 * 100/* Num of values in region */, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(2 * 100/* Num of values in region */, keyIndex1Stats.getNumberOfKeys());
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

    assertEquals(100/* Num of values in region */, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());

    // Should not have any effect as invalidated values are destroyed
    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(100/* Num of values in region */, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());

    // NumOfKeys should get zero as all values are destroyed
    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(500, keyIndex1Stats.getNumUpdates());

    assertEquals(0, keyIndex1Stats.getNumberOfKeys());

    qs.removeIndex(keyIndex1);
    region.destroyRegion();
  }

  /**
   * Test CompactRenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForCompactRangeIndex() throws Exception {
    createAndPopulateRegion();
    keyIndex2 =
        (IndexProtocol) qs.createIndex("multiKeyIndex2", IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex2 instanceof PartitionedIndex);

    IndexStatistics keyIndex1Stats = keyIndex2.getStatistics();
    assertEquals(89, keyIndex1Stats.getNumberOfBucketIndexes());

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
    region.destroyRegion();
  }


  /**
   * Test CompactMapRenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForCompactMapRangeIndex() throws Exception {
    createAndPopulateRegion();
    keyIndex3 = (IndexProtocol) qs.createIndex("multiKeyIndex3", IndexType.FUNCTIONAL,
        "positions['DELL', 'YHOO']", SEPARATOR + "portfolio p");

    assertTrue(keyIndex3 instanceof PartitionedIndex);

    IndexStatistics keyIndexStats = keyIndex3.getStatistics();
    assertTrue(keyIndexStats instanceof IndexStatistics);
    assertEquals(89, keyIndexStats.getNumberOfBucketIndexes());

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, keyIndexStats.getNumberOfKeys());
    assertEquals(100, keyIndexStats.getNumberOfValues());
    assertEquals(100, keyIndexStats.getNumUpdates());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, keyIndexStats.getNumberOfKeys());
    assertEquals(100, keyIndexStats.getNumberOfValues());
    assertEquals(200, keyIndexStats.getNumUpdates());

    String queryStr =
        "select * from " + SEPARATOR
            + "portfolio where positions['DELL'] != NULL OR positions['YHOO'] != NULL";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    // Index should not be used
    assertEquals(0, keyIndexStats.getTotalUses());

    for (int i = 0; i < 50; i++) {
      region.invalidate(Integer.toString(i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, keyIndexStats.getNumberOfKeys());
    assertEquals(50, keyIndexStats.getNumberOfValues());
    assertEquals(250, keyIndexStats.getNumUpdates());

    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, keyIndexStats.getNumberOfKeys());
    assertEquals(50, keyIndexStats.getNumberOfValues());
    assertEquals(250, keyIndexStats.getNumUpdates());

    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(300, keyIndexStats.getNumUpdates());
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
    createAndPopulateRegion();
    keyIndex3 = (IndexProtocol) qs.createIndex("multiKeyIndex3", IndexType.FUNCTIONAL,
        "positions['DELL', 'YHOO']", SEPARATOR + "portfolio");

    assertTrue(keyIndex3 instanceof PartitionedIndex);

    IndexStatistics keyIndexStats = keyIndex3.getStatistics();
    assertTrue(keyIndexStats instanceof IndexStatistics);
    assertEquals(89, keyIndexStats.getNumberOfBucketIndexes());

    assertEquals(89, keyIndexStats.getNumberOfBucketIndexes());
    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, keyIndexStats.getNumberOfKeys());
    assertEquals(100, keyIndexStats.getNumberOfValues());
    assertEquals(100, keyIndexStats.getNumUpdates());

    Position.cnt = 0;
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, keyIndexStats.getNumberOfKeys());
    assertEquals(100, keyIndexStats.getNumberOfValues());
    assertEquals(200, keyIndexStats.getNumUpdates());

    String queryStr =
        "select * from " + SEPARATOR
            + "portfolio where positions['DELL'] != NULL OR positions['YHOO'] != NULL";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    // Both RangeIndex should be used
    assertEquals(100 /* Execution time */, keyIndexStats.getTotalUses());

    for (int i = 0; i < 50; i++) {
      region.invalidate(Integer.toString(i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, keyIndexStats.getNumberOfKeys());
    assertEquals(50, keyIndexStats.getNumberOfValues());
    assertEquals(300, keyIndexStats.getNumUpdates());

    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, keyIndexStats.getNumberOfKeys());
    assertEquals(50, keyIndexStats.getNumberOfValues());
    assertEquals(300, keyIndexStats.getNumUpdates());


    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(400, keyIndexStats.getNumUpdates());
    assertEquals(0, keyIndexStats.getNumberOfKeys());
    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());

    qs.removeIndex(keyIndex3);
    region.destroyRegion();
  }

  /**
   * Test RenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForRangeIndexBeforeRegionCreation() throws Exception {
    // Destroy region
    createRegion();
    assertEquals(0, region.size());

    keyIndex1 = (IndexProtocol) qs.createIndex("multiKeyIndex4", IndexType.FUNCTIONAL, "pos.secId",
        SEPARATOR + "portfolio p, p.positions.values pos");

    // Recreate all entries in the region
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertTrue(keyIndex1 instanceof PartitionedIndex);

    IndexStatistics keyIndex1Stats = keyIndex1.getStatistics();
    assertEquals(89, keyIndex1Stats.getNumberOfBucketIndexes());

    // Initial stats test (keys, values & updates)
    assertEquals(2 * 100/* Num of values in region */, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());

    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(2 * 100/* Num of values in region */, keyIndex1Stats.getNumberOfKeys());
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

    assertEquals(100/* Num of values in region */, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());

    // Should not have any effect as invalidated values are destroyed
    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(100/* Num of values in region */, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());

    // NumOfKeys should get zero as all values are destroyed
    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(500, keyIndex1Stats.getNumUpdates());

    assertEquals(0, keyIndex1Stats.getNumberOfKeys());

    qs.removeIndex(keyIndex1);
    region.destroyRegion();
  }

  /**
   * Test CompactRenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForCompactRangeIndexBeforeRegionCreation() throws Exception {
    // Destroy region
    createRegion();
    assertEquals(0, region.size());

    keyIndex2 =
        (IndexProtocol) qs.createIndex("multiKeyIndex5", IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    // Recreate all entries in the region
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertTrue(keyIndex2 instanceof PartitionedIndex);

    IndexStatistics keyIndex1Stats = keyIndex2.getStatistics();
    assertEquals(89, keyIndex1Stats.getNumberOfBucketIndexes());

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
    region.destroyRegion();
  }

  /**
   * Test MapRenageIndex IndexStatistics for keys, values, updates and uses.
   *
   */
  @Test
  public void testStatsForCompactMapRangeIndexBeforeRegionCreation() throws Exception {
    // Destroy region
    createRegion();
    assertEquals(0, region.size());

    keyIndex3 = (IndexProtocol) qs.createIndex("multiKeyIndex6", IndexType.FUNCTIONAL,
        "positions['DELL', 'YHOO']", SEPARATOR + "portfolio");

    // Recreate all entries in the region
    Position.cnt = 0;
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    assertTrue(keyIndex3 instanceof PartitionedIndex);

    IndexStatistics keyIndexStats = keyIndex3.getStatistics();
    assertTrue(keyIndexStats instanceof IndexStatistics);
    assertEquals(89, keyIndexStats.getNumberOfBucketIndexes());

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, keyIndexStats.getNumberOfKeys());
    assertEquals(100, keyIndexStats.getNumberOfValues());
    assertEquals(100, keyIndexStats.getNumUpdates());

    Position.cnt = 0;
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, keyIndexStats.getNumberOfKeys());
    assertEquals(100, keyIndexStats.getNumberOfValues());
    assertEquals(200, keyIndexStats.getNumUpdates());

    String queryStr =
        "select * from " + SEPARATOR
            + "portfolio where positions['DELL'] != NULL OR positions['YHOO'] != NULL";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    assertEquals((0 /* Execution time */), keyIndexStats.getTotalUses());

    for (int i = 0; i < 50; i++) {
      region.invalidate(Integer.toString(i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, keyIndexStats.getNumberOfKeys());
    assertEquals(50, keyIndexStats.getNumberOfValues());
    assertEquals(250, keyIndexStats.getNumUpdates());

    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, keyIndexStats.getNumberOfKeys());
    assertEquals(50, keyIndexStats.getNumberOfValues());
    assertEquals(250, keyIndexStats.getNumUpdates());

    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(300, keyIndexStats.getNumUpdates());
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
  public void testStatsForMapRangeIndexBeforeRegionCreation() throws Exception {
    // Destroy region
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    createRegion();
    assertEquals(0, region.size());

    keyIndex3 = (IndexProtocol) qs.createIndex("multiKeyIndex6", IndexType.FUNCTIONAL,
        "positions['DELL', 'YHOO']", SEPARATOR + "portfolio");

    // Recreate all entries in the region
    Position.cnt = 0;
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    assertTrue(keyIndex3 instanceof PartitionedIndex);

    IndexStatistics keyIndexStats = keyIndex3.getStatistics();
    assertTrue(keyIndexStats instanceof IndexStatistics);
    assertEquals(89, keyIndexStats.getNumberOfBucketIndexes());

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, keyIndexStats.getNumberOfKeys());
    assertEquals(100, keyIndexStats.getNumberOfValues());
    assertEquals(100, keyIndexStats.getNumUpdates());

    Position.cnt = 0;
    for (int i = 0; i < 100; i++) {
      region.put(Integer.toString(i), new Portfolio(i, i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(100, keyIndexStats.getNumberOfKeys());
    assertEquals(100, keyIndexStats.getNumberOfValues());
    assertEquals(200, keyIndexStats.getNumUpdates());

    String queryStr =
        "select * from " + SEPARATOR
            + "portfolio where positions['DELL'] != NULL OR positions['YHOO'] != NULL";
    Query query = qs.newQuery(queryStr);

    for (int i = 0; i < 50; i++) {
      query.execute();
    }

    // Both RangeIndex should be used
    assertEquals(100 /* Execution time */, keyIndexStats.getTotalUses());

    for (int i = 0; i < 50; i++) {
      region.invalidate(Integer.toString(i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, keyIndexStats.getNumberOfKeys());
    assertEquals(50, keyIndexStats.getNumberOfValues());
    assertEquals(300, keyIndexStats.getNumUpdates());

    for (int i = 0; i < 50; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(50, keyIndexStats.getNumberOfKeys());
    assertEquals(50, keyIndexStats.getNumberOfValues());
    assertEquals(300, keyIndexStats.getNumUpdates());


    for (int i = 50; i < 100; i++) {
      region.destroy(Integer.toString(i));
    }

    assertEquals(2, keyIndexStats.getNumberOfMapIndexKeys());
    assertEquals(400, keyIndexStats.getNumUpdates());
    assertEquals(0, keyIndexStats.getNumberOfKeys());

    qs.removeIndex(keyIndex3);
    region.destroyRegion();
  }
}
