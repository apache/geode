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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Instrument;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.IndexTrackingQueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.util.internal.UncheckedUtils;

@Category({OQLIndexTest.class})
public class MapRangeIndexMaintenanceJUnitTest {

  private static final String INDEX_NAME = "keyIndex1";

  private static QueryService qs;
  private static Region region;
  private static Index keyIndex1;

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
    IndexManager.TEST_RANGEINDEX_ONLY = false;
  }

  @Test
  public void testNullMapAsValueOnIndexInitDoesNotThrowException() throws Exception {
    region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("portfolio");
    qs = CacheUtils.getQueryService();

    Portfolio p = new Portfolio(1, 1);
    p.positions = null;
    region.put(1, p);

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio ");

    SelectResults result = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = null")
        .execute();
    assertEquals(0, result.size());
  }

  @Test
  public void testMapIndexIsUsedWithBindKeyParameter() throws Exception {
    // Create Region
    region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("instrument");

    // Initialize observer
    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserverHolder.setInstance(observer);

    // Create map index
    qs = CacheUtils.getQueryService();
    qs.createIndex(INDEX_NAME, "tl.alternateReferences['SOME_KEY', 'SOME_OTHER_KEY']",
        SEPARATOR + "instrument i, i.tradingLines tl");

    // Add instruments
    int numInstruments = 20;
    for (int i = 0; i < numInstruments; i++) {
      String key = String.valueOf(i);
      Object value = Instrument.getInstrument(key);
      region.put(key, value);
    }

    // Execute query
    Query query = qs.newQuery(
        "<trace> select distinct i from " + SEPARATOR
            + "instrument i, i.tradingLines t where t.alternateReferences[$1]='SOME_VALUE'");
    SelectResults results = (SelectResults) query.execute(new Object[] {"SOME_KEY"});

    // Verify index was used
    assertTrue(observer.indexUsed);

    // Verify the results size
    assertEquals(numInstruments, results.size());
  }

  @Test
  public void testNullMapKeysInIndexOnLocalRegionForCompactMap() throws Exception {
    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", SEPARATOR + "portfolio p");

    assertTrue(keyIndex1 instanceof CompactMapRangeIndex);

    // Let MapRangeIndex remove values for key 1.
    Portfolio p = new Portfolio(1, 1);
    p.positions = new HashMap();
    region.put(1, p);

    // Now mapkeys are null for key 1
    region.invalidate(1);

    // Now mapkeys are null for key 1
    region.destroy(1);
  }

  @Test
  public void testNullMapKeysInIndexOnLocalRegion() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof MapRangeIndex);

    // Let MapRangeIndex remove values for key 1.
    Portfolio p = new Portfolio(1, 1);
    p.positions = new HashMap();
    region.put(1, p);

    // Now mapkeys are null for key 1
    region.invalidate(1);

    // Now mapkeys are null for key 1
    region.destroy(1);
  }

  /**
   * Test index object's comapreTo Function implementation correctness for indexes.
   */
  @Test
  public void testDuplicateKeysInCompactRangeIndexOnLocalRegion() throws Exception {
    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", new TestObject("SUN", 1));
    map1.put("IBM", new TestObject("IBM", 2));
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);

    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    assertTrue(keyIndex1 instanceof CompactMapRangeIndex);

    // Put duplicate TestObject with "IBM" name.
    region.put(Integer.toString(1), p);
    Portfolio p2 = new Portfolio(2, 2);
    HashMap map2 = new HashMap();
    map2.put("YHOO", new TestObject("YHOO", 3));
    map2.put("IBM", new TestObject("IBM", 2));

    p2.positions = map2;
    region.put(Integer.toString(2), p2);

    // Following destroy fails if fix for 44123 is not there.
    region.destroy(Integer.toString(1));
  }

  /**
   * Test index object's comapreTo Function implementation correctness for indexes.
   */
  @Test
  public void testDuplicateKeysInRangeIndexOnLocalRegion() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;

    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", new TestObject("SUN", 1));
    map1.put("IBM", new TestObject("IBM", 2));
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);

    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    assertTrue(keyIndex1 instanceof MapRangeIndex);

    // Put duplicate TestObject with "IBM" name.
    region.put(Integer.toString(1), p);
    Portfolio p2 = new Portfolio(2, 2);
    HashMap map2 = new HashMap();
    map2.put("YHOO", new TestObject("YHOO", 3));
    map2.put("IBM", new TestObject("IBM", 2));

    p2.positions = map2;
    region.put(Integer.toString(2), p2);

    // Following destroy fails if fix for 44123 is not there.
    region.destroy(Integer.toString(1));
  }

  @Test
  public void testUndefinedForMapRangeIndex() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    keyIndex1 = qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", SEPARATOR + "portfolio ");
    assertTrue("Index should be a MapRangeIndex ", keyIndex1 instanceof MapRangeIndex);

    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        // add some string objects generating UNDEFINEDs as index keys
        if (i % 2 == 0) {
          region.put(Integer.toString(i), "Portfolio-" + i);
        } else {
          region.put(Integer.toString(i), new Portfolio(i, i));
        }
      }
    }
    assertEquals(100, region.size());

    qs.removeIndexes();

    // recreate index to verify they get updated correctly
    keyIndex1 = qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", SEPARATOR + "portfolio ");
    assertTrue("Index should be a MapRangeIndex ", keyIndex1 instanceof MapRangeIndex);
  }

  @Test
  public void testUndefinedForCompactMapRangeIndex() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    keyIndex1 = qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", SEPARATOR + "portfolio ");
    assertTrue("Index should be a CompactMapRangeIndex ",
        keyIndex1 instanceof CompactMapRangeIndex);

    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        // add some string objects generating UNDEFINEDs as index keys
        if (i % 2 == 0) {
          region.put(Integer.toString(i), "Portfolio-" + i);
        } else {
          region.put(Integer.toString(i), new Portfolio(i, i));
        }
      }
    }
    assertEquals(100, region.size());

    qs.removeIndexes();

    // recreate index to verify they get updated correctly
    keyIndex1 = qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", SEPARATOR + "portfolio ");
    assertTrue("Index should be a CompactMapRangeIndex ",
        keyIndex1 instanceof CompactMapRangeIndex);
  }

  @Test
  public void testNullMapValuesInIndexOnLocalRegionForCompactMap() throws Exception {
    region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("portfolio");
    qs = CacheUtils.getQueryService();
    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio ");

    Portfolio p = new Portfolio(1, 1);
    p.positions = new HashMap();
    region.put(1, p);

    Portfolio p2 = new Portfolio(2, 2);
    p2.positions = null;
    region.put(2, p2);

    Portfolio p3 = new Portfolio(3, 3);
    p3.positions = new HashMap();
    p3.positions.put("IBM", "something");
    p3.positions.put("SUN", null);
    region.put(3, p3);
    region.put(3, p3);

    SelectResults result = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = null")
        .execute();
    assertEquals(1, result.size());
  }

  @Test
  public void testQueriesForValueInMapFieldWithoutIndex() throws Exception {
    region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("portfolio");
    qs = CacheUtils.getQueryService();
    testQueriesForValueInMapField(region, qs);
  }

  @Test
  public void testQueriesForValueInMapFieldWithMapIndexWithOneKey() throws Exception {
    region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("portfolio");
    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions['SUN']", SEPARATOR + "portfolio ");
    assertThat(keyIndex1).isInstanceOf(CompactRangeIndex.class);
    testQueriesForValueInMapField(region, qs);

    long keys = ((CompactRangeIndex) keyIndex1).internalIndexStats.getNumberOfKeys();
    long mapIndexKeys =
        ((CompactRangeIndex) keyIndex1).internalIndexStats.getNumberOfMapIndexKeys();
    long values =
        ((CompactRangeIndex) keyIndex1).internalIndexStats.getNumberOfValues();
    long uses =
        ((CompactRangeIndex) keyIndex1).internalIndexStats.getTotalUses();

    // The number of keys must be equal to the number of different values the
    // positions map takes in region entries for the 'SUN' key:
    // ("nothing", "more", null) + 1 for any entry that does not have
    // a value for the "SUN" key (UNDEFINED)
    assertThat(keys).isEqualTo(4);
    // mapIndexKeys must be zero because the index used is a range index and not a map index
    assertThat(mapIndexKeys).isEqualTo(0);
    // The number of values must be equal to the number of region entries
    assertThat(values).isEqualTo(7);
    // The index must be used in every query
    assertThat(uses).isEqualTo(4);
  }

  @Test
  public void testQueriesForValueInMapFieldWithMapIndexWithSeveralKeys() throws Exception {
    region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("portfolio");
    qs = CacheUtils.getQueryService();

    keyIndex1 =
        qs.createIndex(INDEX_NAME, "positions['SUN', 'ERICSSON']", SEPARATOR + "portfolio ");
    assertThat(keyIndex1).isInstanceOf(CompactMapRangeIndex.class);
    testQueriesForValueInMapField(region, qs);

    long keys = ((CompactMapRangeIndex) keyIndex1).internalIndexStats.getNumberOfKeys();
    long mapIndexKeys =
        ((CompactMapRangeIndex) keyIndex1).internalIndexStats.getNumberOfMapIndexKeys();
    long values =
        ((CompactMapRangeIndex) keyIndex1).internalIndexStats.getNumberOfValues();
    long uses =
        ((CompactMapRangeIndex) keyIndex1).internalIndexStats.getTotalUses();

    // The number of keys must be equal to the number of different values the
    // positions map takes for the 'SUN' key (null, "nothing", "more") plus
    // the number of different values the positions map takes for the 'ERICSSON' key
    // ("hey") for each entry in the region.
    assertThat(keys).isEqualTo(4);
    // The number of mapIndexKeys must be equal to the number of keys
    // in the index that appear in region entries:
    // 'SUN', 'ERICSSON'
    assertThat(mapIndexKeys).isEqualTo(2);
    // The number of values must be equal to the number of values the
    // positions map takes for each key indexed in the map
    // for each entry in the region:
    // null, "nothing", "more", "hey", "more"
    assertThat(values).isEqualTo(5);
    // The index must not be used in queries with "!="
    assertThat(uses).isEqualTo(2);
  }

  @Test
  public void testQueriesForValueInMapFieldWithMapIndexWithStar() throws Exception {
    region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("portfolio");
    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio ");
    assertThat(keyIndex1).isInstanceOf(CompactMapRangeIndex.class);
    testQueriesForValueInMapField(region, qs);

    long keys = ((CompactMapRangeIndex) keyIndex1).internalIndexStats.getNumberOfKeys();
    long mapIndexKeys =
        ((CompactMapRangeIndex) keyIndex1).internalIndexStats.getNumberOfMapIndexKeys();
    long values =
        ((CompactMapRangeIndex) keyIndex1).internalIndexStats.getNumberOfValues();
    long uses =
        ((CompactMapRangeIndex) keyIndex1).internalIndexStats.getTotalUses();

    // The number of keys must be equal to the number of different values the
    // positions map takes for each key (null not included)
    // for each entry in the region:
    // "something", null, "nothing", "more", "hey", "tip"
    assertThat(keys).isEqualTo(6);
    // The number of mapIndexKeys must be equal to the number of different keys
    // that appear in entries of the region:
    // "IBM", "ERICSSON", "HP", "SUN", null
    assertThat(mapIndexKeys).isEqualTo(5);
    // The number of values must be equal to the number of values the
    // positions map takes for each key (null not included)
    // for each entry in the region:
    // "something", null, "nothing", "more", "hey", "more", "tip"
    assertThat(values).isEqualTo(7);
    // The index must not be used in queries with "!="
    assertThat(uses).isEqualTo(2);
  }

  public void testQueriesForValueInMapField(Region<Object, Object> region, QueryService qs)
      throws Exception {

    // Empty map
    Portfolio p = new Portfolio(1, 1);
    p.positions = new HashMap<>();
    region.put(1, p);

    // Map is null
    Portfolio p2 = new Portfolio(2, 2);
    p2.positions = null;
    region.put(2, p2);

    // Map with null value for "SUN" key
    Portfolio p3 = new Portfolio(3, 3);
    p3.positions = new HashMap<>();
    p3.positions.put("IBM", "something");
    p3.positions.put("SUN", null);
    region.put(3, p3);

    // Map with not null value for "SUN" key
    Portfolio p4 = new Portfolio(4, 4);
    p4.positions = new HashMap<>();
    p4.positions.put("SUN", "nothing");
    region.put(4, p4);

    // Map with null key
    Portfolio p5 = new Portfolio(5, 5);
    p5.positions = new HashMap<>();
    p5.positions.put("SUN", "more");
    // The next one causes trouble with gfsh as json cannot show maps with null keys
    p5.positions.put(null, "empty");
    region.put(5, p5);

    // One more with map without the "SUN" key
    Portfolio p6 = new Portfolio(6, 6);
    p6.positions = new HashMap<>();
    p6.positions.put("ERICSSON", "hey");
    region.put(6, p6);

    // Map with a repeated value for the "SUN" key
    Portfolio p7 = new Portfolio(7, 7);
    p7.positions = new HashMap<>();
    p7.positions.put("SUN", "more");
    p7.positions.put("HP", "tip");
    region.put(7, p7);

    String query;
    query = "select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = null";
    SelectResults<Object> result = UncheckedUtils.uncheckedCast(qs
        .newQuery(query)
        .execute());
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.contains(p3)).isTrue();

    query = "select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] != null";
    result = UncheckedUtils.uncheckedCast(qs
        .newQuery(query)
        .execute());
    assertThat(result.size()).isEqualTo(6);
    assertThat(result.containsAll(Arrays.asList(p, p2, p4, p5, p6, p7))).isTrue();

    query = "select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 'nothing'";
    result = UncheckedUtils.uncheckedCast(qs
        .newQuery(query)
        .execute());
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.contains(p4)).isTrue();

    query = "select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] != 'nothing'";
    result = UncheckedUtils.uncheckedCast(qs
        .newQuery(query)
        .execute());
    assertThat(result.size()).isEqualTo(6);
    assertThat(result.containsAll(Arrays.asList(p, p2, p3, p5, p6, p7))).isTrue();

    query = "select * from " + SEPARATOR + "portfolio p";
    result = UncheckedUtils.uncheckedCast(qs
        .newQuery(query)
        .execute());
    assertThat(result.size()).isEqualTo(7);
    assertThat(result.containsAll(Arrays.asList(p, p2, p3, p4, p5, p6, p7))).isTrue();
  }

  @Test
  public void testNullMapValuesInIndexOnLocalRegionForMap() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("portfolio");
    qs = CacheUtils.getQueryService();
    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio ");

    Portfolio p = new Portfolio(1, 1);
    p.positions = new HashMap();
    region.put(1, p);

    Portfolio p2 = new Portfolio(2, 2);
    p2.positions = null;
    region.put(2, p2);

    Portfolio p3 = new Portfolio(3, 3);
    p3.positions = new HashMap();
    p3.positions.put("SUN", null);
    region.put(3, p3);

    SelectResults result = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = null")
        .execute();
    assertEquals(1, result.size());
  }

  @Test
  public void updatingAMapFieldWithDifferentKeysShouldRemoveOldKeysThatAreNoLongerPresent()
      throws Exception {
    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    region.put(1, p);
    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    assertTrue(keyIndex1 instanceof CompactMapRangeIndex);

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    map2.put("NEW_KEY", 1);
    region.put(1, p2);

    SelectResults results = (SelectResults) qs
        .newQuery(
            "select * from " + SEPARATOR
                + "portfolio p where p.positions['SUN'] = 1 OR p.positions['IBM'] = 2")
        .execute();
    assertEquals(0, results.size());
  }

  @Test
  public void updatingAMapFieldSameKeysSameValuesShouldUpdateCorrectly() throws Exception {
    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    region.put(1, p);
    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    assertTrue(keyIndex1 instanceof CompactMapRangeIndex);

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    map2.put("SUN", 1);
    map2.put("IBM", 2);
    region.put(1, p2);

    SelectResults results = (SelectResults) qs
        .newQuery(
            "select * from " + SEPARATOR
                + "portfolio p where p.positions['SUN'] = 1 OR p.positions['IBM'] = 2")
        .execute();
    assertEquals(1, results.size());
  }

  @Test
  public void updatingAMapFieldWithNoKeysShouldRemoveOldKeysThatAreNoLongerPresent()
      throws Exception {
    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    region.put(1, p);
    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    region.put(1, p2);

    SelectResults results = (SelectResults) qs
        .newQuery(
            "select * from " + SEPARATOR
                + "portfolio p where p.positions['SUN'] = 1 OR p.positions['IBM'] = 2")
        .execute();
    assertEquals(0, results.size());
  }

  @Test
  public void updatingEmptyMapToMapWithKeysShouldIndexNewKeysCorrectly() throws Exception {
    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    region.put(1, p);
    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    map2.put("SUN", 1);
    map2.put("IBM", 2);
    region.put(1, p2);

    SelectResults results = (SelectResults) qs
        .newQuery(
            "select * from " + SEPARATOR
                + "portfolio p where p.positions['SUN'] = 1 OR p.positions['IBM'] = 2")
        .execute();
    assertEquals(1, results.size());
  }

  @Test
  public void testUpdateWithSameKeysSameValuesShouldRetainIndexMappings() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    region.put(1, p);
    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");
    region.put(1, p);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());
  }

  @Test
  public void updatingWithSameKeysDifferentValuesShouldRetainIndexMappings() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    region.put(1, p);
    qs = CacheUtils.getQueryService();

    keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");
    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    map2.put("SUN", 3);
    map2.put("IBM", 4);
    region.put(1, p2);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 3")
        .execute();
    assertEquals(1, results.size());
  }

  @Test
  public void updatingWithSameKeysDifferentValuesAfterRemovingShouldReinsertIndexMappings()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    map2.put("GOOG", 1);
    region.put(1, p2);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map3.put("SUN", 3);
    map3.put("IBM", 4);
    region.put(1, p3);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 3")
        .execute();
    assertEquals(1, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['GOOG'] = 1")
        .execute();

    assertEquals(0, results.size());
  }

  @Test
  public void updatingWithSameKeysDifferentValuesAfterRemovingUsingRegionDestroyShouldReinsertIndexMappings()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    region.destroy(1);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map3.put("SUN", 3);
    map3.put("IBM", 4);
    region.put(1, p3);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 3")
        .execute();
    assertEquals(1, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['GOOG'] = 1")
        .execute();

    assertEquals(0, results.size());
  }

  @Test
  public void updatingWithSameKeysDifferentValuesAfterRemovingUsingInplaceModificationShouldReinsertIndexMappings()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    map1.remove("SUN");
    map1.remove("IBM");
    map1.put("GOOG", 1);

    region.put(1, p1);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map3.put("SUN", 3);
    map3.put("IBM", 4);
    region.put(1, p3);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 3")
        .execute();
    assertEquals(1, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['GOOG'] = 1")
        .execute();

    assertEquals(0, results.size());
  }

  @Test
  public void updatingWithSameKeysSameValuesAfterRemovingShouldReinsertIndexMappings()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    map2.put("GOOG", 1);
    region.put(1, p2);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map3.put("SUN", 1);
    map3.put("IBM", 2);
    region.put(1, p3);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['IBM'] = 2")
        .execute();
    assertEquals(1, results.size());
  }

  @Test
  public void updatingWithSameKeysSameValuesAfterRemovingUsingRegionDestroyShouldReinsertIndexMappings()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    region.destroy(1);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map3.put("SUN", 1);
    map3.put("IBM", 2);
    region.put(1, p3);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['IBM'] = 2")
        .execute();
    assertEquals(1, results.size());
  }

  @Test
  public void updatingWithSameKeysSameValuesAfterRemovingUsingInPlaceModificationShouldReinsertIndexMappings()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    map1.remove("SUN");
    map1.remove("IBM");

    map1.put("GOOG", 2);
    region.put(1, p1);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map3.put("SUN", 1);
    map3.put("IBM", 2);
    region.put(1, p3);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['IBM'] = 2")
        .execute();
    assertEquals(1, results.size());
  }

  @Test
  public void updatingWithSameKeysSameValuesAfterRemovingShouldReinsertIndexMappingsWithTwoRegionPuts()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    map2.put("SUN", 1);
    map2.put("IBM", 2);
    p2.positions = map1;
    region.put(2, p2);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(2, results.size());

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map2.put("GOOG", 1);
    region.put(1, p3);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    Portfolio p4 = new Portfolio(1, 1);
    HashMap map4 = new HashMap();
    p4.positions = map4;
    map4.put("SUN", 1);
    map4.put("IBM", 2);
    region.put(1, p4);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(2, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['IBM'] = 2")
        .execute();
    assertEquals(2, results.size());
  }

  @Test
  public void updatingWithSameKeysSameValuesAfterRemovingUsingRegionDestroyShouldReinsertIndexMappingsWithTwoRegionPuts()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    map2.put("SUN", 1);
    map2.put("IBM", 2);
    p2.positions = map1;
    region.put(2, p2);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(2, results.size());

    region.destroy(1);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map3.put("SUN", 1);
    map3.put("IBM", 2);
    region.put(1, p3);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(2, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['IBM'] = 2")
        .execute();
    assertEquals(2, results.size());
  }

  @Test
  public void updatingWithSameKeysSameValuesAfterRemovingUsingInPlaceModificationShouldReinsertIndexMappingsWithTwoRegionPuts()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    map2.put("SUN", 1);
    map2.put("IBM", 2);
    p2.positions = map2;
    region.put(2, p2);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(2, results.size());

    map1.remove("SUN");
    map1.remove("IBM");

    map1.put("GOOG", 2);
    region.put(1, p1);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map3.put("SUN", 1);
    map3.put("IBM", 2);
    region.put(1, p3);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(2, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['IBM'] = 2")
        .execute();
    assertEquals(2, results.size());
  }

  @Test
  public void updatingWithSameKeysDifferentValuesAfterRemovingShouldReinsertIndexMappingsWithTwoRegionPuts()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map3.put("SUN", 5);
    map3.put("IBM", 6);
    region.put(2, p3);

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    map2.put("GOOG", 1);
    region.put(1, p2);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    Portfolio p4 = new Portfolio(1, 1);
    HashMap map4 = new HashMap();
    p4.positions = map4;
    map4.put("SUN", 3);
    map4.put("IBM", 4);
    region.put(1, p4);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 3")
        .execute();
    assertEquals(1, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['GOOG'] = 1")
        .execute();

    assertEquals(0, results.size());
  }

  @Test
  public void updatingWithSameKeysDifferentValuesAfterRemovingUsingRegionDestroyShouldReinsertIndexMappingsWithTwoRegionPuts()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    map2.put("SUN", 5);
    map2.put("IBM", 6);
    p2.positions = map2;
    region.put(2, p2);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    region.destroy(1);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map3.put("SUN", 3);
    map3.put("IBM", 4);
    region.put(1, p3);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 3")
        .execute();
    assertEquals(1, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['GOOG'] = 1")
        .execute();

    assertEquals(0, results.size());
  }

  @Test
  public void updatingWithSameKeysDifferentValuesAfterRemovingUsingInplaceModificationShouldReinsertIndexMappingsWithTwoRegionPuts()
      throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();
    Index keyIndex1 = qs.createIndex(INDEX_NAME, "positions[*]", SEPARATOR + "portfolio");

    Portfolio p1 = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    p1.positions = map1;
    region.put(1, p1);

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    map2.put("SUN", 5);
    map2.put("IBM", 6);
    p2.positions = map2;
    region.put(2, p2);

    SelectResults results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(1, results.size());

    map1.remove("SUN");
    map1.remove("IBM");
    map1.put("GOOG", 1);

    region.put(1, p1);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    Portfolio p3 = new Portfolio(1, 1);
    HashMap map3 = new HashMap();
    p3.positions = map3;
    map3.put("SUN", 3);
    map3.put("IBM", 4);
    region.put(1, p3);

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 1")
        .execute();
    assertEquals(0, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['SUN'] = 3")
        .execute();
    assertEquals(1, results.size());

    results = (SelectResults) qs
        .newQuery("select * from " + SEPARATOR + "portfolio p where p.positions['GOOG'] = 1")
        .execute();

    assertEquals(0, results.size());
  }

  /**
   * TestObject with wrong comareTo() implementation implementation. Which throws NullPointer while
   * removing mapping from a MapRangeIndex.
   */
  private static class TestObject implements Cloneable, Comparable {
    String name;
    int id;

    public TestObject(String name, int id) {
      this.name = name;
      this.id = id;
    }

    @Override
    public int compareTo(Object o) {
      if (id == ((TestObject) o).id) {
        return 0;
      } else {
        return id > ((TestObject) o).id ? -1 : 1;
      }
    }
  }

  private static class MyQueryObserverAdapter extends IndexTrackingQueryObserver {
    public boolean indexUsed = false;

    @Override
    public void afterIndexLookup(Collection results) {
      super.afterIndexLookup(results);
      indexUsed = true;
    }
  }
}
