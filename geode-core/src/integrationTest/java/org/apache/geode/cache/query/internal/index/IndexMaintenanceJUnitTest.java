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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.AbstractIndex.RegionEntryToValuesMap;
import org.apache.geode.cache.query.internal.index.IndexStore.IndexStoreEntry;
import org.apache.geode.cache.query.internal.index.MemoryIndexStore.MemoryIndexStoreEntry;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexMaintenanceJUnitTest {

  private QueryService qs;
  private Region region;
  private Set idSet;

  @Before
  public void setUp() throws Exception {
    idSet = new HashSet();

    CacheUtils.startCache();
    Cache cache = CacheUtils.getCache();
    region = CacheUtils.createRegion("portfolio", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    for (int j = 0; j < 6; ++j) {
      idSet.add(j + "");
    }
    qs = cache.getQueryService();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
    DefaultQueryService.TEST_QUERY_HETEROGENEOUS_OBJECTS = false;
    IndexManager.TEST_RANGEINDEX_ONLY = false;
  }

  /**
   * Tests Index maintenance on heterogenous objects
   */
  @Test
  public void testIndexMaintenanceWithHeterogenousObjects() throws Exception {
    DefaultQueryService.TEST_QUERY_HETEROGENEOUS_OBJECTS = true;
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    Cache cache = CacheUtils.getCache();
    qs = cache.getQueryService();
    region = CacheUtils.createRegion("portfolio1", null);
    idSet.clear();
    Portfolio p = new Portfolio(4);
    region.put("4", p);
    idSet.add("" + p.getID());
    p = new Portfolio(5);
    region.put("5", p);
    idSet.add("" + p.getID());
    region.put("6", 6);
    Index i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID()", SEPARATOR + "portfolio1 pf");
    RangeIndex ri = (RangeIndex) i1;
    assertEquals(2, ri.valueToEntriesMap.size());
    Iterator itr = ri.valueToEntriesMap.values().iterator();
    while (itr.hasNext()) {
      RangeIndex.RegionEntryToValuesMap re2ValMap = (RangeIndex.RegionEntryToValuesMap) itr.next();
      assertEquals(1, re2ValMap.map.size());
      Object obj = re2ValMap.map.values().iterator().next();
      assertFalse(obj instanceof Collection);
      assertTrue(obj instanceof Portfolio);
      Portfolio pf = (Portfolio) obj;
      assertTrue(idSet.contains(String.valueOf(pf.getID())));
    }
    assertEquals(1, ri.undefinedMappedEntries.map.size());
    Map.Entry entry = (Map.Entry) ri.undefinedMappedEntries.map.entrySet().iterator().next();
    assertFalse(entry.getValue() instanceof Collection);
    assertTrue(entry.getValue() instanceof Integer);
    assertTrue(entry.getValue().equals(6));

    region.put("7", 7);
    idSet.add(7);
    assertEquals(2, ri.undefinedMappedEntries.map.size());
    itr = ri.undefinedMappedEntries.map.entrySet().iterator();
    while (itr.hasNext()) {
      entry = (Map.Entry) itr.next();
      assertFalse(entry.getValue() instanceof Collection);
      assertTrue(entry.getValue() instanceof Integer);
      idSet.contains(entry.getValue());
    }

    region.remove("7");
    idSet.remove(7);
    Index i2 =
        qs.createIndex("indx2", IndexType.FUNCTIONAL, "pf.pkid", SEPARATOR + "portfolio1 pf");
    ri = (RangeIndex) i2;
    assertEquals(2, ri.valueToEntriesMap.size());
    itr = ri.valueToEntriesMap.values().iterator();
    while (itr.hasNext()) {
      RangeIndex.RegionEntryToValuesMap re2ValMap = (RangeIndex.RegionEntryToValuesMap) itr.next();
      assertEquals(1, re2ValMap.map.size());
      Object obj = re2ValMap.map.values().iterator().next();
      assertFalse(obj instanceof Collection);
      assertTrue(obj instanceof Portfolio);
      Portfolio pf = (Portfolio) obj;
      assertTrue(idSet.contains(String.valueOf(pf.getID())));
    }
    assertEquals(1, ri.undefinedMappedEntries.map.size());
    entry = (Map.Entry) ri.undefinedMappedEntries.map.entrySet().iterator().next();
    assertFalse(entry.getValue() instanceof Collection);
    assertTrue(entry.getValue() instanceof Integer);
    assertTrue(entry.getValue().equals(6));

    region.put("7", 7);
    idSet.add(7);
    assertEquals(2, ri.undefinedMappedEntries.map.size());
    itr = ri.undefinedMappedEntries.map.entrySet().iterator();
    while (itr.hasNext()) {
      entry = (Map.Entry) itr.next();
      assertFalse(entry.getValue() instanceof Collection);
      assertTrue(entry.getValue() instanceof Integer);
      idSet.contains(entry.getValue());
    }
  }

  /**
   * Tests query on region containing heterogenous objects
   */
  @Test
  public void testQueryOnHeterogenousObjects() throws Exception {
    DefaultQueryService.TEST_QUERY_HETEROGENEOUS_OBJECTS = true;
    Cache cache = CacheUtils.getCache();
    region = CacheUtils.createRegion("portfolio1", null);
    for (int i = 0; i < 5; ++i) {
      Portfolio p = new Portfolio(i + 1);
      region.put(i + 1, p);
    }

    for (int i = 5; i < 10; ++i) {
      region.put(i + 1, i + 1);
    }
    String queryStr =
        "Select distinct * from " + SEPARATOR + "portfolio1 pf1 where pf1.getID() > 3";
    Query q = qs.newQuery(queryStr);
    SelectResults rs = (SelectResults) q.execute();
    assertEquals(2, rs.size());
    Iterator itr = rs.iterator();
    while (itr.hasNext()) {
      Portfolio p = (Portfolio) itr.next();
      assertTrue(p == region.get(4) || p == region.get(5));
    }

    Index i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID()", SEPARATOR + "portfolio1 pf");
    QueryObserver old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private boolean indexUsed = false;

      @Override
      public void beforeIndexLookup(Index index, int oper, Object key) {
        indexUsed = true;
      }

      @Override
      public void endQuery() {
        assertTrue(indexUsed);
      }
    });

    rs = (SelectResults) q.execute();
    assertEquals(2, rs.size());
    itr = rs.iterator();
    while (itr.hasNext()) {
      Portfolio p = (Portfolio) itr.next();
      assertTrue(p == region.get(4) || p == region.get(5));
    }
    qs.removeIndex(i1);

    queryStr = "Select distinct * from " + SEPARATOR + "portfolio1 pf1 where pf1.pkid > '3'";
    q = qs.newQuery(queryStr);
    rs = (SelectResults) q.execute();
    assertEquals(2, rs.size());
    itr = rs.iterator();
    while (itr.hasNext()) {
      Portfolio p = (Portfolio) itr.next();
      assertTrue(p == region.get(4) || p == region.get(5));
    }

    i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.pkid", SEPARATOR + "portfolio1 pf");
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private boolean indexUsed = false;

      @Override
      public void beforeIndexLookup(Index index, int oper, Object key) {
        indexUsed = true;
      }

      @Override
      public void endQuery() {
        assertTrue(indexUsed);
      }
    });

    rs = (SelectResults) q.execute();
    assertEquals(2, rs.size());
    itr = rs.iterator();
    while (itr.hasNext()) {
      Portfolio p = (Portfolio) itr.next();
      assertTrue(p == region.get(4) || p == region.get(5));
    }
  }

  /**
   * Tests Index maintenance on method Keys() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodKeys() throws Exception {
    Index i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "ks.toString",
        SEPARATOR + "portfolio.keys() ks");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    CompactRangeIndex ri = (CompactRangeIndex) i1;
    validateIndexForKeys(ri);
  }

  /**
   * Tests Index maintenance on method asList() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodAsList() throws Exception {
    Index i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID",
        SEPARATOR + "portfolio.asList() pf");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    CompactRangeIndex ri = (CompactRangeIndex) i1;
    validateIndexForValues(ri);
  }

  /**
   * Tests Index maintenance on method values() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodValues() throws Exception {
    Index i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID",
        SEPARATOR + "portfolio.values() pf");
    assertTrue(i1 instanceof CompactRangeIndex);
    Cache cache = CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    CompactRangeIndex ri = (CompactRangeIndex) i1;
    validateIndexForValues(ri);
  }

  /**
   * Tests Index maintenance on method getValues() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodGetValues() throws Exception {
    Index i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID",
            SEPARATOR + "portfolio.getValues() pf");
    assertTrue(i1 instanceof CompactRangeIndex);
    Cache cache = CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    CompactRangeIndex ri = (CompactRangeIndex) i1;
    validateIndexForValues(ri);
  }

  /**
   * Tests Index maintenance on method toArray() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodtoArray() throws Exception {
    Index i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID",
        SEPARATOR + "portfolio.toArray() pf");
    assertTrue(i1 instanceof CompactRangeIndex);
    Cache cache = CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    CompactRangeIndex ri = (CompactRangeIndex) i1;
    validateIndexForValues(ri);
  }

  /**
   * Tests Index maintenance on method asSet() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodAsSet() throws Exception {
    Index i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID",
        SEPARATOR + "portfolio.asSet() pf");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    CompactRangeIndex ri = (CompactRangeIndex) i1;
    validateIndexForValues(ri);
  }

  /**
   * Tests Index maintenance on method keySet() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodKeySet() throws Exception {
    Index i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "ks.toString",
            SEPARATOR + "portfolio.keySet() ks");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    CompactRangeIndex ri = (CompactRangeIndex) i1;
    validateIndexForKeys(ri);
  }

  /**
   * Tests Index maintenance on method getKeys() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodGetKeys() throws Exception {
    Index i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "ks.toString",
            SEPARATOR + "portfolio.getKeys() ks");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    CompactRangeIndex ri = (CompactRangeIndex) i1;
    validateIndexForKeys(ri);
  }

  /**
   * Tests Index maintenance on method entrySet() as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodEntrySet() throws Exception {
    Index i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "entries.value.getID",
        SEPARATOR + "portfolio.entrySet() entries");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    CompactRangeIndex ri = (CompactRangeIndex) i1;
    validateIndexForEntries(ri);
  }

  /**
   * Tests Index maintenance on method getEntries( ) as iterator ( with focus on behaviour if not
   * implemented in DummyQRegion
   */
  @Test
  public void testIndexMaintenanceWithIndexOnMethodGetEntries() throws Exception {
    Index i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "entries.value.getID",
        SEPARATOR + "portfolio.getEntries() entries");
    CacheUtils.getCache();
    region = CacheUtils.getRegion(SEPARATOR + "portfolio");
    region.put("4", new Portfolio(4));
    region.put("5", new Portfolio(5));
    CompactRangeIndex ri = (CompactRangeIndex) i1;
    validateIndexForEntries(ri);
  }

  @Test
  public void testMapKeyIndexMaintenanceForNonCompactTypeAllKeysIndex() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion) CacheUtils.createRegion("testRgn", null);
    int ID = 1;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (; ID <= 5; ++ID) {
      MapKeyIndexData mkid = new MapKeyIndexData(ID);
      for (int j = 1; j <= ID; ++j) {
        mkid.maap.put("key" + j, "val" + j);
      }
      testRgn.put(ID, mkid);
    }
    --ID;
    Index i1 =
        qs.createIndex("Index1", IndexType.FUNCTIONAL, "objs.maap[*]", SEPARATOR + "testRgn objs");
    assertEquals(i1.getCanonicalizedIndexedExpression(), "index_iter1.maap[*]");
    assertTrue(i1 instanceof MapRangeIndex);
    MapRangeIndex mri = (MapRangeIndex) i1;
    // Test index maintenance
    // addition of new Portfolio object
    Map<Object, AbstractIndex> indxMap = mri.getRangeIndexHolderForTesting();
    assertEquals(indxMap.size(), ID);
    for (int j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      RangeIndex rng = (RangeIndex) indxMap.get("key" + j);
      Iterator itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        RegionEntryToValuesMap entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (Integer elem : expectedElements) {
          RegionEntry re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    // addition of new Portfolio in the Map
    ++ID;// ID = 6;
    MapKeyIndexData mkid = new MapKeyIndexData(ID);
    for (int j = 1; j <= ID; ++j) {
      mkid.maap.put("key" + j, "val" + j);
    }
    testRgn.put(ID, mkid);
    assertEquals(indxMap.size(), ID);
    for (int j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      RangeIndex rng = (RangeIndex) indxMap.get("key" + j);
      Iterator itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        RegionEntryToValuesMap entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (Integer elem : expectedElements) {
          RegionEntry re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    // addition of new key in the positions map
    mkid.maap.put("key7", "val7");
    testRgn.put(ID, mkid);
    assertEquals(indxMap.size(), 7);

    for (int j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      RangeIndex rng = (RangeIndex) indxMap.get("key" + j);
      Iterator itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        RegionEntryToValuesMap entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (Integer elem : expectedElements) {
          RegionEntry re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    assertTrue(indxMap.containsKey("key7"));
    RangeIndex rng = (RangeIndex) indxMap.get("key7");
    Iterator itr = rng.valueToEntriesMap.values().iterator();
    assertEquals(rng.valueToEntriesMap.size(), 1);
    assertTrue(rng.valueToEntriesMap.containsKey("val7"));
    RegionEntryToValuesMap entryMap = (RegionEntryToValuesMap) itr.next();
    assertEquals(1, entryMap.getNumEntries());
    RegionEntry re = testRgn.basicGetEntry(6);
    entryMap.containsEntry(re);
    // deletion of key in the positions map
    mkid.maap.remove("key7");
    testRgn.put(ID, mkid);
    assertEquals(indxMap.size(), ID + 1);
    for (int j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      rng = (RangeIndex) indxMap.get("key" + j);
      itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (Integer elem : expectedElements) {
          re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    // update of key in the positions map
    mkid = (MapKeyIndexData) testRgn.get(1);
    mkid.maap.put("key1", "val2");
    testRgn.put(1, mkid);
    assertEquals(indxMap.size(), ID + 1);
    for (int j = 1; j <= ID; ++j) {
      String keey = "key" + j;
      assertTrue(indxMap.containsKey(keey));
      rng = (RangeIndex) indxMap.get(keey);
      itr = rng.valueToEntriesMap.values().iterator();
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        assertTrue(rng.valueToEntriesMap.containsKey("val1"));
        assertTrue(rng.valueToEntriesMap.containsKey("val2"));
      } else {
        assertEquals(rng.valueToEntriesMap.size(), 1);
        assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      }

      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val1");
        assertEquals(5, entryMap.getNumEntries());
        expectedElements.remove(1);
        for (Integer elem : expectedElements) {
          re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val2");
        assertEquals(1, entryMap.getNumEntries());
        re = testRgn.basicGetEntry(1);
        assertTrue(entryMap.containsEntry(re));

      } else {
        while (itr.hasNext()) {
          entryMap = (RegionEntryToValuesMap) itr.next();
          assertEquals(ID + 1 - j, entryMap.getNumEntries());
          for (Integer elem : expectedElements) {
            re = testRgn.basicGetEntry(elem);

            assertTrue(entryMap.containsEntry(re));
          }
        }
      }
    }
    // deletion of portfolio object key in the positions map
    testRgn.remove(ID);
    --ID;// ID = 5;
    // No Key Indexes are removed from a MapRangeIndex even if they are empty.
    assertEquals(indxMap.size(), ID + 2);
    for (int j = 1; j <= ID; ++j) {
      String keey = "key" + j;
      assertTrue(indxMap.containsKey(keey));
      rng = (RangeIndex) indxMap.get(keey);
      itr = rng.valueToEntriesMap.values().iterator();
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        assertTrue(rng.valueToEntriesMap.containsKey("val1"));
        assertTrue(rng.valueToEntriesMap.containsKey("val2"));
      } else {
        assertEquals(rng.valueToEntriesMap.size(), 1);
        assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      }

      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val1");
        assertEquals(4, entryMap.getNumEntries());
        expectedElements.remove(1);
        for (Integer elem : expectedElements) {
          re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val2");
        assertEquals(1, entryMap.getNumEntries());
        re = testRgn.basicGetEntry(1);
        assertTrue(entryMap.containsEntry(re));

      } else {
        while (itr.hasNext()) {
          entryMap = (RegionEntryToValuesMap) itr.next();
          assertEquals(ID + 1 - j, entryMap.getNumEntries());
          for (Integer elem : expectedElements) {
            re = testRgn.basicGetEntry(elem);

            assertTrue(entryMap.containsEntry(re));
          }
        }
      }
    }
  }

  @Test
  public void testMapKeyIndexMaintenanceForNonCompactTypeSpecificKeysIndex() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion) CacheUtils.createRegion("testRgn", null);
    int ID = 1;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (; ID <= 5; ++ID) {
      MapKeyIndexData mkid = new MapKeyIndexData(ID);
      for (int j = 1; j <= ID; ++j) {
        mkid.maap.put("key" + j, "val" + j);
      }
      testRgn.put(ID, mkid);
    }
    --ID;// ID = 5;
    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "objs.maap['key1','key2','key3','key7']", SEPARATOR + "testRgn objs");
    assertEquals(i1.getCanonicalizedIndexedExpression(),
        "index_iter1.maap['key1','key2','key3','key7']");
    assertTrue(i1 instanceof MapRangeIndex);
    MapRangeIndex mri = (MapRangeIndex) i1;
    // Test index maintenance
    // addition of new Portfolio object
    Map<Object, AbstractIndex> indxMap = mri.getRangeIndexHolderForTesting();
    assertEquals(indxMap.size(), 4);
    for (int j = 1; j <= 3; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      RangeIndex rng = (RangeIndex) indxMap.get("key" + j);
      Iterator itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        RegionEntryToValuesMap entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (Integer elem : expectedElements) {
          RegionEntry re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    for (int j = 4; j <= ID; ++j) {
      assertFalse(indxMap.containsKey("key" + j));
    }
    // addition of new Portfolio in the Map
    ++ID; // ID = 6
    MapKeyIndexData mkid = new MapKeyIndexData(ID);
    for (int j = 1; j <= ID; ++j) {
      mkid.maap.put("key" + j, "val" + j);
    }
    testRgn.put(ID, mkid);
    assertEquals(indxMap.size(), 4);
    for (int j = 1; j <= 3; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      RangeIndex rng = (RangeIndex) indxMap.get("key" + j);
      Iterator itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        RegionEntryToValuesMap entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (Integer elem : expectedElements) {
          RegionEntry re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    for (int j = 4; j <= ID; ++j) {
      assertFalse(indxMap.containsKey("key" + j));
    }
    // addition of new key in the positions map
    mkid.maap.put("key7", "val7");
    testRgn.put(ID, mkid);
    assertEquals(indxMap.size(), 4);

    for (int j = 1; j <= 3; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      RangeIndex rng = (RangeIndex) indxMap.get("key" + j);
      Iterator itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        RegionEntryToValuesMap entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (Integer elem : expectedElements) {
          RegionEntry re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    assertTrue(indxMap.containsKey("key7"));
    RangeIndex rng = (RangeIndex) indxMap.get("key7");
    Iterator itr = rng.valueToEntriesMap.values().iterator();
    assertEquals(rng.valueToEntriesMap.size(), 1);
    assertTrue(rng.valueToEntriesMap.containsKey("val7"));
    RegionEntryToValuesMap entryMap = (RegionEntryToValuesMap) itr.next();
    assertEquals(1, entryMap.getNumEntries());
    RegionEntry re = testRgn.basicGetEntry(6);
    entryMap.containsEntry(re);

    // deletion of key in the positions map
    mkid.maap.remove("key7");
    testRgn.put(ID, mkid);
    // No Key Indexes are removed from a MapRangeIndex even if they are empty.
    assertEquals(indxMap.size(), 4);
    for (int j = 1; j <= 3; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      rng = (RangeIndex) indxMap.get("key" + j);
      itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (Integer elem : expectedElements) {
          re = testRgn.basicGetEntry(elem);

          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    // update of key in the positions map
    mkid = (MapKeyIndexData) testRgn.get(1);
    mkid.maap.put("key1", "val2");
    testRgn.put(1, mkid);
    assertEquals(indxMap.size(), 4);
    for (int j = 1; j <= 3; ++j) {
      String keey = "key" + j;
      assertTrue(indxMap.containsKey(keey));
      rng = (RangeIndex) indxMap.get(keey);
      itr = rng.valueToEntriesMap.values().iterator();
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        assertTrue(rng.valueToEntriesMap.containsKey("val1"));
        assertTrue(rng.valueToEntriesMap.containsKey("val2"));
      } else {
        assertEquals(rng.valueToEntriesMap.size(), 1);
        assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      }

      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val1");
        assertEquals(5, entryMap.getNumEntries());
        expectedElements.remove(1);
        for (Integer elem : expectedElements) {
          re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val2");
        assertEquals(1, entryMap.getNumEntries());
        re = testRgn.basicGetEntry(1);
        assertTrue(entryMap.containsEntry(re));

      } else {
        while (itr.hasNext()) {
          entryMap = (RegionEntryToValuesMap) itr.next();
          assertEquals(ID + 1 - j, entryMap.getNumEntries());
          for (Integer elem : expectedElements) {
            re = testRgn.basicGetEntry(elem);
            assertTrue(entryMap.containsEntry(re));
          }
        }
      }
    }
    // deletion of portfolio object key in the positions map
    testRgn.remove(3);

    assertEquals(indxMap.size(), 4);
    for (int j = 1; j <= 3; ++j) {
      String keey = "key" + j;
      assertTrue(indxMap.containsKey(keey));
      rng = (RangeIndex) indxMap.get(keey);
      itr = rng.valueToEntriesMap.values().iterator();
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        assertTrue(rng.valueToEntriesMap.containsKey("val1"));
        assertTrue(rng.valueToEntriesMap.containsKey("val2"));
      } else {
        assertEquals(rng.valueToEntriesMap.size(), 1);
        assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      }
      if (keey.equals("key1")) {
        assertEquals(rng.valueToEntriesMap.size(), 2);
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val1");
        assertEquals(4, entryMap.getNumEntries());

        for (int k = 2; k <= 6; ++k) {
          if (k == 3) {
            continue;
          } else {
            re = testRgn.basicGetEntry(k);
            assertTrue(entryMap.containsEntry(re));
          }
        }
        entryMap = (RegionEntryToValuesMap) rng.valueToEntriesMap.get("val2");
        assertEquals(1, entryMap.getNumEntries());
        re = testRgn.basicGetEntry(1);
        assertTrue(entryMap.containsEntry(re));

      } else {
        while (itr.hasNext()) {
          entryMap = (RegionEntryToValuesMap) itr.next();
          assertEquals(ID - j, entryMap.getNumEntries());
          for (int p = j; p <= ID; ++p) {
            re = testRgn.basicGetEntry(p);
            if (p == 3) {
              assertNull(re);
            } else {
              assertTrue(entryMap.containsEntry(re));
            }
          }
        }
      }
    }
  }

  @Test
  public void testMapIndexRecreationForAllKeys() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion) CacheUtils.createRegion("testRgn", null);
    int ID = 1;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (; ID <= 5; ++ID) {
      MapKeyIndexData mkid = new MapKeyIndexData(ID);
      for (int j = 1; j <= ID; ++j) {
        mkid.maap.put("key" + j, "val" + j);
      }
      testRgn.put(ID, mkid);
    }
    --ID;
    Index i1 =
        qs.createIndex("Index1", IndexType.FUNCTIONAL, "objs.maap[*]", SEPARATOR + "testRgn objs");
    assertEquals(i1.getCanonicalizedIndexedExpression(), "index_iter1.maap[*]");
    assertTrue(i1 instanceof MapRangeIndex);
    MapRangeIndex mri = (MapRangeIndex) i1;
    // Test index maintenance
    // addition of new Portfolio object
    Map<Object, AbstractIndex> indxMap = mri.getRangeIndexHolderForTesting();
    assertEquals(indxMap.size(), ID);
    for (int j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      RangeIndex rng = (RangeIndex) indxMap.get("key" + j);
      Iterator itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        RegionEntryToValuesMap entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (Integer elem : expectedElements) {
          RegionEntry re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
    IndexManager im = testRgn.getIndexManager();
    im.rerunIndexCreationQuery();
    ID = 5;
    i1 = im.getIndex("Index1");
    assertEquals(i1.getCanonicalizedIndexedExpression(), "index_iter1.maap[*]");
    assertTrue(i1 instanceof MapRangeIndex);
    mri = (MapRangeIndex) i1;
    // Test index maintenance
    // addition of new Portfolio object
    indxMap = mri.getRangeIndexHolderForTesting();
    assertEquals(indxMap.size(), ID);
    for (int j = 1; j <= ID; ++j) {
      assertTrue(indxMap.containsKey("key" + j));
      RangeIndex rng = (RangeIndex) indxMap.get("key" + j);
      Iterator itr = rng.valueToEntriesMap.values().iterator();
      assertEquals(rng.valueToEntriesMap.size(), 1);
      assertTrue(rng.valueToEntriesMap.containsKey("val" + j));
      Set<Integer> expectedElements = new HashSet<Integer>();
      for (int k = j; k <= ID; ++k) {
        expectedElements.add(k);
      }
      while (itr.hasNext()) {
        RegionEntryToValuesMap entryMap = (RegionEntryToValuesMap) itr.next();
        assertEquals(ID + 1 - j, entryMap.getNumEntries());
        for (Integer elem : expectedElements) {
          RegionEntry re = testRgn.basicGetEntry(elem);
          assertTrue(entryMap.containsEntry(re));
        }
      }
    }
  }

  /**
   * Tests Index maintenance on data loaded via cache loader
   */
  @Test
  public void testIndexMaintenanceOnCacheLoadedData() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    Cache cache = CacheUtils.getCache();
    qs = cache.getQueryService();
    region = CacheUtils.createRegion("portfolio1", null);
    AttributesMutator am = region.getAttributesMutator();
    am.setCacheLoader(new CacheLoader() {

      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        String key = (String) helper.getKey();
        Portfolio p = new Portfolio(Integer.parseInt(key));
        return p;
      }

      @Override
      public void close() {
        // nothing
      }
    });

    Index i1 =
        qs.createIndex("indx1", IndexType.FUNCTIONAL, "pf.getID()", SEPARATOR + "portfolio1 pf");
    List keys = new ArrayList();
    keys.add("1");
    keys.add("2");
    keys.add("3");
    keys.add("4");

    region.getAll(keys);
  }

  /**
   * Tests Index maintenance on data loaded via cache loader
   */
  @Test
  public void testIndexMaintenanceOnPutAll() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    Cache cache = CacheUtils.getCache();
    qs = cache.getQueryService();
    region = CacheUtils.createRegion("portfolio1", null);
    region.put("1", new Portfolio(1));
    Index i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "posvals.secId",
        SEPARATOR + "portfolio1 pf, pf.positions.values posvals ");
    Map data = new HashMap();
    for (int i = 1; i < 11; ++i) {
      data.put("" + i, new Portfolio(i + 2));
    }

    region.putAll(data);
  }

  @Test
  public void testBug43597() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    Cache cache = CacheUtils.getCache();
    qs = cache.getQueryService();
    region = CacheUtils.createRegion("portfolio1", null);
    Index i1 = qs.createIndex("indx1", IndexType.FUNCTIONAL, "posvals",
        SEPARATOR + "portfolio1 pf, pf.getCollectionHolderMap.values posvals ");
    Portfolio pf1 = new Portfolio(1);
    Map collHolderMap = pf1.getCollectionHolderMap();
    collHolderMap.clear();
    collHolderMap.put(1, 1);
    collHolderMap.put(2, 1);
    region.put("1", pf1);

    pf1 = new Portfolio(2);
    collHolderMap = pf1.getCollectionHolderMap();
    collHolderMap.clear();
    collHolderMap.put(3, 1);
    collHolderMap.put(4, 1);
    region.put("1", pf1);
  }

  private void validateIndexForKeys(CompactRangeIndex ri) {
    assertEquals(6, ri.getIndexStorage().size());
    CloseableIterator<IndexStoreEntry> itr = null;
    try {
      itr = ri.getIndexStorage().iterator(null);
      while (itr.hasNext()) {
        IndexStoreEntry reEntry = (IndexStoreEntry) itr.next();
        Object obj = reEntry.getDeserializedRegionKey();
        assertTrue(obj instanceof String);
        assertTrue(idSet.contains(obj));
      }
    } finally {
      if (itr != null) {
        itr.close();
      }
    }
  }

  private void validateIndexForEntries(CompactRangeIndex ri) {
    assertEquals(6, ri.getIndexStorage().size());
    Iterator itr = ri.getIndexStorage().iterator(null);
    while (itr.hasNext()) {
      Object obj = itr.next();
      assertFalse(obj instanceof Collection);
      MemoryIndexStoreEntry re = (MemoryIndexStoreEntry) obj;
      Portfolio pf = (Portfolio) re.getRegionEntry().getValueInVM((LocalRegion) ri.getRegion());
      assertTrue(idSet.contains(String.valueOf(pf.getID())));
    }
  }

  private void validateIndexForValues(CompactRangeIndex ri) {
    assertEquals(6, ri.getIndexStorage().size());
    CloseableIterator<IndexStoreEntry> itr = null;
    try {
      itr = ri.getIndexStorage().iterator(null);
      while (itr.hasNext()) {
        Object regionEntries = itr.next();
        assertTrue(regionEntries instanceof IndexStoreEntry);
      }
    } finally {
      if (itr != null) {
        itr.close();
      }
    }
  }

  private static class MapKeyIndexData implements Serializable {
    int id;
    public Map maap = new HashMap();

    public MapKeyIndexData(int id) {
      this.id = id;
    }

    public void addKeyValue(Object key, Object value) {
      this.maap.put(key, value);
    }
  }
}
