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
package org.apache.geode.cache.query;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.NonTXEntry;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * RegionJUnitTest.java
 *
 * Created on January 31, 2005, 3:54 PM
 *
 */
@Category({OQLQueryTest.class})
public class RegionJUnitTest {

  static String[] queries = {"status = 'active'", "status <> 'active'", "ID > 2", "ID < 1",
      "ID >= 2", "ID <= 1", "status = 'active' AND ID = 0", "status = 'active' AND ID = 1",
      "status = 'active' OR ID = 1", "isActive", "isActive()", "testMethod(true)", "NOT isActive",
      "P1.secId = 'SUN'", "status = 'active' AND ( ID = 1 OR P1.secId = 'SUN')",};

  Region region;
  QueryService qs;
  Cache cache;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void regionQueryWithFullQueryShouldNotFail() throws Exception {
    SelectResults results = region.query("SeLeCt * FROM " + SEPARATOR + "pos where ID = 1");
    assertEquals(1, results.size());

    results = region.query("SELECT * FROM " + SEPARATOR + "pos");
    assertEquals(4 /* num entries added in setup */, results.size());
  }

  @Test
  public void regionQueryExecuteWithFullQueryWithDifferentRegionShouldFail() throws Exception {
    expectedException.expect(QueryInvalidException.class);
    cache.createRegionFactory(RegionShortcut.REPLICATE).create("otherRegion");
    SelectResults results = region.query("select * FROM " + SEPARATOR + "otherRegion where ID = 1");
  }

  @Test
  public void regionQueryExecuteWithMulipleRegionsInFullQueryShouldFail() throws Exception {
    expectedException.expect(QueryInvalidException.class);
    cache.createRegionFactory(RegionShortcut.REPLICATE).create("otherRegion");
    SelectResults results = region
        .query("select * FROM " + SEPARATOR + "pos, " + SEPARATOR + "otherRegion where ID = 1");
  }


  @Test
  public void testShortcutMethods() throws Exception {
    for (final String query : queries) {
      Object r = region.query(query);
    }
  }

  @Test
  public void testQueryServiceInterface() throws Exception {
    for (final String query : queries) {
      Query q = qs.newQuery("select distinct * from " + SEPARATOR + "pos where " + query);
      Object r = q.execute();
    }
  }


  @Test
  public void testParameterBinding() throws Exception {
    Query q = qs.newQuery("select distinct * from " + SEPARATOR + "pos where ID = $1");
    Object[] params = new Object[] {0};// {"active"};
    Object r = q.execute(params);

    q = qs.newQuery("select distinct * from $1 where status = $2 and ID = $3");
    params = new Object[] {region, "active", 0};
    r = q.execute(params);
  }



  @Test
  public void testQRegionInterface() throws Exception {
    String[] queries = {"select distinct * from " + SEPARATOR + "pos.keys where toString = '1'",
        "select distinct * from " + SEPARATOR + "pos.values where status = 'active'",
        "select distinct * from " + SEPARATOR + "pos.entries where key = '1'",
        "select distinct * from " + SEPARATOR + "pos.entries where value.status = 'active'"};

    for (final String query : queries) {
      Query q = qs.newQuery(query);
      Object r = q.execute();
    }
  }


  @Test
  public void testInvalidEntries() throws Exception {
    region.invalidate("1");
    region.invalidate("3");
    Query q = qs.newQuery("select distinct * from " + SEPARATOR + "pos");
    SelectResults results = (SelectResults) q.execute();
    assertEquals(2, results.size()); // should not include NULLs
  }

  @Test
  public void testRegionEntryAccess() throws Exception {
    for (final Object o : region.entrySet(false)) {
      Region.Entry entry = (Region.Entry) o;
      RegionEntry regionEntry = null;
      if (entry instanceof NonTXEntry) {
        regionEntry = ((NonTXEntry) entry).getRegionEntry();
      } else {
        regionEntry = ((EntrySnapshot) entry).getRegionEntry();
      }
      assertNotNull(regionEntry);
    }


    LocalRegion lRegion = (LocalRegion) region;
    for (final Object key : lRegion.keys()) {
      Region.Entry rEntry = lRegion.getEntry(key);
      RegionEntry regionEntry = null;
      if (rEntry instanceof NonTXEntry) {
        regionEntry = ((NonTXEntry) rEntry).getRegionEntry();
      } else {
        regionEntry = ((EntrySnapshot) rEntry).getRegionEntry();
      }
      assertNotNull(regionEntry);
    }
  }

  @Test
  public void testRegionNames() {
    String[] queryStrs =
        new String[] {"SELECT * FROM " + SEPARATOR + "pos",
            "SELECT * FROM " + SEPARATOR + "pos where status='active'"};

    CacheUtils.startCache();
    cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setValueConstraint(Portfolio.class);
    RegionAttributes regionAttributes = attributesFactory.create();

    cache.createRegion("p_os", regionAttributes);
    cache.createRegion("p-os", regionAttributes);

    for (final String queryStr : queryStrs) {
      Query q = CacheUtils.getQueryService().newQuery(queryStr);
      try {
        q.execute();
      } catch (Exception ex) {
        // Failed - Any other exception.
        fail("Failed to execute the query. '" + queryStr + "' Error: " + ex.getMessage());
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setValueConstraint(Portfolio.class);
    RegionAttributes regionAttributes = attributesFactory.create();

    region = cache.createRegion("pos", regionAttributes);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));

    qs = cache.getQueryService();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

}
