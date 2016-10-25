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
/**
 * 
 */
package org.apache.geode.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.apache.geode.cache.query.IndexMaintenanceException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class MapRangeIndexMaintenanceJUnitTest{

  static QueryService qs;
  static Region region;
  static Index keyIndex1;
  
  public static final int NUM_BKTS = 20;
  public static final String INDEX_NAME = "keyIndex1"; 

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
  public void testNullMapKeysInIndexOnLocalRegionForCompactMap() throws Exception{

    //Create Partition Region
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
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof CompactMapRangeIndex);

    //Let MapRangeIndex remove values for key 1.
    Portfolio p = new Portfolio(1, 1);
    p.positions = new HashMap();
    region.put(1, p);
    
    //Now mapkeys are null for key 1
    try {
      region.invalidate(1);
    } catch (NullPointerException e) {
      fail("Test Failed! region.destroy got NullPointerException!");
    }
    
    //Now mapkeys are null for key 1
    try {
      region.destroy(1);
    } catch (NullPointerException e) {
      fail("Test Failed! region.destroy got NullPointerException!");
    }
  }
  
  @Test
  public void testNullMapKeysInIndexOnLocalRegion() throws Exception{
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    //Create Partition Region
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
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof MapRangeIndex);

    //Let MapRangeIndex remove values for key 1.
    Portfolio p = new Portfolio(1, 1);
    p.positions = new HashMap();
    region.put(1, p);
    
    //Now mapkeys are null for key 1
    try {
      region.invalidate(1);
    } catch (NullPointerException e) {
      fail("Test Failed! region.destroy got NullPointerException!");
    }
    
    //Now mapkeys are null for key 1
    try {
      region.destroy(1);
    } catch (NullPointerException e) {
      fail("Test Failed! region.destroy got NullPointerException!");
    }
  }

  /**
   * Test index object's comapreTo Function implementation correctness for indexes.
   * @throws Exception
   */
  @Test
  public void testDuplicateKeysInCompactRangeIndexOnLocalRegion() throws Exception{

    //Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", new TestObject("SUN", 1));
    map1.put("IBM", new TestObject("IBM", 2));
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions[*]", "/portfolio");
    
    assertTrue(keyIndex1 instanceof CompactMapRangeIndex);

    //Put duplicate TestObject with "IBM" name.
    region.put(Integer.toString(1), p);
    Portfolio p2 = new Portfolio(2, 2);
    HashMap map2 = new HashMap();
    map2.put("YHOO", new TestObject("YHOO", 3));
    map2.put("IBM", new TestObject("IBM", 2));
    
    p2.positions = map2;
    region.put(Integer.toString(2), p2);

    //Following destroy fails if fix for 44123 is not there.
    try {
      region.destroy(Integer.toString(1));
    } catch (NullPointerException e) {
      fail("Test Failed! region.destroy got NullPointerException!");
    } catch (Exception ex) {
      if (ex instanceof IndexMaintenanceException) {
        if (! ex.getCause().getMessage()
            .contains("compareTo function is errorneous")) {
          fail("Test Failed! Did not get expected exception IMQException."
              + ex.getMessage());
        }
      } else {
        ex.printStackTrace();
        fail("Test Failed! Did not get expected exception IMQException.");
      }
    }
  }
  
  /**
   * Test index object's comapreTo Function implementation correctness for indexes.
   * @throws Exception
   */
  @Test
  public void testDuplicateKeysInRangeIndexOnLocalRegion() throws Exception{
    IndexManager.TEST_RANGEINDEX_ONLY = true;

    //Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    map1.put("SUN", new TestObject("SUN", 1));
    map1.put("IBM", new TestObject("IBM", 2));
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions[*]", "/portfolio");
    
    assertTrue(keyIndex1 instanceof MapRangeIndex);

    //Put duplicate TestObject with "IBM" name.
    region.put(Integer.toString(1), p);
    Portfolio p2 = new Portfolio(2, 2);
    HashMap map2 = new HashMap();
    map2.put("YHOO", new TestObject("YHOO", 3));
    map2.put("IBM", new TestObject("IBM", 2));
    
    p2.positions = map2;
    region.put(Integer.toString(2), p2);

    //Following destroy fails if fix for 44123 is not there.
    try {
      region.destroy(Integer.toString(1));
    } catch (NullPointerException e) {
      fail("Test Failed! region.destroy got NullPointerException!");
    } catch (Exception ex) {
      if (ex instanceof IndexMaintenanceException) {
        if (! ex.getCause().getMessage()
            .contains("compareTo function is errorneous")) {
          fail("Test Failed! Did not get expected exception IMQException."
              + ex.getMessage());
        }
      } else {
        ex.printStackTrace();
        fail("Test Failed! Did not get expected exception IMQException.");
      }
    }
  }

  @Test
  public void testUndefinedForMapRangeIndex() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();   
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", "/portfolio ");
    assertTrue("Index should be a MapRangeIndex ", keyIndex1 instanceof MapRangeIndex);
   
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        // add some string objects generating UNDEFINEDs as index keys
        if(i % 2 == 0) {
          region.put(Integer.toString(i), "Portfolio-"+ i);
        } else {
          region.put(Integer.toString(i), new Portfolio(i, i));
        }
      }
    }
    assertEquals(100, region.size());
    
    qs.removeIndexes();
   
    // recreate index to verify they get updated correctly
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", "/portfolio ");
    assertTrue("Index should be a MapRangeIndex ", keyIndex1 instanceof MapRangeIndex);
    
  }
  
  @Test
  public void testUndefinedForCompactMapRangeIndex() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    qs = CacheUtils.getQueryService();   
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", "/portfolio ");
    assertTrue("Index should be a CompactMapRangeIndex ", keyIndex1 instanceof CompactMapRangeIndex);
   
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        // add some string objects generating UNDEFINEDs as index keys
        if(i % 2 == 0) {
          region.put(Integer.toString(i), "Portfolio-"+ i);
        } else {
          region.put(Integer.toString(i), new Portfolio(i, i));
        }
      }
    }
    assertEquals(100, region.size());
    
    qs.removeIndexes();
   
    // recreate index to verify they get updated correctly
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions['SUN', 'IBM']", "/portfolio ");
    assertTrue("Index should be a CompactMapRangeIndex ", keyIndex1 instanceof CompactMapRangeIndex);
  }

  @Test
  public void testNullMapValuesInIndexOnLocalRegionForCompactMap() throws Exception{
    region = CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("portfolio");
    qs = CacheUtils.getQueryService();
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions[*]", "/portfolio ");

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
    
    SelectResults result = (SelectResults) qs.newQuery("select * from /portfolio p where p.positions['SUN'] = null").execute();
    assertEquals(1, result.size());
  }

  @Test
  public void testNullMapValuesInIndexOnLocalRegionForMap() throws Exception{
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    region = CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("portfolio");
    qs = CacheUtils.getQueryService();
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions[*]", "/portfolio ");

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
    
    SelectResults result = (SelectResults) qs.newQuery("select * from /portfolio p where p.positions['SUN'] = null").execute();
    assertEquals(1, result.size());
  }

  @Test
  public void updatingAMapFieldWithDifferentKeysShouldRemoveOldKeysThatAreNoLongerPresent() throws Exception {
    //Create Partition Region
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

    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions[*]", "/portfolio");

    assertTrue(keyIndex1 instanceof CompactMapRangeIndex);

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    map2.put("NEW_KEY", 1);
    region.put(1, p2);

    SelectResults results = (SelectResults) qs.newQuery("select * from /portfolio p where p.positions['SUN'] = 1 OR p.positions['IBM'] = 2")
                                              .execute();
    assertEquals(0, results.size());
  }

  @Test
  public void updatingAMapFieldSameKeysSameValuesShouldUpdateCorrectly() throws Exception {
    //Create Partition Region
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

    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions[*]", "/portfolio");

    assertTrue(keyIndex1 instanceof CompactMapRangeIndex);

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    map2.put("SUN", 1);
    map2.put("IBM", 2);
    region.put(1, p2);

    SelectResults results = (SelectResults) qs.newQuery("select * from /portfolio p where p.positions['SUN'] = 1 OR p.positions['IBM'] = 2")
                                              .execute();
    assertEquals(1, results.size());
  }

  @Test
  public void updatingAMapFieldWithNoKeysShouldRemoveOldKeysThatAreNoLongerPresent() throws Exception {
    //Create Partition Region
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

    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions[*]", "/portfolio");

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    region.put(1, p2);

    SelectResults results = (SelectResults) qs.newQuery("select * from /portfolio p where p.positions['SUN'] = 1 OR p.positions['IBM'] = 2")
                                              .execute();
    assertEquals(0, results.size());
  }

  @Test
  public void updatingEmptyMapToMapWithKeysShouldIndexNewKeysCorrectly() throws Exception {
    //Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    Portfolio p = new Portfolio(1, 1);
    HashMap map1 = new HashMap();
    p.positions = map1;
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    region.put(1, p);
    qs = CacheUtils.getQueryService();

    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions[*]", "/portfolio");

    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    map2.put("SUN", 1);
    map2.put("IBM", 2);
    region.put(1, p2);

    SelectResults results = (SelectResults) qs.newQuery("select * from /portfolio p where p.positions['SUN'] = 1 OR p.positions['IBM'] = 2")
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

    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions[*]", "/portfolio");
    region.put(1, p);

    SelectResults results = (SelectResults) qs.newQuery("select * from /portfolio p where p.positions['SUN'] = 1")
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

    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME, "positions[*]", "/portfolio");
    Portfolio p2 = new Portfolio(1, 1);
    HashMap map2 = new HashMap();
    p2.positions = map2;
    map2.put("SUN",3);
    map2.put("IBM",4);
    region.put(1, p2);

    SelectResults results = (SelectResults) qs.newQuery("select * from /portfolio p where p.positions['SUN'] = 1")
                                              .execute();
    assertEquals(0, results.size());

    results = (SelectResults) qs.newQuery("select * from /portfolio p where p.positions['SUN'] = 3")
                                              .execute();
    assertEquals(1, results.size());
  }




  /**
   * TestObject with wrong comareTo() implementation implementation.
   * Which throws NullPointer while removing mapping from a MapRangeIndex.
   *
   */
  public class TestObject implements Cloneable, Comparable {
    public TestObject(String name, int id) {
      super();
      this.name = name;
      this.id = id;
    }
    String name;
    int id;
    
    @Override
    public int compareTo(Object o) {
      if (id == ((TestObject)o).id)
        return 0;
      else
        return id > ((TestObject)o).id ? -1 : 1;
    }
  }
}
