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
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.text.ParseException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.util.internal.GeodeGlossary;

@Category({OQLQueryTest.class})
public class QueryUndefinedJUnitTest implements Serializable {

  private static final String regionName = "test";

  @Before
  public void setUp() throws Exception {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "Query.VERBOSE", "true");
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  private static final String[] queries =
      new String[] {"select * from " + SEPARATOR + "test WHERE age != 25",
          "select * from " + SEPARATOR + "test WHERE age !=25 AND flag = true",
          "select * from " + SEPARATOR + "test WHERE age != 25 AND age > 0 and age < 100",
          "select * from " + SEPARATOR + "test WHERE age != 25 OR age > 0",
          "select * from " + SEPARATOR + "test WHERE age > 0",
          "select * from " + SEPARATOR + "test WHERE age IN (select t.age from " + SEPARATOR
              + "test t)",
          "select t.age from " + SEPARATOR + "test t",
          "select * from " + SEPARATOR + "test WHERE age != 25 and flag = false",
          "select * from " + SEPARATOR + "test WHERE age IN (select t.age from " + SEPARATOR
              + "test t) and age != 25"};

  // the test will be validating against the May date, so expected values revolve around month of
  // May
  private static final int[] expectedResults = new int[] {2, 2, 1, 3, 2, 3, 3, 0, 2};



  private void executeQueryTest(Cache cache, String[] queries, int[] expectedResults) {
    CacheUtils.log("********Execute Query Test********");
    QueryService queryService = cache.getQueryService();
    Query query = null;
    String queryString = null;
    int numQueries = queries.length;
    try {
      for (int i = 0; i < numQueries; i++) {
        queryString = queries[i];
        query = queryService.newQuery(queries[i]);
        SelectResults result = (SelectResults) query.execute();
        assertEquals(queries[i], expectedResults[i], result.size());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + queryString + ":" + query + " Execution Failed!");
    }
    CacheUtils.log("********Completed Executing Query Test********");

    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  @Test
  public void testJSONUndefinedValuePR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    createJSONData(region);
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  @Test
  public void testJSONUndefinedValueWithIndexPR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    createJSONData(region);
    CacheUtils.getQueryService().createIndex("ageIndex", "age", SEPARATOR + "test");
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  @Test
  public void testJSONUndefinedValueWithHashIndexPR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    createJSONData(region);
    CacheUtils.getQueryService().createHashIndex("ageIndex", "age", SEPARATOR + "test");
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  @Test
  public void testJSONUndefinedValueRR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    createJSONData(region);
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  @Test
  public void testJSONUndefinedValueWithIndexRR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    createJSONData(region);
    CacheUtils.getQueryService().createIndex("ageIndex", "age", SEPARATOR + "test");
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  @Test
  public void testJSONUndefinedValueWithHashIndexRR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    createJSONData(region);
    CacheUtils.getQueryService().createHashIndex("ageIndex", "age", SEPARATOR + "test");
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  @Test
  public void testObjectUndefinedValuePR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    createObjectData(region);
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  @Test
  public void testObjectUndefinedValueWithIndexPR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    createObjectData(region);
    CacheUtils.getQueryService().createIndex("ageIndex", "age", SEPARATOR + "test");
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  @Test
  public void testObjectUndefinedValueWithHashIndexPR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    createObjectData(region);
    CacheUtils.getQueryService().createHashIndex("ageIndex", "age", SEPARATOR + "test");
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  @Test
  public void testObjectUndefinedValueRR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    createObjectData(region);
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  @Test
  public void testObjectUndefinedValueWithIndexRR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    createObjectData(region);
    CacheUtils.getQueryService().createIndex("ageIndex", "age", SEPARATOR + "test");
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  @Test
  public void testObjectUndefinedValueWithHashIndexRR() throws Exception {
    Region region =
        CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    createObjectData(region);
    CacheUtils.getQueryService().createHashIndex("ageIndex", "age", SEPARATOR + "test");
    executeQueryTest(CacheUtils.getCache(), queries, expectedResults);
  }

  private void createJSONData(Region region) throws ParseException {
    String obj1 = "{\"_id\": \"10002\", \"age\": 26, \"flag\": true }";
    String obj2 = "{\"_id\": \"10001\", \"age\": 25, \"flag\": true }";
    String obj3 = "{\"_id\": \"10003\", \"flag\": true }";
    region.put("value1", JSONFormatter.fromJSON(obj1));
    region.put("value2", JSONFormatter.fromJSON(obj2));
    region.put("value3", JSONFormatter.fromJSON(obj3));
  }

  private void createObjectData(Region region) throws ParseException {
    String obj1 = "{\"_id\": \"10002\", \"age\": 26, \"flag\": true }";
    String obj2 = "{\"_id\": \"10001\", \"age\": 25, \"flag\": true }";
    String obj3 = "{\"_id\": \"10003\", \"flag\": true }";
    region.put("value1", new PersonType1("10002", 26, true));
    region.put("value2", new PersonType1("10001", 25, true));
    region.put("value3", new PersonType2("10003", true));
  }


  public class PersonType1 implements Serializable {
    public String name;
    public int age;


    public boolean flag;

    public PersonType1(String name, int age, boolean flag) {
      this.name = name;
      this.age = age;
      this.flag = flag;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public boolean isFlag() {
      return flag;
    }

    public void setFlag(boolean flag) {
      this.flag = flag;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }

  public class PersonType2 implements Serializable {
    public String name;
    public boolean flag;

    public PersonType2(String name, boolean flag) {
      this.name = name;
      this.flag = flag;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public boolean isFlag() {
      return flag;
    }

    public void setFlag(boolean flag) {
      this.flag = flag;
    }
  }
}
