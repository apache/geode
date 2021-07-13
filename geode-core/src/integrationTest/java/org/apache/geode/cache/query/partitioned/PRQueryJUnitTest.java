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
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegionTestHelper;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * Class verifies Region#query(String predicate) API for PartitionedRegion on a single VM.
 *
 *
 */
@Category({OQLQueryTest.class})
public class PRQueryJUnitTest {
  final String regionName = "portfolios";

  LogWriter logger = null;

  @Before
  public void setUp() {
    if (logger == null) {
      logger = PartitionedRegionTestHelper.getLogger();
    }

  }

  /**
   * Tests the execution of query on a PartitionedRegion created on a single data store. <br>
   * 1. Creates a PR with redundancy=0 on a single VM. 2. Puts some test Objects in cache. 3. Fires
   * queries on the data and verifies the result.
   *
   */
  @Test
  public void testQueryOnSingleDataStore() throws Exception {
    Region<Integer, Object> region =
        PartitionedRegionTestHelper.createPartitionedRegion(regionName, "100", 0);
    PortfolioData[] portfolios = new PortfolioData[100];
    for (int j = 0; j < 100; j++) {
      portfolios[j] = new PortfolioData(j);
    }
    try {
      populateData(region, portfolios);

      String queryString = "ID < 5";
      SelectResults<PortfolioData> resSet = region.query(queryString);
      Assert.assertTrue(resSet.size() == 5);

      queryString = "ID > 5 and ID <=15";
      resSet = region.query(queryString);
      Assert.assertTrue(resSet.size() == 10);
    } finally {
      region.close();
    }
  }

  @Test
  public void testQueryWithNullProjectionValue() throws Exception {
    Region<String, HashMap<String, String>> region =
        PartitionedRegionTestHelper.createPartitionedRegion(regionName, "100", 0);
    int size = 10;
    HashMap<String, String> value;
    for (int j = 0; j < size; j++) {
      value = new HashMap<>();
      value.put("account" + j, "account" + j);
      region.put("" + j, value);
    }

    String queryString = "Select p.get('account') from " + SEPARATOR + region.getName() + " p ";
    Query query = PartitionedRegionTestHelper.getCache().getQueryService().newQuery(queryString);
    SelectResults<HashMap<String, String>> sr =
        (SelectResults<HashMap<String, String>>) query.execute();
    Assert.assertTrue(sr.size() == size);

    try {
      queryString = "Select p.get('acc') from " + SEPARATOR + region.getName() + " p ";
      query = PartitionedRegionTestHelper.getCache().getQueryService().newQuery(queryString);
      sr = (SelectResults<HashMap<String, String>>) query.execute();
      Assert.assertTrue(sr.size() == 10);
      for (Object r : sr.asList()) {
        if (r != null) {
          fail("Expected null value, but found " + r);
        }
      }
    } finally {
      region.close();
    }
  }

  @Test
  public void testOrderByQuery() throws Exception {
    Region<Integer, Object> region =
        PartitionedRegionTestHelper.createPartitionedRegion(regionName, "100", 0);
    String[] values = new String[100];
    for (int j = 0; j < 100; j++) {
      values[j] = "" + j;
    }

    try {
      populateData(region, values);

      String queryString =
          "Select distinct p from " + SEPARATOR + region.getName() + " p order by p";
      Query query = PartitionedRegionTestHelper.getCache().getQueryService().newQuery(queryString);
      SelectResults sr = (SelectResults) query.execute();

      Assert.assertTrue(sr.size() == 100);
    } finally {
      region.close();
    }
  }

  @Test
  public void testNestedPRQuery() throws Exception {

    Region<String, PdxInstance> region =
        PartitionedRegionTestHelper.createPartitionedRegion("TEST_REGION");

    InternalCache cache = PartitionedRegionTestHelper.getCache();
    QueryService queryService = cache.getQueryService();
    Query query = queryService.newQuery(
        "SELECT distinct COUNT(*) FROM (SELECT DISTINCT tr.id, tr.domain FROM " + SEPARATOR
            + "TEST_REGION tr)");
    region.put("1", cache.createPdxInstanceFactory("obj1").writeString("id", "1")
        .writeString("domain", "domain1").create());
    region.put("2", cache.createPdxInstanceFactory("obj2").writeString("id", "1")
        .writeString("domain", "domain1").create());
    region.put("3", cache.createPdxInstanceFactory("obj3").writeString("id", "1")
        .writeString("domain", "domain1").create());
    region.put("4", cache.createPdxInstanceFactory("obj4").writeString("id", "1")
        .writeString("domain", "domain1").create());
    region.put("5", cache.createPdxInstanceFactory("obj5").writeString("id", "1")
        .writeString("domain", "domain1").create());
    region.put("6", cache.createPdxInstanceFactory("obj6").writeString("id", "1")
        .writeString("domain", "domain2").create());
    region.put("7", cache.createPdxInstanceFactory("obj7").writeString("id", "1")
        .writeString("domain", "domain2").create());
    region.put("8", cache.createPdxInstanceFactory("obj8").writeString("id", "1")
        .writeString("domain", "domain2").create());
    region.put("9", cache.createPdxInstanceFactory("obj9").writeString("id", "1")
        .writeString("domain", "domain2").create());
    region.put("10", cache.createPdxInstanceFactory("obj10").writeString("id", "1")
        .writeString("domain", "domain2").create());
    region.put("11", cache.createPdxInstanceFactory("obj11").writeString("id", "1")
        .writeString("domain", "domain2").create());

    SelectResults queryResults = (SelectResults) query.execute();
    Assert.assertTrue(queryResults.size() == 1);
    Iterator iterator = queryResults.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertTrue(("" + iterator.next()).equals("2"));
  }

  /**
   * Populates the region with the Objects stores in the data Object array.
   *
   */
  private void populateData(Region<Integer, Object> region, Object[] data) {
    for (int j = 0; j < data.length; j++) {
      region.put(j, data[j]);
    }
  }
}
