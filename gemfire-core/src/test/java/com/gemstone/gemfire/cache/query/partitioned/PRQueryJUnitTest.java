/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.partitioned;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.PartitionedRegionTestHelper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Class verifies Region#query(String predicate) API for PartitionedRegion on a
 * single VM.
 * 
 * @author rreja
 * 
 */
@Category(IntegrationTest.class)
public class PRQueryJUnitTest
{
  String regionName = "portfolios";

  LogWriter logger = null;

  @Before
  public void setUp() throws Exception
  {
    if (logger == null) {
      logger = PartitionedRegionTestHelper.getLogger();
    }
  }

  /**
   * Tests the execution of query on a PartitionedRegion created on a single
   * data store. <br>
   * 1. Creates a PR with redundancy=0 on a single VM. 2. Puts some test Objects
   * in cache. 3. Fires queries on the data and verifies the result.
   * 
   * @throws Exception
   */
  @Test
  public void testQueryOnSingleDataStore() throws Exception
  {
    Region region = PartitionedRegionTestHelper.createPartitionedRegion(
        regionName, "100", 0);
    PortfolioData[] portfolios = new PortfolioData[100];
    for (int j = 0; j < 100; j++) {
      portfolios[j] = new PortfolioData(j);
    }
    try {
      populateData(region, portfolios);

      String queryString = "ID < 5";
      SelectResults resSet = region.query(queryString);
      Assert.assertTrue(resSet.size() == 5);

      queryString = "ID > 5 and ID <=15";
      resSet = region.query(queryString);
      Assert.assertTrue(resSet.size() == 10);
    } finally { 
      region.close();
    }
  }

  @Test
  public void testQueryWithNullProjectionValue() throws Exception
  {
    Region region = PartitionedRegionTestHelper.createPartitionedRegion(
        regionName, "100", 0);
    int size = 10;
    HashMap value = null;
    for (int j = 0; j < size; j++) {
      value = new HashMap();
      value.put("account" + j, "account" + j);
      region.put("" +j,  value);
    }

    String queryString = "Select p.get('account') from /" + region.getName() + " p ";
    Query query = region.getCache().getQueryService().newQuery(queryString);
    SelectResults sr = (SelectResults)query.execute();
    Assert.assertTrue(sr.size() == size);
    
    try {
      queryString = "Select p.get('acc') from /" + region.getName() + " p ";
      query = region.getCache().getQueryService().newQuery(queryString);
      sr = (SelectResults)query.execute();
      Assert.assertTrue(sr.size() == 10);
      for (Object r : sr.asList()){
        if (r  != null) {
          fail("Expected null value, but found " + r);
        }
      }
    } finally {
      region.close();
    }
  }
  
  @Test
  public void testOrderByQuery() throws Exception
  {
    Region region = PartitionedRegionTestHelper.createPartitionedRegion(
        regionName, "100", 0);
    String[] values = new String[100];
    for (int j = 0; j < 100; j++) {
      values[j] = new String(""+ j);
    }

    try {
      populateData(region, values);

      String queryString = "Select distinct p from /" + region.getName() + " p order by p";
      Query query = region.getCache().getQueryService().newQuery(queryString);
      SelectResults sr = (SelectResults)query.execute();

      Assert.assertTrue(sr.size() == 100);
    } finally {
      region.close();
    }
  }

  @Test
  public void testNestedPRQuery() throws Exception {
    Cache cache = CacheUtils.getCache();
    QueryService queryService = CacheUtils.getCache().getQueryService();
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create("TEST_REGION");
    Query query = queryService.newQuery("SELECT distinct COUNT(*) FROM (SELECT DISTINCT tr.id, tr.domain FROM /TEST_REGION tr)");
    region.put("1", cache.createPdxInstanceFactory("obj1").writeString("id", "1").writeString("domain", "domain1").create());
    region.put("2", cache.createPdxInstanceFactory("obj2").writeString("id", "1").writeString("domain", "domain1").create());
    region.put("3", cache.createPdxInstanceFactory("obj3").writeString("id", "1").writeString("domain", "domain1").create());
    region.put("4", cache.createPdxInstanceFactory("obj4").writeString("id", "1").writeString("domain", "domain1").create());
    region.put("5", cache.createPdxInstanceFactory("obj5").writeString("id", "1").writeString("domain", "domain1").create());
    region.put("6", cache.createPdxInstanceFactory("obj6").writeString("id", "1").writeString("domain", "domain2").create());
    region.put("7", cache.createPdxInstanceFactory("obj7").writeString("id", "1").writeString("domain", "domain2").create());
    region.put("8", cache.createPdxInstanceFactory("obj8").writeString("id", "1").writeString("domain", "domain2").create());
    region.put("9", cache.createPdxInstanceFactory("obj9").writeString("id", "1").writeString("domain", "domain2").create());
    region.put("10", cache.createPdxInstanceFactory("obj10").writeString("id", "1").writeString("domain", "domain2").create());
    region.put("11", cache.createPdxInstanceFactory("obj11").writeString("id", "1").writeString("domain", "domain2").create());

    SelectResults queryResults = (SelectResults) query.execute();
    Assert.assertTrue(queryResults.size() == 1);
    Iterator iterator = queryResults.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertTrue(("" + iterator.next()).equals("2"));
 }

  /**
   * Populates the region with the Objects stores in the data Object array.
   * 
   * @param region
   * @param data
   */
  private void populateData(Region region, Object[] data)
  {
    for (int j = 0; j < data.length; j++) {
      region.put(new Integer(j), data[j]);
    }
  }
}
