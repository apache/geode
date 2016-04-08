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
package com.gemstone.gemfire.cache.query.partitioned;

import static org.junit.Assert.fail;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.PRQueryProcessor;
import com.gemstone.gemfire.internal.cache.PartitionedRegionTestHelper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Class verifies Region#query(String predicate) API for PartitionedRegion on a
 * single VM.
 * 
 * 
 */
@Category(IntegrationTest.class)
public class PRQueryNumThreadsJUnitTest
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
    PRQueryProcessor.TEST_NUM_THREADS = 10;
    try {
      populateData(region, portfolios);

      String queryString = "ID < 5";
      SelectResults resSet = region.query(queryString);
      Assert.assertTrue(resSet.size() == 5);

      queryString = "ID > 5 and ID <=15";
      resSet = region.query(queryString);
      Assert.assertTrue(resSet.size() == 10);
    } finally { 
      PRQueryProcessor.TEST_NUM_THREADS = 0;
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
    
    PRQueryProcessor.TEST_NUM_THREADS = 10;
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
      PRQueryProcessor.TEST_NUM_THREADS = 0;
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
    PRQueryProcessor.TEST_NUM_THREADS = 10;
    try {
      populateData(region, values);

      String queryString = "Select distinct p from /" + region.getName() + " p order by p";
      Query query = region.getCache().getQueryService().newQuery(queryString);
      SelectResults sr = (SelectResults)query.execute();

      Assert.assertTrue(sr.size() == 100);
    } finally {
      PRQueryProcessor.TEST_NUM_THREADS = 0;
      region.close();
    }
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
