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
package org.apache.geode.cache.query.internal.index;

import org.apache.geode.cache.*;
import org.apache.geode.cache.query.*;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.ParseException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class CompactRangeIndexIndexMapJUnitTest {

  
  @Before
  public void setUp() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "Query.VERBOSE", "true");
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "Query.VERBOSE", "false");
    CacheUtils.closeCache();
  }

  @Test
  public void testCreateFromEntriesIndex() {
    
  }
  
  @Test
  public void testCreateIndexAndPopulate() {
    
  }
  
  @Test
  public void testLDMIndexCreation() throws Exception {
    Cache cache = CacheUtils.getCache();
    Region region = createLDMRegion("portfolios");
    QueryService queryService = cache.getQueryService();
    Index index = queryService.createIndex("IDIndex", "p.ID", "/portfolios p, p.positions ps");
    assertTrue(index instanceof CompactRangeIndex);
  }
  
  @Test
  public void testFirstLevelEqualityQuery() throws Exception {
    testIndexAndQuery("p.ID", "/portfolios p", "Select * from /portfolios p where p.ID = 1");
    testIndexAndQuery("p.ID", "/portfolios p", "Select * from /portfolios p where p.ID > 1");
    testIndexAndQuery("p.ID", "/portfolios p", "Select * from /portfolios p where p.ID < 10");
  }
  
  @Test
  public void testSecondLevelEqualityQuery() throws Exception {
    boolean oldTestLDMValue = IndexManager.IS_TEST_LDM;
    boolean oldTestExpansionValue = IndexManager.IS_TEST_EXPANSION;
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select * from /portfolios p where p.ID = 1");
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select p.ID from /portfolios p where p.ID = 1");
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select p from /portfolios p where p.ID > 3");
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select ps from /portfolios p, p.positions.values ps where ps.secId = 'VMW'");
    IndexManager.IS_TEST_LDM = oldTestLDMValue;
    IndexManager.IS_TEST_EXPANSION = oldTestExpansionValue;
  }
  
  @Test
  public void testMultipleSecondLevelMatches() throws Exception {
    boolean oldTestLDMValue = IndexManager.IS_TEST_LDM;
    boolean oldTestExpansionValue = IndexManager.IS_TEST_EXPANSION;
    testIndexAndQuery("ps.secId", "/portfolios p, p.positions.values ps", "Select * from /portfolios p, p.positions.values ps where ps.secId = 'VMW'");
    IndexManager.IS_TEST_LDM = oldTestLDMValue;
    IndexManager.IS_TEST_EXPANSION = oldTestExpansionValue;
  }
  
  //executes queries against both no index and ldm index
  //compares size counts of both and compares results
  private void testIndexAndQuery(String indexExpression, String regionPath, String queryString) throws Exception {
    Cache cache = CacheUtils.getCache();
    int numEntries = 20;
    QueryService queryService = cache.getQueryService();
    IndexManager.IS_TEST_LDM = false;
    IndexManager.IS_TEST_EXPANSION = false;
    Region region = createReplicatedRegion("portfolios");
    createPortfolios(region, numEntries);
    
    //Test no index
    //Index index = queryService.createIndex("IDIndex", indexExpression, regionPath);
    Query query = queryService.newQuery(queryString);
    SelectResults noIndexResults = (SelectResults) query.execute();
    //clean up
    queryService.removeIndexes();
    
    //creates indexes that may be used by the queries
    Index index = queryService.createIndex("IDIndex", indexExpression, regionPath);
    query = queryService.newQuery(queryString);
    SelectResults memResults = (SelectResults) query.execute();
    //clean up
    queryService.removeIndexes();
    region.destroyRegion();
    
    //Now execute against a replicated region with regular indexes
    //we want to make sure we don't create and LDM index so undo the test hook
    IndexManager.IS_TEST_LDM = true;
    IndexManager.IS_TEST_EXPANSION = true;
    region = createLDMRegion("portfolios");
    createPortfolios(region, numEntries);

    index = queryService.createIndex("IDIndex", indexExpression, regionPath);
    query = queryService.newQuery(queryString);
    SelectResults ldmResults = (SelectResults) query.execute();
    
    assertEquals("Size for no index and index results should be equal", noIndexResults.size(), memResults.size());
    assertEquals("Size for memory and ldm index results should be equal", memResults.size(), ldmResults.size());
    CacheUtils.log("Size is:" + memResults.size());
    //now check elements for both
    for (Object o: ldmResults) {
      assertTrue(memResults.contains(o));
    }
    queryService.removeIndexes();
    region.destroyRegion();
    
  }
  
  
  //Should be changed to ldm region
  //Also should remove IS_TEST_LDM when possible
  private Region createLDMRegion(String regionName) throws ParseException {
    IndexManager.IS_TEST_LDM = true;
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }
  
  private Region createReplicatedRegion(String regionName) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }
  
  private void createPortfolios(Region region, int num) {
    for (int i = 0; i < num; i++) {
      Portfolio p = new Portfolio(i);
      p.positions = new HashMap();
      p.positions.put("VMW", new Position("VMW", Position.cnt * 1000));
      p.positions.put("IBM", new Position("IBM", Position.cnt * 1000));
      p.positions.put("VMW_2", new Position("VMW", Position.cnt * 1000));
      region.put("" + i, p);
    }
  }
  
}
