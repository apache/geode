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
package com.gemstone.gemfire.cache.query.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.index.CompactRangeIndex;
import com.gemstone.gemfire.cache.query.internal.index.IndexProtocol;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class QueryTraceJUnitTest {

  static QueryService qs;
  static Region region;
  static Index keyIndex1;
  
  private static final String queryStr = "select * from /portfolio where ID > 0";
  public static final int NUM_BKTS = 20;
  public static final String INDEX_NAME = "keyIndex1"; 
  
  private static File logfile;
  
  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
  
  /**
   * Tests tracing on queries with <TRACE> or <trace> tag.
   * @throws Exception
   */
  @Test
  public void testTraceOnPartitionedRegionWithTracePrefix() throws Exception{
    
    String slComment = "-- single line comment with TRACE \n" ;
    String mlComment = " /* Multi-line comments here" +
    "* ends here " +
    "* with TRACE too" +
    "*/ <TRACE> "; 
    String prefix = slComment + mlComment;
                
    //Create Partition Region
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(NUM_BKTS);
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof PartitionedIndex);

    Query query = qs.newQuery(prefix+queryStr);
    assertTrue(((DefaultQuery)query).isTraced());
    
    SelectResults results = (SelectResults)query.execute();
    assertTrue(QueryObserverHolder.getInstance() instanceof IndexTrackingQueryObserver);
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();
  }

  @Test
  public void testTraceOnLocalRegionWithTracePrefix() throws Exception{
    
    String slComment = "-- single line comment with TRACE \n" ;
    String mlComment = " /* Multi-line comments here" +
    "* ends here " +
    "* with TRACE too" +
    "*/ <TRACE> "; 
    String prefix = slComment + mlComment;
    
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
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    Query query = qs.newQuery(prefix+queryStr);
    assertTrue(((DefaultQuery)query).isTraced());
    
    SelectResults results = (SelectResults)query.execute();
    assertTrue(QueryObserverHolder.getInstance() instanceof IndexTrackingQueryObserver);
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();    
  }

  /**
   * negative testing: if  <TRACE> is in comments not tracing is done.
   * @throws Exception
   */
  @Test
  public void testNegTraceOnPartitionedRegionWithTracePrefix() throws Exception{
    
    String slComment = "-- single line comment with TRACE \n" ;
    String mlComment = " /* Multi-line comments here" +
    "* ends here " +
    "* with TRACE too" +
    "*/"; 
    String prefix = slComment + mlComment;
    
    //Create Partition Region
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(NUM_BKTS);
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof PartitionedIndex);

    Query query = qs.newQuery(prefix+queryStr);
    assertFalse(((DefaultQuery)query).isTraced());
    
    SelectResults results = (SelectResults)query.execute();
    assertFalse(QueryObserverHolder.getInstance() instanceof IndexTrackingQueryObserver);
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();
  }

  /**
   * negative testing: if  <TRACE> is in comments not tracing is done.
   * @throws Exception
   */
  @Test
  public void testNegTraceOnLocalRegionWithTracePrefix() throws Exception{
    
    String slComment = "-- single line comment with TRACE \n" ;
    String mlComment = " /* Multi-line comments here" +
    "* ends here " +
    "* with TRACE too" +
    "*/"; 
    String prefix = slComment + mlComment;

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
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    Query query = qs.newQuery(prefix+queryStr);
    assertFalse(((DefaultQuery)query).isTraced());
    
    SelectResults results = (SelectResults)query.execute();
    assertFalse(QueryObserverHolder.getInstance() instanceof IndexTrackingQueryObserver);
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();    
  }

  /**
   * No Query comments
   * @throws Exception
   */
  @Test
  public void testTraceOnPartitionedRegionWithTracePrefixNoComments() throws Exception{
    
    String prefix = "  <TRACE> ";
    //Create Partition Region
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(NUM_BKTS);
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof PartitionedIndex);

    Query query = qs.newQuery(prefix+queryStr);
    assertTrue(((DefaultQuery)query).isTraced());
    
    SelectResults results = (SelectResults)query.execute();
    assertTrue(QueryObserverHolder.getInstance() instanceof IndexTrackingQueryObserver);
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();    
  }

  @Test
  public void testTraceOnLocalRegionWithTracePrefixNoComments() throws Exception{
    
    String prefix = "  <TRACE> ";

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
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    Query query = qs.newQuery(prefix+queryStr);
    assertTrue(((DefaultQuery)query).isTraced());
    
    SelectResults results = (SelectResults)query.execute();
    assertTrue(QueryObserverHolder.getInstance() instanceof IndexTrackingQueryObserver);
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();
  }

  @Test
  public void testTraceOnPartitionedRegionWithSmallTracePrefixNoComments() throws Exception{
    
    String prefix = "<trace> ";
    //Create Partition Region
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(NUM_BKTS);
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof PartitionedIndex);

    Query query = qs.newQuery(prefix+queryStr);
    assertTrue(((DefaultQuery)query).isTraced());
    
    SelectResults results = (SelectResults)query.execute();
    assertTrue(QueryObserverHolder.getInstance() instanceof IndexTrackingQueryObserver);
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();
    
  }

  @Test
  public void testTraceOnLocalRegionWithSmallTracePrefixNoComments() throws Exception{
    
    String prefix = "<trace> ";

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
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    Query query = qs.newQuery(prefix+queryStr);
    assertTrue(((DefaultQuery)query).isTraced());
    
    SelectResults results = (SelectResults)query.execute();
    assertTrue(QueryObserverHolder.getInstance() instanceof IndexTrackingQueryObserver);
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();    
  }

  @Test
  public void testQueryFailLocalRegionWithSmallTraceSuffixNoComments() throws Exception{
    
    String suffix = "<trace> ";

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
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    try {
      Query query = qs.newQuery(queryStr + suffix);
    } catch (Exception e) {
      if (!(e instanceof QueryInvalidException)) {
        fail("Test Failed: Query is invalid but exception was not thrown!");
      }
    }    
  }

@Test
  public void testQueryFailLocalRegionWithSmallTracePrefixNoSpace() throws Exception{
    
    String prefix = "<trace>";

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
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    try {
      Query query = qs.newQuery(prefix+queryStr);
    } catch (Exception e) {
      if (!(e instanceof QueryInvalidException)) {
        fail("Test Failed: Query is invalid but exception was not thrown!");
      }
    }    
  }

}
