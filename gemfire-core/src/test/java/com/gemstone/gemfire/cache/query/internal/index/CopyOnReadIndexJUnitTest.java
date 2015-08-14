/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal.index;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.QueryTestUtils;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * @author jhuynh
 *
 */
@Category(IntegrationTest.class)
public class CopyOnReadIndexJUnitTest {
  
  static int numObjects = 10;
  static int objectsAndResultsMultiplier = 10;
  QueryTestUtils utils;
  static String regionName = "portfolios";
  static final String indexName = "testIndex";
  String[] queries = {"select * from /" + regionName + " p where p.indexKey = 1",
      "select distinct * from /" + regionName + " p where p.indexKey = 1 order by p.indexKey",
      "select * from /" + regionName + " p, p.positions.values pv where pv.secId = '1'",
      "select * from /" + regionName + " p where p in (select * from /" + regionName + " pi where pi.indexKey = 1)",
      
      //"select * from /" + regionName + " p where p.ID = ELEMENT(select pi.ID from /" + regionName + " pi where pi.ID = 1)"
  };
  
  int[] expectedResults = { 1,
      1,
     1,
     1//,
     //1
  };
  
  boolean[] containsInnerQuery = { false,
      false,
     false,
     true
      
  };
  
  @Before
  public void setUp() throws java.lang.Exception {
    utils = new QueryTestUtils();
    java.util.Properties p = new java.util.Properties();
    p.put("mcast-port", "0");
    utils.createCache(p);
    utils.getCache().setCopyOnRead(true);
    Portfolio.resetInstanceCount();
  }
  
  @After
  public void tearDown() throws java.lang.Exception {
    utils.getCache().getQueryService().removeIndexes();
    utils.closeCache();
  }
  
  /**
   * 
   * @param region
   * @param numObjects
   * @param objectsAndResultsMultiplier number of similar objects to put into the cache so that results from queries will be satisfied by the multiple
   */
  private void createData(Region region, int numObjects, int objectsAndResultsMultiplier) {
    for (int i = 0 ; i < numObjects; i++) {
      for (int j = 0; j < objectsAndResultsMultiplier; j++) {
        int regionKey = i * objectsAndResultsMultiplier + j;
        Portfolio p = new Portfolio(regionKey);
        p.indexKey = i;
        p.status = "testStatus";
        p.positions = new HashMap();
        p.positions.put("" + 1, new Position("" + i, i));
        region.put("key-" + regionKey, p);
      }
    }   
  }

  @Test
  public void testCopyOnReadWithHashIndexWithLocalRegion() throws Exception {
    utils.createLocalRegion(regionName);
    utils.createHashIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }

  @Test
  public void testCopyOnReadWithHashIndexWithReplicatedRegion() throws Exception {
    utils.createReplicateRegion(regionName);
    utils.createHashIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadWithHashIndexWithPartitionedRegion() throws Exception {
    utils.createPartitionRegion(regionName, null);
    utils.createHashIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, true);
  }
  
  @Test
  public void testCopyOnReadWithCompactRangeIndexWithLocalRegion() throws Exception {
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadWithCompactRangeIndexWithReplicatedRegion() throws Exception {
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadWithCompactRangeIndexWithPartitionedRegion() throws Exception {
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, true);
  }

  @Test
  public void testCopyOnReadWithRangeIndexWithLocalRegion() throws Exception {
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadWithRangeIndexWithReplicatedRegion() throws Exception {
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadWithRangeIndexWithPartitionedRegion() throws Exception {
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, true);
  }
  
  @Test
  public void testCopyOnReadWithRangeIndexTupleWithLocalRegion() throws Exception {
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadWithRangeIndexTupleWithReplicatedRegion() throws Exception {
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadWithRangeIndexTupleWithPartitionedRegion() throws Exception {
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, true);
  }
  
  //Test copy on read false
  @Test
  public void testCopyOnReadFalseWithHashIndexWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createLocalRegion(regionName);
    utils.createHashIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadFalseWithHashIndexWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createReplicateRegion(regionName);
    utils.createHashIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadFalseWithHashIndexWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createPartitionRegion(regionName, null);
    utils.createHashIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, true);
  }
  
  @Test
  public void testCopyOnReadFalseWithCompactRangeIndexWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadFalseWithCompactRangeIndexWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadFalseWithCompactRangeIndexWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, true);
  }

  @Test
  public void testCopyOnReadFalseWithRangeIndexWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadFalseWithRangeIndexWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadFalseWithRangeIndexWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "p.indexKey", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, true);
  }
  
  @Test
  public void testCopyOnReadFalseWithRangeIndexTupleWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadFalseWithRangeIndexTupleWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, false);
  }
  
  @Test
  public void testCopyOnReadFalseWithRangeIndexTupleWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects, objectsAndResultsMultiplier, true, true);
  }

 /**
  * 
  * @param queries
  * @param expectedResults
  * @param numObjects
  * @param objectsAndResultsMultiplier
  * @param hasIndex
  * @param isPR
  * @throws Exception
  */
  private void helpExecuteQueriesCopyOnRead(String[] queries, int[] expectedResults, int numObjects, int objectsAndResultsMultiplier, boolean hasIndex, boolean isPR) throws Exception {
    Region region = utils.getCache().getRegion("/" + regionName);
    createData(region, numObjects, objectsAndResultsMultiplier);
    for (int i = 0; i < queries.length; i++) {
      Portfolio.instanceCount.set(numObjects * objectsAndResultsMultiplier);
      if (hasIndex && isPR) {
        Portfolio.instanceCount.set(numObjects * objectsAndResultsMultiplier * 2);
      }
      helpTestCopyOnRead(queries[i], expectedResults[i], numObjects, objectsAndResultsMultiplier, hasIndex, isPR, containsInnerQuery[i]);
    }
  }
  
  /**
   * 
   * @param queries
   * @param expectedResults
   * @param numObjects
   * @param objectsAndResultsMultiplier
   * @param hasIndex
   * @param isPR
   * @throws Exception
   */
  private void helpExecuteQueriesCopyOnReadFalse(String[] queries, int[] expectedResults, int numObjects, int objectsAndResultsMultiplier, boolean hasIndex, boolean isPR) throws Exception {
    Region region = utils.getCache().getRegion("/" + regionName);
    createData(region, numObjects, objectsAndResultsMultiplier);
    for (int i = 0; i < queries.length; i++) {
      Portfolio.instanceCount.set(numObjects * objectsAndResultsMultiplier);
      if (hasIndex && isPR) {
        Portfolio.instanceCount.set(numObjects * objectsAndResultsMultiplier * 2);
      }
      helpTestCopyOnReadFalse(queries[i], expectedResults[i], numObjects, objectsAndResultsMultiplier, hasIndex, isPR, containsInnerQuery[i]);
    }
  }
  
  private void helpTestCopyOnRead(String queryString, int expectedResultsSize, int numObjects, int objectsAndResultsMultiplier, boolean hasIndex, boolean isPR, boolean containsInnerQuery) throws Exception {
    int expectedResultsSizeMultiplied = expectedResultsSize * objectsAndResultsMultiplier;
    int numInstances = numObjects * objectsAndResultsMultiplier;
   
    //We are a 1 VM test, replicated regions would put the actual domain object into the cache
    if (hasIndex && isPR) {
      //If we are PR we serialize the values
      //BUT if we have an index, we deserialize the values, so our instance count is double of our put amount so far
      numInstances += numObjects * objectsAndResultsMultiplier;
    }
    assertEquals("Unexpected number of Portfolio instances", numInstances, Portfolio.instanceCount.get());

    //execute query
    QueryService qs = utils.getCache().getQueryService();
    Query query = qs.newQuery(queryString);
    SelectResults results = (SelectResults) query.execute();
    assertEquals("Results did not match expected count for query" + queryString, expectedResultsSizeMultiplied, results.size());
    for (Object o: results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        p.status = "discardStatus";
      }
      else {
        Struct struct = (Struct)o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        p.status = "discardStatus";
      }
    }
    
    if (!hasIndex && isPR) {
      //We are PR and we do not have an index, so we must deserialize all the values in the cache at this point
      numInstances += (numObjects * objectsAndResultsMultiplier);
      if (containsInnerQuery) {
        //if we have an inner query, it would deserialize all objects for the inner query as well
        numInstances += numObjects * objectsAndResultsMultiplier;
      }
    }
    //So with all the deserialized objects we must also combine the query results of the query due to copy on read
   assertEquals("Unexpected number of Portfolio instances for query " + query, numInstances + expectedResultsSizeMultiplied, Portfolio.instanceCount.get());

    
    results = (SelectResults) query.execute();
    assertEquals("No results were found", expectedResultsSizeMultiplied, results.size());
    for (Object o: results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        assertEquals("status should not have been changed", "testStatus", p.status);
      }
      else {
        Struct struct = (Struct)o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        assertEquals("status should not have been changed", "testStatus", p.status);
      }
    }
    if (!hasIndex && isPR) {
      //Again, because we have no index, we must deserialize the values in the region
      numInstances += (numObjects * objectsAndResultsMultiplier);
      if (containsInnerQuery) {
        //If we have an inner query, we must also deserialize the values in the region for this query
        //We have some interesting logic in LocalRegion when deserializing.  Based on these flags
        //we do not store the deserialized instance back in the cache
        numInstances += numObjects * objectsAndResultsMultiplier;
      }
    }
    //So with all the deserialized objects we must also combine the query results of two queries at this point.  These results themselves would have been copied
    assertEquals("Unexpected number of Portfolio instances", numInstances + expectedResultsSizeMultiplied * 2, Portfolio.instanceCount.get());

  }
  
  private void helpTestCopyOnReadFalse(String queryString, int expectedResultsSize, int numObjects, int objectsAndResultsMultiplier, boolean hasIndex, boolean isPR, boolean containsInnerQuery) throws Exception {
    int numInstances = numObjects * objectsAndResultsMultiplier;
   
    if (hasIndex && isPR) {
      numInstances += numObjects * objectsAndResultsMultiplier;
    }
    assertEquals("Unexpected number of Portfolio instances" + queryString, numInstances, Portfolio.instanceCount.get());
    
    //execute query
    QueryService qs = utils.getCache().getQueryService();
    Query query = qs.newQuery(queryString);
    SelectResults results = (SelectResults) query.execute();
    assertEquals("No results were found", expectedResultsSize * objectsAndResultsMultiplier, results.size());
    for (Object o: results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        p.status = "discardStatus";
      }
      else {
        Struct struct = (Struct)o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        p.status = "discardStatus";
      }
    }
    if (!hasIndex && isPR) {
      //Copy on read is false, due to logic in local region
      //when we deserialize, we end up caching the deserialized value
      //This is why we don't have to worry about inner queries increasing the count
      numInstances += numObjects * objectsAndResultsMultiplier;
    }
    assertEquals("Unexpected number of Portfolio instances" + queryString, numInstances, Portfolio.instanceCount.get());

    results = (SelectResults) query.execute();
    assertEquals("No results were found", expectedResultsSize * objectsAndResultsMultiplier, results.size());
    for (Object o: results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        assertEquals("status should have been changed", "discardStatus", p.status);
      }
      else {
        Struct struct = (Struct)o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        assertEquals("status should have been changed", "discardStatus", p.status);
      }
    }
    
    //Unlike the copy on read case, we do not need to increase the instance count
    //This is because of logic in LocalRegion where if copy on read is false, we cache the deserialized value
    assertEquals("Unexpected number of Portfolio instances" + queryString, numInstances, Portfolio.instanceCount.get());

  }
  
  
}

