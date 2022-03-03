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
package org.apache.geode.cache.query.internal;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.QueryTestUtils;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class CopyOnReadQueryJUnitTest {

  private static final int numObjects = 10;
  private static final int objectsAndResultsMultiplier = 100;
  private static final String regionName = "portfolios";
  private static final String indexName = "testIndex";

  private QueryTestUtils utils;
  private final String[] queries =
      {"select * from " + SEPARATOR + regionName + " p where p.indexKey = 1",
          "select distinct * from " + SEPARATOR + regionName
              + " p where p.indexKey = 1 order by p.indexKey",
          "select * from " + SEPARATOR + regionName
              + " p, p.positions.values pv where pv.secId = '1'",
          "select * from " + SEPARATOR + regionName + " p where p in (select * from " + SEPARATOR
              + regionName
              + " pi where pi.indexKey = 1)"};

  private final int[] expectedResults = {1, 1, 1, 1};

  private final boolean[] containsInnerQuery = {false, false, false, true};

  @Before
  public void setUp() throws java.lang.Exception {
    utils = new QueryTestUtils();
    utils.createCache(null);
    utils.getCache().setCopyOnRead(true);
    Portfolio.resetInstanceCount();
  }

  @After
  public void tearDown() throws java.lang.Exception {
    utils.getCache().getQueryService().removeIndexes();
    utils.closeCache();
  }

  private void createData(Region region, int numObjects, int objectsAndResultsMultiplier) {
    for (int i = 0; i < numObjects; i++) {
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

  // Test copy on read with no index
  @Test
  public void testCopyOnReadWithNoIndexWithLocalRegion() throws Exception {
    utils.createLocalRegion(regionName);
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier,
        false, false);
  }

  @Test
  public void testCopyOnReadWithNoIndexWithReplicatedRegion() throws Exception {
    utils.createReplicateRegion(regionName);
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier,
        false, false);
  }

  @Test
  public void testCopyOnReadWithNoIndexWithPartitionedRegion() throws Exception {
    utils.createPartitionRegion(regionName, null);
    helpExecuteQueriesCopyOnRead(queries, expectedResults, numObjects, objectsAndResultsMultiplier,
        false, true);
  }

  // Test copy on read false with no index
  @Test
  public void testCopyOnReadFalseWithNoIndexWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createLocalRegion(regionName);
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, false, false);
  }

  @Test
  public void testCopyOnReadFalseWithNoIndexWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createReplicateRegion(regionName);
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, false, false);
  }

  @Test
  public void testCopyOnReadFalseWithNoIndexWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    for (int i = 0; i < queries.length; i++) {
      Portfolio.instanceCount.set(0);
      utils.createPartitionRegion(regionName, null);
      Region region = utils.getCache().getRegion(SEPARATOR + regionName);
      createData(region, numObjects, objectsAndResultsMultiplier);
      helpTestCopyOnReadFalse(queries[i], expectedResults[i], numObjects,
          objectsAndResultsMultiplier, false, true, containsInnerQuery[i]);
      region.destroyRegion();
    }
  }

  @Test
  public void testCopyOnReadFalseWithHashIndexWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createLocalRegion(regionName);
    utils.createHashIndex(indexName, "p.indexKey", SEPARATOR + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, true, false);
  }

  @Test
  public void testCopyOnReadFalseWithHashIndexWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createReplicateRegion(regionName);
    utils.createHashIndex(indexName, "p.indexKey", SEPARATOR + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, true, false);
  }

  @Test
  public void testCopyOnReadFalseWithHashIndexWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createPartitionRegion(regionName, null);
    utils.createHashIndex(indexName, "p.indexKey", SEPARATOR + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, true, true);
  }

  @Test
  public void testCopyOnReadFalseWithCompactRangeIndexWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "p.indexKey", SEPARATOR + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, true, false);
  }

  @Test
  public void testCopyOnReadFalseWithCompactRangeIndexWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "p.indexKey", SEPARATOR + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, true, false);
  }

  @Test
  public void testCopyOnReadFalseWithCompactRangeIndexWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "p.indexKey", SEPARATOR + regionName + " p");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, true, true);
  }

  @Test
  public void testCopyOnReadFalseWithRangeIndexWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "p.indexKey",
        SEPARATOR + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, true, false);
  }

  @Test
  public void testCopyOnReadFalseWithRangeIndexWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "p.indexKey",
        SEPARATOR + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, true, false);
  }

  @Test
  public void testCopyOnReadFalseWithRangeIndexWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "p.indexKey",
        SEPARATOR + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, true, true);
  }

  @Test
  public void testCopyOnReadFalseWithRangeIndexTupleWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "pv.secId", SEPARATOR + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, true, false);
  }

  @Test
  public void testCopyOnReadFalseWithRangeIndexTupleWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "pv.secId", SEPARATOR + regionName + " p, p.positions.values pv");
    helpExecuteQueriesCopyOnReadFalse(queries, expectedResults, numObjects,
        objectsAndResultsMultiplier, true, false);
  }

  private void helpExecuteQueriesCopyOnRead(String[] queries, int[] expectedResults, int numObjects,
      int objectsAndResultsMultiplier, boolean hasIndex, boolean isPR) throws Exception {
    Region region = utils.getCache().getRegion(SEPARATOR + regionName);
    createData(region, numObjects, objectsAndResultsMultiplier);
    for (int i = 0; i < queries.length; i++) {
      Portfolio.instanceCount.set(numObjects * objectsAndResultsMultiplier);
      if (hasIndex && isPR) {
        Portfolio.instanceCount.set(numObjects * objectsAndResultsMultiplier * 2);
      }
      helpTestCopyOnRead(queries[i], expectedResults[i], numObjects, objectsAndResultsMultiplier,
          hasIndex, isPR, containsInnerQuery[i]);
    }
  }

  private void helpExecuteQueriesCopyOnReadFalse(String[] queries, int[] expectedResults,
      int numObjects, int objectsAndResultsMultiplier, boolean hasIndex, boolean isPR)
      throws Exception {
    Region region = utils.getCache().getRegion(SEPARATOR + regionName);
    createData(region, numObjects, objectsAndResultsMultiplier);
    for (int i = 0; i < queries.length; i++) {
      Portfolio.instanceCount.set(numObjects * objectsAndResultsMultiplier);
      if (hasIndex && isPR) {
        Portfolio.instanceCount.set(numObjects * objectsAndResultsMultiplier * 2);
      }
      helpTestCopyOnReadFalse(queries[i], expectedResults[i], numObjects,
          objectsAndResultsMultiplier, hasIndex, isPR, containsInnerQuery[i]);
    }
  }

  private void helpTestCopyOnRead(String queryString, int expectedResultsSize, int numObjects,
      int objectsAndResultsMultiplier, boolean hasIndex, boolean isPR, boolean containsInnerQuery)
      throws Exception {
    int expectedResultsSizeMultiplied = expectedResultsSize * objectsAndResultsMultiplier;
    int numInstances = numObjects * objectsAndResultsMultiplier;

    // We are a 1 VM test, replicated regions would put the actual domain object into the cache
    if (hasIndex && isPR) {
      // If we are PR we serialize the values
      // BUT if we have an index, we deserialize the values, so our instance count is double of our
      // put amount so far
      numInstances += numObjects * objectsAndResultsMultiplier;
    }
    assertEquals("Unexpected number of Portfolio instances", numInstances,
        Portfolio.instanceCount.get());

    // execute query
    QueryService qs = utils.getCache().getQueryService();
    Query query = qs.newQuery(queryString);
    SelectResults results = (SelectResults) query.execute();
    assertEquals("Results did not match expected count for query" + queryString,
        expectedResultsSizeMultiplied, results.size());
    for (Object o : results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        p.status = "discardStatus";
      } else {
        Struct struct = (Struct) o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        p.status = "discardStatus";
      }
    }

    if (!hasIndex && isPR) {
      // We are PR and we do not have an index, so we must deserialize all the values in the cache
      // at this point
      numInstances += (numObjects * objectsAndResultsMultiplier);
      if (containsInnerQuery) {
        // if we have an inner query, it would deserialize all objects for the inner query as well
        numInstances += numObjects * objectsAndResultsMultiplier;
      }
    }
    // So with all the deserialized objects we must also combine the query results of the query due
    // to copy on read
    assertEquals("Unexpected number of Portfolio instances for query " + query,
        numInstances + expectedResultsSizeMultiplied, Portfolio.instanceCount.get());


    results = (SelectResults) query.execute();
    assertEquals("No results were found", expectedResultsSizeMultiplied, results.size());
    for (Object o : results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        assertEquals("status should not have been changed", "testStatus", p.status);
      } else {
        Struct struct = (Struct) o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        assertEquals("status should not have been changed", "testStatus", p.status);
      }
    }
    if (!hasIndex && isPR) {
      // Again, because we have no index, we must deserialize the values in the region
      numInstances += (numObjects * objectsAndResultsMultiplier);
      if (containsInnerQuery) {
        // If we have an inner query, we must also deserialize the values in the region for this
        // query
        // We have some interesting logic in LocalRegion when deserializing. Based on these flags
        // we do not store the deserialized instance back in the cache
        numInstances += numObjects * objectsAndResultsMultiplier;
      }
    }
    // So with all the deserialized objects we must also combine the query results of two queries at
    // this point. These results themselves would have been copied
    assertEquals("Unexpected number of Portfolio instances",
        numInstances + expectedResultsSizeMultiplied * 2, Portfolio.instanceCount.get());
  }

  private void helpTestCopyOnReadFalse(String queryString, int expectedResultsSize, int numObjects,
      int objectsAndResultsMultiplier, boolean hasIndex, boolean isPR, boolean containsInnerQuery)
      throws Exception {
    int numInstances = numObjects * objectsAndResultsMultiplier;

    if (hasIndex && isPR) {
      numInstances += numObjects * objectsAndResultsMultiplier;
    }
    assertEquals("Unexpected number of Portfolio instances" + queryString, numInstances,
        Portfolio.instanceCount.get());

    // execute query
    QueryService qs = utils.getCache().getQueryService();
    Query query = qs.newQuery(queryString);
    SelectResults results = (SelectResults) query.execute();
    assertEquals("No results were found for query: " + queryString,
        expectedResultsSize * objectsAndResultsMultiplier, results.size());
    for (Object o : results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        p.status = "discardStatus";
      } else {
        Struct struct = (Struct) o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        p.status = "discardStatus";
      }
    }
    if (!hasIndex && isPR) {
      // Copy on read is false, due to logic in local region
      // when we deserialize, we end up caching the deserialized value
      // This is why we don't have to worry about inner queries increasing the count
      numInstances += numObjects * objectsAndResultsMultiplier;
    }
    assertEquals("Unexpected number of Portfolio instances" + queryString, numInstances,
        Portfolio.instanceCount.get());

    results = (SelectResults) query.execute();
    assertEquals("No results were found", expectedResultsSize * objectsAndResultsMultiplier,
        results.size());
    for (Object o : results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        assertEquals("status should have been changed", "discardStatus", p.status);
      } else {
        Struct struct = (Struct) o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        assertEquals("status should have been changed", "discardStatus", p.status);
      }
    }

    // Unlike the copy on read case, we do not need to increase the instance count
    // This is because of logic in LocalRegion where if copy on read is false, we cache the
    // deserialized value
    assertEquals("Unexpected number of Portfolio instances" + queryString, numInstances,
        Portfolio.instanceCount.get());
  }
}
