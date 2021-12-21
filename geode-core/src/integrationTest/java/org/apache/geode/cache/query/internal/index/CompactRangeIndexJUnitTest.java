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
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.QueryTestUtils;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.DefaultQuery.TestHook;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.util.internal.UncheckedUtils;

@Category({OQLIndexTest.class})
public class CompactRangeIndexJUnitTest {

  private QueryTestUtils utils;
  private Index index;

  @Before
  public void setUp() {
    IndexManager.INDEX_ELEMARRAY_THRESHOLD_FOR_TESTING = 3;
    utils = new QueryTestUtils();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    utils.initializeQueryMap();
    utils.createCache(props);
    utils.createReplicateRegion("exampleRegion");
  }

  @Test
  public void testCompactRangeIndex() throws Exception {
    index = utils.createIndex("type", "\"type\"", SEPARATOR + "exampleRegion");
    putValues(9);
    isUsingIndexElemArray("type1");
    putValues(10);
    isUsingConcurrentHashSet("type1");
    utils.removeIndex("type", SEPARATOR + "exampleRegion");
    executeQueryWithAndWithoutIndex(4);
    putOffsetValues(2);
    executeQueryWithCount();
    executeQueryWithAndWithoutIndex(3);
    executeRangeQueryWithDistinct(8);
    executeRangeQueryWithoutDistinct(9);
  }

  /**
   * Tests adding entries to compact range index where the key is null fixes bug 47151 where null
   * keyed entries would be removed after being added
   */
  @Test
  public void testNullKeyCompactRangeIndex() throws Exception {
    index = utils.createIndex("indexName", "status", SEPARATOR + "exampleRegion");
    Region region = utils.getCache().getRegion("exampleRegion");

    // create objects
    int numObjects = 10;
    for (int i = 1; i <= numObjects; i++) {
      Portfolio p = new Portfolio(i);
      p.status = null;
      region.put("KEY-" + i, p);
    }
    // execute query and check result size
    QueryService qs = utils.getCache().getQueryService();
    SelectResults results = (SelectResults) qs
        .newQuery("Select * from " + SEPARATOR + "exampleRegion r where r.status = null").execute();
    assertEquals("Null matched Results expected", numObjects, results.size());
  }

  /**
   * Tests adding entries to compact range index where the the key of an indexed map field is null.
   */
  @Test
  public void testNullMapKeyCompactRangeIndex() throws Exception {
    index = utils.createIndex("indexName", "positions[*]", SEPARATOR + "exampleRegion");
    Region region = utils.getCache().getRegion("exampleRegion");

    // create objects
    int numObjects = 10;
    for (int i = 1; i <= numObjects; i++) {
      Portfolio p = new Portfolio(i);
      p.status = null;
      p.getPositions().put(null, "something");
      region.put("KEY-" + i, p);
    }
    // execute query and check result size
    QueryService qs = utils.getCache().getQueryService();
    SelectResults results = (SelectResults) qs
        .newQuery(
            "Select * from " + SEPARATOR + "exampleRegion r where r.position[null] = something")
        .execute();
    assertEquals("Null matched Results expected", numObjects, results.size());
  }

  /**
   * Tests adding entries to compact range index where the the key of an indexed map field is null.
   */
  @Test
  public void testNullMapKeyCompactRangeIndexCreateIndexLater() throws Exception {
    Region region = utils.getCache().getRegion("exampleRegion");

    // create objects
    int numObjects = 10;
    for (int i = 1; i <= numObjects; i++) {
      Portfolio p = new Portfolio(i);
      p.status = null;
      p.getPositions().put(null, "something");
      region.put("KEY-" + i, p);
    }
    index = utils.createIndex("indexName", "positions[*]", SEPARATOR + "exampleRegion");
    // execute query and check result size
    QueryService qs = utils.getCache().getQueryService();
    SelectResults results = (SelectResults) qs
        .newQuery(
            "Select * from " + SEPARATOR + "exampleRegion r where r.position[null] = something")
        .execute();
    assertEquals("Null matched Results expected", numObjects, results.size());
  }

  /**
   * Tests race condition where we possibly were missing remove calls due to transitioning to an
   * empty index elem before adding the entries the fix is to add the entries to the elem and then
   * transition to that elem
   */
  @Test
  public void testCompactRangeIndexMemoryIndexStoreMaintenance() throws Exception {
    try {
      index = utils.createIndex("compact range index", "p.status", SEPARATOR + "exampleRegion p");
      final Region r = utils.getCache().getRegion(SEPARATOR + "exampleRegion");
      Portfolio p0 = new Portfolio(0);
      p0.status = "active";
      final Portfolio p1 = new Portfolio(1);
      p1.status = "active";
      r.put("0", p0);

      DefaultQuery.testHook = new MemoryIndexStoreREToIndexElemTestHook();
      final CountDownLatch threadsDone = new CountDownLatch(2);

      Thread t1 = new Thread(new Runnable() {
        @Override
        public void run() {
          r.put("1", p1);
          threadsDone.countDown();
        }
      });
      t1.start();

      Thread t0 = new Thread(new Runnable() {
        @Override
        public void run() {
          r.remove("0");
          threadsDone.countDown();

        }
      });
      t0.start();
      threadsDone.await(90, TimeUnit.SECONDS);
      QueryService qs = utils.getCache().getQueryService();
      SelectResults results = (SelectResults) qs
          .newQuery("Select * from " + SEPARATOR + "exampleRegion r where r.status='active'")
          .execute();
      // the remove should have happened
      assertEquals(1, results.size());

      results = (SelectResults) qs
          .newQuery("Select * from " + SEPARATOR + "exampleRegion r where r.status!='inactive'")
          .execute();
      assertEquals(1, results.size());

      CompactRangeIndex cindex = (CompactRangeIndex) index;
      MemoryIndexStore indexStore = (MemoryIndexStore) cindex.getIndexStorage();
      CloseableIterator iterator = indexStore.get("active");
      int count = 0;
      while (iterator.hasNext()) {
        count++;
        iterator.next();
      }
      assertEquals("incorrect number of entries in collection", 1, count);
    } finally {
      DefaultQuery.testHook = null;
    }
  }

  /**
   * Tests race condition when we are transitioning index collection from elem array to concurrent
   * hash set The other thread could remove from the empty concurrent hash set. Instead we now set a
   * token, do all the puts into a collection and then unsets the token to the new collection
   */
  @Test
  public void testMemoryIndexStoreMaintenanceTransitionFromElemArrayToTokenToConcurrentHashSet()
      throws Exception {
    try {
      index = utils.createIndex("compact range index", "p.status", SEPARATOR + "exampleRegion p");
      final Region r = utils.getCache().getRegion(SEPARATOR + "exampleRegion");
      Portfolio p0 = new Portfolio(0);
      p0.status = "active";
      Portfolio p1 = new Portfolio(1);
      p1.status = "active";
      final Portfolio p2 = new Portfolio(2);
      p2.status = "active";
      Portfolio p3 = new Portfolio(3);
      p3.status = "active";
      r.put("0", p0);
      r.put("1", p1);
      r.put("3", p3);

      // now we set the test hook. That way previous calls would not affect the test hooks
      DefaultQuery.testHook = new MemoryIndexStoreIndexElemToTokenToConcurrentHashSetTestHook();
      final CountDownLatch threadsDone = new CountDownLatch(2);
      Thread t2 = new Thread(new Runnable() {
        @Override
        public void run() {
          r.put("2", p2);
          threadsDone.countDown();

        }
      });
      t2.start();

      Thread t0 = new Thread(new Runnable() {
        @Override
        public void run() {
          r.remove("0");
          threadsDone.countDown();
        }
      });
      t0.start();

      threadsDone.await(90, TimeUnit.SECONDS);
      QueryService qs = utils.getCache().getQueryService();
      SelectResults results = (SelectResults) qs
          .newQuery("Select * from " + SEPARATOR + "exampleRegion r where r.status='active'")
          .execute();
      // the remove should have happened
      assertEquals(3, results.size());

      results = (SelectResults) qs
          .newQuery("Select * from " + SEPARATOR + "exampleRegion r where r.status!='inactive'")
          .execute();
      assertEquals(3, results.size());

      CompactRangeIndex cindex = (CompactRangeIndex) index;
      MemoryIndexStore indexStore = (MemoryIndexStore) cindex.getIndexStorage();
      CloseableIterator iterator = indexStore.get("active");
      int count = 0;
      while (iterator.hasNext()) {
        count++;
        iterator.next();
      }
      assertEquals("incorrect number of entries in collection", 3, count);
    } finally {
      DefaultQuery.testHook = null;
    }
  }

  @Test
  public void testInvalidTokens() throws Exception {
    final Region r = utils.getCache().getRegion(SEPARATOR + "exampleRegion");
    r.put("0", new Portfolio(0));
    r.invalidate("0");
    index = utils.createIndex("compact range index", "p.status", SEPARATOR + "exampleRegion p");
    QueryService qs = utils.getCache().getQueryService();
    SelectResults results = (SelectResults) qs
        .newQuery("Select * from " + SEPARATOR + "exampleRegion r where r.status='active'")
        .execute();
    // the remove should have happened
    assertEquals(0, results.size());

    results = (SelectResults) qs
        .newQuery("Select * from " + SEPARATOR + "exampleRegion r where r.status!='inactive'")
        .execute();
    assertEquals(0, results.size());

    CompactRangeIndex cindex = (CompactRangeIndex) index;
    MemoryIndexStore indexStore = (MemoryIndexStore) cindex.getIndexStorage();
    CloseableIterator iterator = indexStore.get(QueryService.UNDEFINED);
    int count = 0;
    while (iterator.hasNext()) {
      count++;
      iterator.next();
    }
    assertEquals("incorrect number of entries in collection", 0, count);
  }

  @Test
  public void testUpdateInProgressWithMethodInvocationInIndexClauseShouldNotThrowException()
      throws Exception {
    try {
      CompactRangeIndex.TEST_ALWAYS_UPDATE_IN_PROGRESS = true;
      index = utils.createIndex("indexName", "getP1().getSharesOutstanding()",
          SEPARATOR + "exampleRegion");
      Region region = utils.getCache().getRegion("exampleRegion");

      // create objects
      int numObjects = 10;
      for (int i = 1; i <= numObjects; i++) {
        Portfolio p = new Portfolio(i);
        p.status = null;
        region.put("KEY-" + i, p);
      }
      // execute query and check result size
      QueryService qs = utils.getCache().getQueryService();
      SelectResults results = (SelectResults) qs
          .newQuery(
              "<trace>SELECT DISTINCT e.key FROM " + SEPARATOR
                  + "exampleRegion AS e WHERE e.ID = 1 AND e.getP1().getSharesOutstanding() >= -1 AND e.getP1().getSharesOutstanding() <= 1000 LIMIT 10 ")
          .execute();
    } finally {
      CompactRangeIndex.TEST_ALWAYS_UPDATE_IN_PROGRESS = false;
    }
  }

  private static class MemoryIndexStoreREToIndexElemTestHook implements TestHook {

    private final CountDownLatch readyToStartRemoveLatch;
    private final CountDownLatch waitForRemoveLatch;
    private final CountDownLatch waitForTransitioned;

    public MemoryIndexStoreREToIndexElemTestHook() {
      waitForRemoveLatch = new CountDownLatch(1);
      waitForTransitioned = new CountDownLatch(1);
      readyToStartRemoveLatch = new CountDownLatch(1);
    }

    @Override
    public void doTestHook(final SPOTS spot, final DefaultQuery _ignored,
        final ExecutionContext executionContext) {
      try {
        switch (spot) {
          case ATTEMPT_REMOVE:
            if (!readyToStartRemoveLatch.await(21, TimeUnit.SECONDS)) {
              throw new AssertionError("Time ran out waiting for other thread to initiate put");
            }
            break;
          case TRANSITIONED_FROM_REGION_ENTRY_TO_ELEMARRAY:
            readyToStartRemoveLatch.countDown();
            if (!waitForRemoveLatch.await(21, TimeUnit.SECONDS)) {
              throw new AssertionError("Time ran out waiting for other thread to initiate remove");
            }
            break;
          case BEGIN_REMOVE_FROM_ELEM_ARRAY:
            waitForRemoveLatch.countDown();
            if (waitForTransitioned.await(21, TimeUnit.SECONDS)) {
              throw new AssertionError(
                  "Time ran out waiting for transition from region entry to elem array");
            }
            break;
        }
      } catch (InterruptedException e) {
        throw new AssertionError("Interrupted while waiting for test to complete");
      }
    }
  }

  /**
   * Test hook that waits for another thread to begin removing The current thread should then
   * continue to set the token then continue and convert to chs while holding the lock to the elem
   * array still After the conversion of chs, the lock is released and then remove can proceed
   */
  private static class MemoryIndexStoreIndexElemToTokenToConcurrentHashSetTestHook
      implements TestHook {

    private final CountDownLatch waitForRemoveLatch;
    private final CountDownLatch waitForTransitioned;
    private final CountDownLatch waitForRetry;
    private final CountDownLatch readyToStartRemoveLatch;

    public MemoryIndexStoreIndexElemToTokenToConcurrentHashSetTestHook() {
      waitForRemoveLatch = new CountDownLatch(1);
      waitForTransitioned = new CountDownLatch(1);
      waitForRetry = new CountDownLatch(1);
      readyToStartRemoveLatch = new CountDownLatch(1);
    }

    @Override
    public void doTestHook(final SPOTS spot, final DefaultQuery _ignored,
        final ExecutionContext executionContext) {
      try {
        switch (spot) {
          case ATTEMPT_REMOVE:
            if (!readyToStartRemoveLatch.await(21, TimeUnit.SECONDS)) {
              throw new AssertionError("Time ran out waiting for other thread to initiate put");
            }
            break;
          case BEGIN_TRANSITION_FROM_ELEMARRAY_TO_CONCURRENT_HASH_SET:
            readyToStartRemoveLatch.countDown();
            if (!waitForRemoveLatch.await(21, TimeUnit.SECONDS)) {
              throw new AssertionError("Time ran out waiting for other thread to initiate remove");
            }
            break;
          case BEGIN_REMOVE_FROM_ELEM_ARRAY:
            waitForRemoveLatch.countDown();
            if (!waitForTransitioned.await(21, TimeUnit.SECONDS)) {
              throw new AssertionError(
                  "Time ran out waiting for transition from elem array to token");
            }
            break;
          case TRANSITIONED_FROM_ELEMARRAY_TO_TOKEN:
            waitForTransitioned.countDown();
            break;
        }
      } catch (InterruptedException e) {
        throw new AssertionError("Interrupted while waiting for test to complete");
      }
    }
  }

  /**
   * Tests adding entries to compact range index where there are undefined and null values
   * for the key
   */
  @Test
  public void testNullAndUndefinedValuesForMapKeyInCompactRangeIndex() throws Exception {
    index = utils.createIndex("indexName", "positions['SUN']", SEPARATOR + "exampleRegion");
    Region<Object, Object> region = utils.getCache().getRegion("exampleRegion");

    // create objects
    Portfolio p1 = new Portfolio(1);
    p1.positions = new HashMap<>();
    p1.positions.put("SUN", "yes");
    region.put("KEY-" + 1, p1);

    // Equivalent to having a null value for positions['SUN'] when querying
    Portfolio p2 = new Portfolio(2);
    p2.positions = new HashMap<>();
    p2.positions.put("ERIC", 2);
    region.put("KEY-" + 2, p2);

    // Undefined value for positions['SUN'] when querying
    Portfolio p3 = new Portfolio(3);
    p3.positions = null;
    region.put("KEY-" + 3, p3);

    // null value for positions['SUN']
    Portfolio p4 = new Portfolio(4);
    p4.positions = new HashMap<>();
    p4.positions.put("SUN", null);
    region.put("KEY-" + 4, p4);

    // execute query for null value and check result size
    QueryService qs = utils.getCache().getQueryService();
    SelectResults<Object> results = UncheckedUtils.uncheckedCast(qs
        .newQuery(
            "Select * from " + SEPARATOR + "exampleRegion r where r.positions['SUN'] = null")
        .execute());
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.contains(p2)).isTrue();
    assertThat(results.contains(p4)).isTrue();

    // execute query for not null value and check result size
    results = UncheckedUtils.uncheckedCast(qs
        .newQuery(
            "Select * from " + SEPARATOR + "exampleRegion r where r.positions['SUN'] != null")
        .execute());
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.contains(p1)).isTrue();
    assertThat(results.contains(p3)).isTrue();
  }

  private void putValues(int num) {
    Region region = utils.getRegion("exampleRegion");
    for (int i = 1; i <= num; i++) {
      region.put("KEY-" + i, new Portfolio(i));
    }
  }

  private void putOffsetValues(int num) {
    Region region = utils.getRegion("exampleRegion");
    for (int i = 1; i <= num; i++) {
      region.put("KEY-" + i, new Portfolio(i + 1));
    }
  }

  public void executeQueryWithCount() throws Exception {
    String[] queries = {"520"};
    for (Object result : utils.executeQueries(queries)) {
      if (result instanceof Collection) {
        for (Object e : (Collection) result) {
          if (e instanceof Integer) {
            assertEquals(10, ((Integer) e).intValue());
          }
        }
      }
    }
  }

  private void isUsingIndexElemArray(String key) {
    if (index instanceof CompactRangeIndex) {
      assertEquals(
          "Expected IndexElemArray but instanceForKey is "
              + getValuesFromMap(key).getClass().getName(),
          getValuesFromMap(key) instanceof IndexElemArray, true);
    } else {
      fail("Should have used CompactRangeIndex");
    }
  }

  private void isUsingConcurrentHashSet(String key) {
    if (index instanceof CompactRangeIndex) {
      assertEquals(
          "Expected concurrent hash set but instanceForKey is "
              + getValuesFromMap(key).getClass().getName(),
          getValuesFromMap(key) instanceof IndexConcurrentHashSet, true);
    } else {
      fail("Should have used CompactRangeIndex");
    }
  }

  private Object getValuesFromMap(String key) {
    MemoryIndexStore ind = (MemoryIndexStore) ((CompactRangeIndex) index).getIndexStorage();
    Map map = ind.valueToEntriesMap;
    Object entryValue = map.get(key);
    return entryValue;
  }

  public void executeQueryWithAndWithoutIndex(int expectedResults) {
    try {
      executeSimpleQuery(expectedResults);
    } catch (Exception e) {
      fail("Query execution failed. : " + e);
    }
    index = utils.createIndex("type", "\"type\"", SEPARATOR + "exampleRegion");
    try {
      executeSimpleQuery(expectedResults);
    } catch (Exception e) {
      fail("Query execution failed. : " + e);
    }
    utils.removeIndex("type", SEPARATOR + "exampleRegion");
  }

  private int executeSimpleQuery(int expResults) throws Exception {
    String[] queries = {"519"}; // SELECT * FROM /exampleRegion WHERE \"type\" = 'type1'
    int results = 0;
    for (Object result : utils.executeQueries(queries)) {
      if (result instanceof SelectResults) {
        Collection<?> collection = ((SelectResults<?>) result).asList();
        results = collection.size();
        assertEquals(expResults, results);
        for (Object e : collection) {
          if (e instanceof Portfolio) {
            assertEquals("type1", ((Portfolio) e).getType());
          }
        }
      }
    }
    return results;
  }

  private int executeRangeQueryWithDistinct(int expResults) throws Exception {
    String[] queries = {"181"};
    int results = 0;
    for (Object result : utils.executeQueries(queries)) {
      if (result instanceof SelectResults) {
        Collection<?> collection = ((SelectResults<?>) result).asList();
        results = collection.size();
        assertEquals(expResults, results);
        int[] ids = {};
        List expectedIds = new ArrayList(Arrays.asList(10, 9, 8, 7, 6, 5, 4, 3, 2));
        for (Object e : collection) {
          if (e instanceof Portfolio) {
            assertTrue(expectedIds.contains(((Portfolio) e).getID()));
            expectedIds.remove((Integer) ((Portfolio) e).getID());
          }
        }
      }
    }
    return results;
  }

  private int executeRangeQueryWithoutDistinct(int expResults) {
    String[] queries = {"181"};
    int results = 0;
    for (Object result : utils.executeQueriesWithoutDistinct(queries)) {
      if (result instanceof SelectResults) {
        Collection<?> collection = ((SelectResults<?>) result).asList();
        results = collection.size();
        assertEquals(expResults, results);
        List expectedIds = new ArrayList(Arrays.asList(10, 9, 8, 7, 6, 5, 4, 3, 3));
        for (Object e : collection) {
          if (e instanceof Portfolio) {
            assertTrue(expectedIds.contains(((Portfolio) e).getID()));
            expectedIds.remove((Integer) ((Portfolio) e).getID());
          }
        }
      }
    }
    return results;
  }

  @After
  public void tearDown() throws Exception {
    IndexManager.INDEX_ELEMARRAY_THRESHOLD_FOR_TESTING = -1;
    utils.closeCache();
  }

}
