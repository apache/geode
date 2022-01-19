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
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.QueryExecutionContext;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionQueryEvaluator;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
@SuppressWarnings("serial")
public class PRQueryDUnitTest extends CacheTestCase {

  private static final int NUMBER_OF_PUTS = 100;
  private static final Object[] EMPTY_PARAMETERS = new Object[0];
  private static final int[] LIMIT = new int[] {10, 15, 30, 0, 1, 9};
  private static final int REDUNDANCY = 0;

  private String regionName;
  private int numberOfBuckets;

  private VM accessor;
  private VM datastore1;
  private VM datastore2;
  private VM datastore3;

  @Before
  public void setUp() throws Exception {
    accessor = getHost(0).getVM(0);
    datastore1 = getHost(0).getVM(1);
    datastore2 = getHost(0).getVM(2);
    datastore3 = getHost(0).getVM(3);

    regionName = getUniqueName();

    numberOfBuckets = 11;
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
    invokeInEveryVM(() -> PRQueryDUnitHelper.setCache(null));
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.put(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.data.*");
    return config;
  }

  /**
   * Test data loss (bucket 0) while the PRQueryEvaluator is processing the query loop
   */
  @Test
  public void testDataLossDuringQueryProcessor() throws Exception {
    datastore1.invoke(() -> {
      createPartitionedRegion();
    });
    datastore2.invoke(() -> {
      createPartitionedRegion();
    });

    Region<Integer, String> region = createPartitionedRegionAccessor();

    // Create bucket zero, one and two
    region.put(0, "zero");
    region.put(1, "one");
    region.put(2, "two");

    addIgnoredException("Data loss detected");

    DefaultQuery query = (DefaultQuery) getCache().getQueryService()
        .newQuery("select distinct * from " + region.getFullPath());
    final ExecutionContext executionContext = new QueryExecutionContext(null, getCache(), query);
    SelectResults results =
        query.getSimpleSelect().getEmptyResultSet(EMPTY_PARAMETERS, getCache(), query);

    Set<Integer> buckets = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      buckets.add(i);
    }

    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    PartitionedRegionQueryEvaluator queryEvaluator =
        new PartitionedRegionQueryEvaluator(partitionedRegion.getSystem(), partitionedRegion, query,
            executionContext,
            EMPTY_PARAMETERS, results, buckets);

    DisconnectingTestHook testHook = new DisconnectingTestHook();
    assertThatThrownBy(() -> queryEvaluator.queryBuckets(testHook))
        .isInstanceOf(QueryException.class);
    assertThat(testHook.isDone()).isTrue();
  }

  @Test
  public void testQueryResultsFromMembers() throws Exception {
    numberOfBuckets = 10;

    datastore1.invoke(() -> {
      createPartitionedRegion();
    });
    datastore2.invoke(() -> {
      createPartitionedRegion();
    });

    datastore3.invoke(() -> {
      Region<Integer, Portfolio> region = createPartitionedRegion();

      for (int i = 1; i <= NUMBER_OF_PUTS; i++) {
        region.put(i, new Portfolio(i));
      }

      Set<Integer> bucketsToQuery = new HashSet<>();
      for (int i = 0; i < numberOfBuckets; i++) {
        bucketsToQuery.add(i);
      }

      String[] queries =
          new String[] {"select * from " + SEPARATOR + regionName + " LIMIT " + LIMIT[0],
              "select * from " + SEPARATOR + regionName + " LIMIT " + LIMIT[1],
              "select * from " + SEPARATOR + regionName + " LIMIT " + LIMIT[2],
              "select * from " + SEPARATOR + regionName + " LIMIT " + LIMIT[3],
              "select * from " + SEPARATOR + regionName + " LIMIT " + LIMIT[4],
              "select * from " + SEPARATOR + regionName + " where ID > 10 LIMIT " + LIMIT[5],};

      for (int i = 0; i < queries.length; i++) {
        DefaultQuery query = (DefaultQuery) getCache().getQueryService().newQuery(queries[i]);
        final ExecutionContext executionContext =
            new QueryExecutionContext(null, getCache(), query);
        SelectResults results =
            query.getSimpleSelect().getEmptyResultSet(EMPTY_PARAMETERS, getCache(), query);

        PartitionedRegion partitionedRegion = (PartitionedRegion) region;
        PartitionedRegionQueryEvaluator queryEvaluator =
            new PartitionedRegionQueryEvaluator(partitionedRegion.getSystem(), partitionedRegion,
                query, executionContext, EMPTY_PARAMETERS, results, bucketsToQuery);

        CollatingTestHook testHook = new CollatingTestHook(queryEvaluator);
        queryEvaluator.queryBuckets(testHook);

        for (Map.Entry<Object, Integer> mapEntry : testHook.getResultsPerMember().entrySet()) {
          Integer resultsCount = mapEntry.getValue();
          assertThat(resultsCount.intValue()).isEqualTo(LIMIT[i]);
        }
      }
    });
  }

  @Test
  public void testQueryResultsFromMembersWithAccessor() throws Exception {
    datastore1.invoke(() -> {
      createPartitionedRegion();
    });
    datastore2.invoke(() -> {
      createPartitionedRegion();
    });

    Region<Integer, Portfolio> region = createPartitionedRegionAccessor();

    for (int i = 1; i <= NUMBER_OF_PUTS; i++) {
      region.put(i, new Portfolio(i));
    }

    Set<Integer> buckets = new HashSet<>();
    for (int i = 0; i < numberOfBuckets; i++) {
      buckets.add(i);
    }

    String[] queries =
        new String[] {"select * from " + SEPARATOR + regionName + " LIMIT " + LIMIT[0],
            "select * from " + SEPARATOR + regionName + " LIMIT " + LIMIT[1],
            "select * from " + SEPARATOR + regionName + " LIMIT " + LIMIT[2],
            "select * from " + SEPARATOR + regionName + " LIMIT " + LIMIT[3],
            "select * from " + SEPARATOR + regionName + " LIMIT " + LIMIT[4],
            "select * from " + SEPARATOR + regionName + " where ID > 10 LIMIT " + LIMIT[5],};

    for (int i = 0; i < queries.length; i++) {
      DefaultQuery query = (DefaultQuery) getCache().getQueryService().newQuery(queries[i]);
      final ExecutionContext executionContext = new QueryExecutionContext(null, getCache(), query);
      SelectResults results =
          query.getSimpleSelect().getEmptyResultSet(EMPTY_PARAMETERS, getCache(), query);

      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      PartitionedRegionQueryEvaluator queryEvaluator =
          new PartitionedRegionQueryEvaluator(partitionedRegion.getSystem(), partitionedRegion,
              query, executionContext, EMPTY_PARAMETERS, results, buckets);

      CollatingTestHook testHook = new CollatingTestHook(queryEvaluator);
      queryEvaluator.queryBuckets(testHook);

      for (Map.Entry<Object, Integer> mapEntry : testHook.getResultsPerMember().entrySet()) {
        Integer resultsCount = mapEntry.getValue();
        assertThat(resultsCount.intValue()).isEqualTo(LIMIT[i]);
      }
    }
  }

  /**
   * Simulate a data loss (buckets 0 and 2) before the PRQueryEvaluator begins the query loop
   */
  @Test
  public void testSimulatedDataLossBeforeQueryProcessor() throws Exception {
    numberOfBuckets = 11;

    datastore1.invoke(() -> {
      createPartitionedRegion();
    });
    datastore2.invoke(() -> {
      createPartitionedRegion();
    });

    accessor.invoke(() -> {
      createPartitionedRegionAccessor();
    });

    addIgnoredException("Data loss detected", accessor);

    accessor.invoke(() -> {
      Region<Integer, String> region = getCache().getRegion(regionName);

      // Create bucket one
      region.put(1, "one");

      DefaultQuery query = (DefaultQuery) getCache().getQueryService()
          .newQuery("select distinct * from " + SEPARATOR + regionName);
      final ExecutionContext executionContext = new QueryExecutionContext(null, getCache(), query);
      SelectResults results =
          query.getSimpleSelect().getEmptyResultSet(EMPTY_PARAMETERS, getCache(), query);

      // Fake data loss
      Set<Integer> buckets = new HashSet<>();
      for (int i = 0; i < 3; i++) {
        buckets.add(i);
      }

      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      PartitionedRegionQueryEvaluator queryEvaluator =
          new PartitionedRegionQueryEvaluator(partitionedRegion.getSystem(), partitionedRegion,
              query, executionContext, EMPTY_PARAMETERS, results, buckets);

      assertThatThrownBy(() -> queryEvaluator.queryBuckets(null))
          .isInstanceOf(QueryException.class);
    });
  }

  private PartitionedRegion createPartitionedRegion() {
    Cache cache = getCache();

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(REDUNDANCY);
    partitionAttributesFactory.setTotalNumBuckets(numberOfBuckets);

    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    return (PartitionedRegion) regionFactory.create(regionName);
  }

  private PartitionedRegion createPartitionedRegionAccessor() {
    Cache cache = getCache();

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(REDUNDANCY);
    partitionAttributesFactory.setTotalNumBuckets(numberOfBuckets);
    partitionAttributesFactory.setLocalMaxMemory(0);

    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    return (PartitionedRegion) regionFactory.create(regionName);
  }

  /**
   * Test hook that disconnects when invoked.
   */
  class DisconnectingTestHook implements PartitionedRegionQueryEvaluator.TestHook {
    private boolean done = false;

    @Override
    public void hook(int spot) throws RuntimeException {
      if (spot == 4) {
        synchronized (this) {
          if (done) {
            return;
          }
          done = true;
        }
        datastore1.invoke(JUnit4DistributedTestCase::disconnectFromDS);
        datastore2.invoke(JUnit4DistributedTestCase::disconnectFromDS);
      }
    }

    boolean isDone() {
      synchronized (this) {
        return done;
      }
    }
  }

  /**
   * Test hook that collates all results per member.
   */
  class CollatingTestHook implements PartitionedRegionQueryEvaluator.TestHook {

    private final Map<Object, Integer> resultsPerMember;
    private final PartitionedRegionQueryEvaluator queryEvaluator;

    CollatingTestHook(PartitionedRegionQueryEvaluator queryEvaluator) {
      resultsPerMember = new HashMap<>();
      this.queryEvaluator = queryEvaluator;
    }

    @Override
    public void hook(int spot) throws RuntimeException {
      if (spot != 3) {
        return;
      }
      for (Object mapEntryObject : queryEvaluator.getResultsPerMember().entrySet()) {
        Map.Entry<Object, Collection<Collection<Object>>> mapEntry = (Map.Entry) mapEntryObject;
        Collection<Collection<Object>> allResults = mapEntry.getValue();
        for (Collection<Object> results : allResults) {
          if (resultsPerMember.containsKey(mapEntry.getKey())) {
            resultsPerMember.put(mapEntry.getKey(),
                results.size() + resultsPerMember.get(mapEntry.getKey()));
          } else {
            resultsPerMember.put(mapEntry.getKey(), results.size());
          }
        }
      }
    }

    Map<Object, Integer> getResultsPerMember() {
      return resultsPerMember;
    }
  }
}
