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

package org.apache.geode.cache.query;

import static org.apache.geode.cache.query.data.TestData.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.junit.categories.IntegrationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @implNote This test class has been designed to test the query execution using the LocalDataSet.executeQuery
 * where one of the parameters passed is the bucket set to be used.
 * Different variation of hashset variables are passed to the function to check for errors.
 */
@Category(IntegrationTest.class)
public class QueryWithBucketParameterIntegrationTest {
  DefaultQuery queryExecutor;
  LocalDataSet lds;

  public QueryWithBucketParameterIntegrationTest() {}

  @Before
  public void setUp() throws Exception {
    String regionName = "pr1";
    int totalBuckets = 40;
    int numValues = 80;
    CacheUtils.startCache();
    Cache cache = CacheUtils.getCache();
    PartitionAttributesFactory pAFactory = getPartitionAttributesFactoryWithPartitionResolver(totalBuckets);
    RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION);
    rf.setPartitionAttributes(pAFactory.create());
    PartitionedRegion pr1 = (PartitionedRegion) rf.create(regionName);
    populateRegion(pr1, numValues);
    QueryService qs = pr1.getCache().getQueryService();
    String query = "select distinct e1.value from /pr1 e1";
    queryExecutor = (DefaultQuery)CacheUtils.getQueryService().newQuery(
      query);
    Set<Integer> set = createAndPopulateSet(totalBuckets);
    lds = new LocalDataSet(pr1, set);
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  private PartitionAttributesFactory getPartitionAttributesFactoryWithPartitionResolver(int totalBuckets) {
    PartitionAttributesFactory pAFactory = new PartitionAttributesFactory();
    pAFactory.setRedundantCopies(1).setTotalNumBuckets(totalBuckets).setPartitionResolver(
      getPartitionResolver());
    return pAFactory;
  }

  private PartitionResolver getPartitionResolver() {
    return new PartitionResolver() {
      public String getName()
      {
        return "PartitionResolverForTest";
      }
      public Serializable getRoutingObject(EntryOperation opDetails)
      {
        return (Serializable)opDetails.getKey();
      }
      public void close() {}
    };
  }

  @Test
  public void testQueryExecuteWithEmptyBucketListExpectNoResults() throws Exception
  {
    SelectResults r = (SelectResults)lds.executeQuery(queryExecutor, null, new HashSet<Integer>());
    assertTrue("Received: A non-empty result collection, expected : Empty result collection", r.isEmpty());
  }

  @Test
  public void testQueryExecuteWithNullBucketListExpectNonEmptyResultSet() throws Exception
  {
    SelectResults r = (SelectResults)lds.executeQuery(queryExecutor, null, null);
    assertFalse("Received: An empty result collection, expected : Non-empty result collection", r.isEmpty());
  }

  @Test
  public void testQueryExecuteWithNonEmptyBucketListExpectNonEmptyResultSet() throws Exception
  {
    int nTestBucketNumber = 15;
    Set<Integer> nonEmptySet = createAndPopulateSet(nTestBucketNumber);
    SelectResults r = (SelectResults)lds.executeQuery(queryExecutor, null, nonEmptySet);
    assertFalse("Received: An empty result collection, expected : Non-empty result collection", r.isEmpty());
  }

  @Test(expected = QueryInvocationTargetException.class)
  public void testQueryExecuteWithLargerBucketListThanExistingExpectQueryInvocationTargetException() throws Exception
  {
    int nTestBucketNumber = 45;
    Set<Integer> overflowSet = createAndPopulateSet(nTestBucketNumber);
    SelectResults r = (SelectResults)lds.executeQuery(queryExecutor, null, overflowSet);
  }
}