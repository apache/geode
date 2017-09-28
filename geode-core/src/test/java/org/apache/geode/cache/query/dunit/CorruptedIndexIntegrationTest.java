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

package org.apache.geode.cache.query.dunit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class CorruptedIndexIntegrationTest extends JUnit4CacheTestCase {

  @Test
  public void putMustSucceedAndIndexInvalidatedWhenAPutCorruptsAnIndex() throws Exception {

    String queryString = "SELECT * FROM /REGION_NAME WHERE ID = 3";
    String regionName = "REGION_NAME";

    Cache cache = getCache();

    Region region =
        cache.createRegionFactory().setDataPolicy(DataPolicy.PARTITION).create(regionName);

    QueryService queryService = cache.getQueryService();
    Index idIndex = queryService.createIndex("idIndex", "ID", "/" + regionName);
    Index exceptionIndex =
        queryService.createIndex("exceptionIndex", "throwExceptionMethod", "/" + regionName);

    IntStream.rangeClosed(1, 3).forEach(i -> region.put(i, new Portfolio(i)));


    assertEquals("Uncorrupted index must have all the entries", 3,
        idIndex.getStatistics().getNumberOfValues());
    assertEquals("Corrupted index should not have indexed any entries", 0,
        exceptionIndex.getStatistics().getNumberOfValues());

    SelectResults results = (SelectResults) queryService.newQuery(queryString).execute();

    assertEquals("Query execution must be successful ", 1, results.size());
  }


  @Test
  public void indexCreationMustFailIfRegionEntriesAreNotCompatible() throws Exception {

    String queryString = "SELECT * FROM /REGION_NAME WHERE ID = 3";
    String regionName = "REGION_NAME";

    Cache cache = getCache();

    Region region =
        cache.createRegionFactory().setDataPolicy(DataPolicy.PARTITION).create(regionName);

    QueryService queryService = cache.getQueryService();

    IntStream.rangeClosed(1, 3).forEach(i -> region.put(i, new Portfolio(i)));

    Index idIndex = queryService.createIndex("idIndex", "ID", "/" + regionName);
    try {
      queryService.createIndex("exceptionIndex", "throwExceptionMethod", "/" + regionName);
      fail();
    } catch (Exception exception) {
      System.out.println("Exception expected!");
    }

    assertEquals("Uncorrupted index must have all the entries ", 3,
        idIndex.getStatistics().getNumberOfValues());

    SelectResults results = (SelectResults) queryService.newQuery(queryString).execute();

    assertEquals("Query execution must be successful ", 1, results.size());
  }

  class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;

    final ArrayList indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }

    public void reset() {
      this.isIndexesUsed = false;
      this.indexesUsed.clear();
    }
  }

  @Test
  public void putMustSucceedWhenTheRangeIndexIsCorrupted() throws Exception {
    String regionName = "portfolio";
    String INDEX_NAME = "key_index1";

    PartitionAttributesFactory partitionAttributes = new PartitionAttributesFactory();
    partitionAttributes.setTotalNumBuckets(1);

    Cache cache = getCache();
    Region region = cache.createRegionFactory().setDataPolicy(DataPolicy.PARTITION)
        .setPartitionAttributes(partitionAttributes.create()).create(regionName);

    Portfolio p = new Portfolio(1, 2);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    map1.put("AOL", 4);
    p.positions = map1;
    region.put(1, p);

    QueryService queryService = cache.getQueryService();
    Index keyIndex1 = queryService.createIndex(INDEX_NAME, "positions[*]", "/portfolio");

    Portfolio p2 = new Portfolio(3, 4);
    HashMap map2 = new HashMap();
    map2.put("APPL", 3);
    map2.put("AOL", "hello");
    p2.positions = map2;

    region.put(2, p2);

    assertEquals("Put must be successful", 2, region.size());


    assertEquals("Index must be invalid at this point ", false, keyIndex1.isValid());

    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);

    SelectResults results = (SelectResults) queryService
        .newQuery(
            "select * from /portfolio p where p.positions['AOL'] = 'hello' OR p.positions['IBM'] = 2")
        .execute();

    assertEquals("Correct results expected from the query execution ", 2, results.size());

    assertEquals("No index must be used while executing the query ", 0,
        observer.indexesUsed.size());

  }

  @Test
  public void rangeIndexCreationMustFailIfRegionEntriesAreNotCompatible() throws Exception {
    String regionName = "portfolio";
    String INDEX_NAME = "key_index1";

    PartitionAttributesFactory partitionAttributes = new PartitionAttributesFactory();
    partitionAttributes.setTotalNumBuckets(1);

    Cache cache = getCache();
    Region region = cache.createRegionFactory().setDataPolicy(DataPolicy.PARTITION)
        .setPartitionAttributes(partitionAttributes.create()).create(regionName);

    Portfolio p = new Portfolio(1, 2);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    map1.put("AOL", 4);
    p.positions = map1;
    region.put(1, p);

    Portfolio p2 = new Portfolio(3, 4);
    HashMap map2 = new HashMap();
    map2.put("APPL", 3);
    map2.put("AOL", "hello");
    p2.positions = map2;

    region.put(2, p2);

    assertEquals("Put must be successful", 2, region.size());

    QueryService queryService = cache.getQueryService();
    try {
      queryService.createIndex(INDEX_NAME, "positions[*]", "/portfolio");
      fail();
    } catch (Exception exception) {
      System.out.println("Expected Exception " + exception);
    }

    assertEquals("There should be no index present", null,
        queryService.getIndex(region, INDEX_NAME));

    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);

    SelectResults results = (SelectResults) queryService
        .newQuery(
            "select * from /portfolio p where p.positions['AOL'] = 'hello' OR p.positions['IBM'] = 2")
        .execute();

    assertEquals("Current results expected from the query execution ", 2, results.size());

    assertEquals("No index must be used while executing the query ", 0,
        observer.indexesUsed.size());

  }

  @Test
  public void rangeIndexCreationMustPassIfEntriesArePresentInDifferentBucketsAndQueriesMustUseThem()
      throws Exception {
    String regionName = "portfolio";
    String INDEX_NAME = "key_index1";


    Cache cache = getCache();
    Region region =
        cache.createRegionFactory().setDataPolicy(DataPolicy.PARTITION).create(regionName);

    Portfolio p = new Portfolio(1, 2);
    HashMap map1 = new HashMap();
    map1.put("SUN", 1);
    map1.put("IBM", 2);
    map1.put("AOL", 4);
    p.positions = map1;
    region.put(1, p);

    Portfolio p2 = new Portfolio(3, 4);
    HashMap map2 = new HashMap();
    map2.put("APPL", 3);
    map2.put("AOL", "hello");
    p2.positions = map2;

    region.put(2, p2);

    assertEquals("Put must be successful", 2, region.size());

    QueryService queryService = cache.getQueryService();

    try {
      Index keyIndex1 = queryService.createIndex(INDEX_NAME, "positions[*]", "/portfolio");
      assertEquals("Index must be valid", true, keyIndex1.isValid());
    } catch (Exception exception) {
      fail();
    }

    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);

    SelectResults results = (SelectResults) queryService
        .newQuery(
            "select * from /portfolio p where p.positions['AOL'] = 'hello' OR p.positions['IBM'] = 2")
        .execute();

    assertEquals("Current results expected from the query execution ", 2, results.size());

    assertEquals("Index must be used while executing the query ", 2, observer.indexesUsed.size());

  }


}
