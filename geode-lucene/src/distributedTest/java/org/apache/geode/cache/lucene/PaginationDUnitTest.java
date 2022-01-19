/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({LuceneTest.class})
@RunWith(GeodeParamsRunner.class)
public class PaginationDUnitTest extends LuceneQueriesAccessorBase {
  protected static final int PAGE_SIZE = 2;
  protected static final int FLUSH_WAIT_TIME_MS = 60000;

  @Override
  protected RegionTestableType[] getListOfRegionTestTypes() {
    return new RegionTestableType[] {RegionTestableType.PARTITION_REDUNDANT_PERSISTENT};
  }

  protected void putEntryInEachBucket() {
    accessor.invoke(() -> {
      final Cache cache = getCache();
      Region<Object, Object> region = cache.getRegion(REGION_NAME);
      IntStream.range(0, NUM_BUCKETS).forEach(i -> region.put(i, new TestObject("hello world")));
    });
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void partitionedRegionStorageExceptionWhenAllDataStoreAreClosedWhilePagination(
      RegionTestableType regionTestType) {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().setFields("text").create(INDEX_NAME, REGION_NAME);
    };


    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    accessor.invoke(() -> initAccessor(createIndex, regionTestType));

    putEntryInEachBucket();

    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, FLUSH_WAIT_TIME_MS));

    accessor.invoke(() -> {
      Cache cache = getCache();
      LuceneService service = LuceneServiceProvider.get(cache);
      LuceneQuery<Integer, TestObject> query;
      query = service.createLuceneQueryFactory().setLimit(1000).setPageSize(PAGE_SIZE)
          .create(INDEX_NAME, REGION_NAME, "world", "text");
      PageableLuceneQueryResults<Integer, TestObject> pages = query.findPages();
      assertTrue(pages.hasNext());
      List<LuceneResultStruct<Integer, TestObject>> page = pages.next();
      assertEquals(page.size(), PAGE_SIZE, page.size());
      dataStore1.invoke(JUnit4CacheTestCase::closeCache);
      try {
        page = pages.next();
        fail();
      } catch (Exception e) {
        Assert.assertEquals(
            "Expected Exception = PartitionedRegionStorageException but hit " + e, true,
            e instanceof PartitionedRegionStorageException);
      }
    });
  }


  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void noExceptionWhenOneDataStoreIsClosedButOneIsStillUpWhilePagination(
      RegionTestableType regionTestType) {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().setFields("text").create(INDEX_NAME, REGION_NAME);
    };


    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionTestType));
    accessor.invoke(() -> initAccessor(createIndex, regionTestType));

    putEntryInEachBucket();

    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, FLUSH_WAIT_TIME_MS));

    accessor.invoke(() -> {
      List<LuceneResultStruct<Integer, TestObject>> combinedResult = new ArrayList<>();
      Cache cache = getCache();
      LuceneService service = LuceneServiceProvider.get(cache);
      LuceneQuery<Integer, TestObject> query;
      query = service.createLuceneQueryFactory().setLimit(1000).setPageSize(PAGE_SIZE)
          .create(INDEX_NAME, REGION_NAME, "world", "text");
      PageableLuceneQueryResults<Integer, TestObject> pages = query.findPages();
      assertTrue(pages.hasNext());
      List<LuceneResultStruct<Integer, TestObject>> page = pages.next();
      combinedResult.addAll(page);
      assertEquals(PAGE_SIZE, page.size());
      dataStore1.invoke(JUnit4CacheTestCase::closeCache);
      for (int i = 0; i < ((NUM_BUCKETS / PAGE_SIZE) - 1); i++) {
        page = pages.next();
        assertEquals(PAGE_SIZE, page.size());
        combinedResult.addAll(page);
      }
      validateTheCombinedResult(combinedResult);
    });
  }

  private void validateTheCombinedResult(
      final List<LuceneResultStruct<Integer, TestObject>> combinedResult) {
    Map<Integer, TestObject> resultMap = combinedResult.stream()
        .collect(Collectors.toMap(LuceneResultStruct::getKey, LuceneResultStruct::getValue));
    assertEquals(NUM_BUCKETS, resultMap.size());

    for (int i = 0; i < NUM_BUCKETS; i++) {
      assertEquals("The aggregate result does not contain the key = " + i, true,
          resultMap.containsKey(i));
      assertEquals(new TestObject("hello world"), resultMap.get(i));
    }
  }

}
