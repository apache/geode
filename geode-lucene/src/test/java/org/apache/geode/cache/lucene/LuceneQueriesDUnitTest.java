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

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.*;
import static org.junit.Assert.*;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;

import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * This test class is intended to contain basic integration tests of the lucene query class that
 * should be executed against a number of different regions types and topologies.
 *
 */
@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneQueriesDUnitTest extends LuceneQueriesAccessorBase {

  private static final long serialVersionUID = 1L;

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void returnCorrectResultsFromStringQueryWithDefaultAnalyzer(
      RegionTestableType regionTestType) {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().addField("text").create(INDEX_NAME, REGION_NAME);
    };
    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionTestType));
    accessor.invoke(() -> initAccessor(createIndex, regionTestType));

    putDataInRegion(accessor);
    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));
    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));
    executeTextSearch(accessor);
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void defaultFieldShouldPropogateCorrectlyThroughFunction(
      RegionTestableType regionTestType) {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().addField("text").create(INDEX_NAME, REGION_NAME);
    };
    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionTestType));
    accessor.invoke(() -> initAccessor(createIndex, regionTestType));
    putDataInRegion(accessor);
    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));
    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));
    executeTextSearch(accessor, "world", "text", 3);
    executeTextSearch(accessor, "world", "noEntriesMapped", 0);
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void canQueryWithCustomLuceneQueryObject(RegionTestableType regionTestType) {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().addField("text").create(INDEX_NAME, REGION_NAME);
    };
    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionTestType));
    accessor.invoke(() -> initAccessor(createIndex, regionTestType));
    putDataInRegion(accessor);
    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));
    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));

    // Execute a query with a custom lucene query object
    accessor.invoke(() -> {
      Cache cache = getCache();
      LuceneService service = LuceneServiceProvider.get(cache);
      LuceneQuery query =
          service.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, index -> {
            return new TermQuery(new Term("text", "world"));
          });
      final PageableLuceneQueryResults results = query.findPages();
      assertEquals(3, results.size());
    });
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void verifyWaitForFlushedFunctionOnAccessor(RegionTestableType regionTestType)
      throws InterruptedException {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().addField("text").create(INDEX_NAME, REGION_NAME);
    };
    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionTestType));
    accessor.invoke(() -> initAccessor(createIndex, regionTestType));
    dataStore1.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));
    dataStore2.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));
    putDataInRegion(accessor);
    assertFalse(waitForFlushBeforeExecuteTextSearch(accessor, 200));
    dataStore1.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));
    dataStore2.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));
    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));
    executeTextSearch(accessor, "world", "text", 3);
    executeTextSearch(accessor, "world", "noEntriesMapped", 0);
  }

  protected void putDataInRegion(VM vm) {
    vm.invoke(() -> {
      final Cache cache = getCache();
      Region<Object, Object> region = cache.getRegion(REGION_NAME);
      region.put(1, new TestObject("hello world"));
      region.put(113, new TestObject("hi world"));
      region.put(2, new TestObject("goodbye world"));
    });
  }

}
