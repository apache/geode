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
import static org.junit.Assert.assertTrue;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.lucene.internal.LuceneIndexFactoryImpl;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * This test class is intended to contain basic integration tests of the lucene query class that
 * should be executed against a number of different regions types and topologies.
 *
 */
@Category({DistributedTest.class, LuceneTest.class})
@RunWith(JUnitParamsRunner.class)
public class LuceneQueriesReindexDUnitTest extends LuceneQueriesAccessorBase {

  private static final long serialVersionUID = 1L;

  private void destroyIndex() {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    luceneService.destroyIndex(INDEX_NAME, REGION_NAME);
  }

  private void recreateIndex() {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    LuceneIndexFactoryImpl indexFactory =
        (LuceneIndexFactoryImpl) luceneService.createIndexFactory().addField("text");
    indexFactory.create(INDEX_NAME, REGION_NAME, true);
  };

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void dropAndRecreateIndex(RegionTestableType regionTestType) throws Exception {
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

    dataStore1.invoke(() -> destroyIndex());

    // re-index stored data
    AsyncInvocation ai1 = dataStore1.invokeAsync(() -> {
      recreateIndex();
    });

    AsyncInvocation ai2 = dataStore2.invokeAsync(() -> {
      recreateIndex();
    });

    ai1.join();
    ai2.join();

    ai1.checkException();
    ai2.checkException();

    executeTextSearch(accessor);
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void reindexThenQuery(RegionTestableType regionTestType) throws Exception {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      LuceneIndexFactoryImpl indexFactory =
          (LuceneIndexFactoryImpl) luceneService.createIndexFactory().addField("text");
      indexFactory.create(INDEX_NAME, REGION_NAME, true);
    };

    // Create dataRegion prior to index
    dataStore1.invoke(() -> initDataStore(regionTestType));
    dataStore2.invoke(() -> initDataStore(regionTestType));
    accessor.invoke(() -> initAccessor(regionTestType));

    // populate region
    putDataInRegion(accessor);

    // re-index stored data
    AsyncInvocation ai1 = dataStore1.invokeAsync(() -> {
      recreateIndex();
    });

    // re-index stored data
    AsyncInvocation ai2 = dataStore2.invokeAsync(() -> {
      recreateIndex();
    });

    ai1.join();
    ai2.join();

    ai1.checkException();
    ai2.checkException();

    executeTextSearch(accessor);
  }

}
