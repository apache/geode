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

import static org.apache.geode.cache.lucene.LuceneDUnitTest.RegionTestableType.PARTITION_WITH_CLIENT;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.lucene.internal.LuceneIndexFactoryImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * This test class is intended to contain basic integration tests of the lucene query class that
 * should be executed against a number of different regions types and topologies.
 *
 */
@Category({LuceneTest.class})
@RunWith(JUnitParamsRunner.class)
public class LuceneQueriesReindexDUnitTest extends LuceneQueriesAccessorBase {

  private static final long serialVersionUID = 1L;

  private void destroyIndex() {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    luceneService.destroyIndex(INDEX_NAME, REGION_NAME);
  }

  private void createIndex(String fieldName) {
    createIndex(INDEX_NAME, fieldName);
  };

  private void createIndex(String indexName, String fieldName) {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    LuceneIndexFactoryImpl indexFactory =
        (LuceneIndexFactoryImpl) luceneService.createIndexFactory().addField(fieldName);
    indexFactory.create(indexName, REGION_NAME, true);
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
      createIndex("text");
    });

    AsyncInvocation ai2 = dataStore2.invokeAsync(() -> {
      createIndex("text");
    });

    ai1.join();
    ai2.join();

    ai1.checkException();
    ai2.checkException();

    waitForFlushBeforeExecuteTextSearch(accessor, 60000);
    executeTextSearch(accessor);
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void afterReindexingWeShouldBeAbleToGetTheStatusOfIndexingProgress(
      RegionTestableType regionTestType) throws Exception {
    dataStore1.invoke(() -> {
      regionTestType.createDataStore(getCache(), REGION_NAME);
    });
    dataStore2.invoke(() -> {
      regionTestType.createDataStore(getCache(), REGION_NAME);
    });
    accessor.invoke(() -> {
      regionTestType.createAccessor(getCache(), REGION_NAME);
    });

    putDataInRegion(accessor);

    // re-index stored data
    AsyncInvocation ai1 = dataStore1.invokeAsync(() -> {
      createIndex("text");
    });

    AsyncInvocation ai2 = dataStore2.invokeAsync(() -> {
      createIndex("text");
    });

    AsyncInvocation ai3 = accessor.invokeAsync(() -> {
      if (regionTestType != PARTITION_WITH_CLIENT) {
        createIndex("text");
      }
    });

    ai1.join();
    ai2.join();
    ai3.join();

    ai1.checkException();
    ai2.checkException();
    ai3.checkException();

    waitForFlushBeforeExecuteTextSearch(accessor, 60000);

    accessor.invoke(
        () -> await().untilAsserted(() -> assertFalse(
            LuceneServiceProvider.get(getCache()).isIndexingInProgress(INDEX_NAME, REGION_NAME))));
    executeTextSearch(accessor);
  }



  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void dropAndRecreateIndexWithDifferentFieldsShouldFail(RegionTestableType regionTestType)
      throws Exception {
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
    verifyCreateIndexWithDifferentFieldShouldFail();
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void recreateIndexWithDifferentFieldsShouldFail(RegionTestableType regionTestType)
      throws Exception {

    dataStore1.invoke(() -> {
      regionTestType.createDataStore(getCache(), REGION_NAME);
    });
    dataStore2.invoke(() -> {
      regionTestType.createDataStore(getCache(), REGION_NAME);
    });
    accessor.invoke(() -> {
      regionTestType.createAccessor(getCache(), REGION_NAME);
    });

    putDataInRegion(accessor);

    verifyCreateIndexWithDifferentFieldShouldFail();
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void recreateDifferentIndexesWithDifferentFieldsShouldPass(
      RegionTestableType regionTestType) throws Exception {

    dataStore1.invoke(() -> {
      regionTestType.createDataStore(getCache(), REGION_NAME);
    });
    dataStore2.invoke(() -> {
      regionTestType.createDataStore(getCache(), REGION_NAME);
    });
    accessor.invoke(() -> {
      regionTestType.createAccessor(getCache(), REGION_NAME);
    });

    putDataInRegion(accessor);

    // re-index stored data
    AsyncInvocation ai1 = dataStore1.invokeAsync(() -> {
      createIndex("text");
    });

    AsyncInvocation ai2 = dataStore2.invokeAsync(() -> {
      createIndex(INDEX_NAME + "2", "text2");
    });

    AsyncInvocation ai3 = dataStore1.invokeAsync(() -> {
      createIndex(INDEX_NAME + "2", "text2");
    });

    AsyncInvocation ai4 = dataStore2.invokeAsync(() -> {
      createIndex("text");
    });

    AsyncInvocation ai5 = accessor.invokeAsync(() -> {
      if (getCache().getRegion(REGION_NAME) instanceof PartitionedRegion) {
        createIndex(INDEX_NAME + "1", "text");
      }
    });

    AsyncInvocation ai6 = accessor.invokeAsync(() -> {
      if (getCache().getRegion(REGION_NAME) instanceof PartitionedRegion) {
        createIndex(INDEX_NAME + "2", "text2");
      }
    });

    ai1.join();
    ai2.join();
    ai3.join();
    ai4.join();
    ai5.join();
    ai6.join();

    ai1.checkException();
    ai2.checkException();
    ai3.checkException();
    ai4.checkException();
    ai5.checkException();
    ai6.checkException();

    waitForFlushBeforeExecuteTextSearch(accessor, 60000);
    executeTextSearch(accessor);
  }

  private void verifyCreateIndexWithDifferentFieldShouldFail() throws Exception {
    AsyncInvocation ai1 = dataStore1.invokeAsync(() -> {
      createIndex("text");
    });

    AsyncInvocation ai2 = dataStore2.invokeAsync(() -> {
      createIndex("text2");
    });

    // wait for at most 10 seconds first for threads to finish
    ai1.join(10000);
    ai2.join(10000);

    // if one thread is still alive after 10 seconds, assert that they other thread throws
    // an exception and then try to create the same index on the
    // other datastore to unblock the thread and wait for the thread to finish
    if (ai1.isAlive()) {
      assertThat(ai2.getException() instanceof UnsupportedOperationException).isTrue();
      dataStore2.invoke(() -> {
        createIndex("text");
      });
      ai1.await();
    } else if (ai2.isAlive()) {
      assertThat(ai1.getException() instanceof UnsupportedOperationException).isTrue();
      dataStore1.invoke(() -> {
        createIndex("text2");
      });
      ai2.await();
    }
    // if both threads finished already, assert that both threads throw exception
    else {
      assertThat(ai1.getException() instanceof UnsupportedOperationException).isTrue();
      assertThat(ai2.getException() instanceof UnsupportedOperationException).isTrue();
    }
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
      createIndex("text");
    });

    // re-index stored data
    AsyncInvocation ai2 = dataStore2.invokeAsync(() -> {
      createIndex("text");
    });

    ai1.join();
    ai2.join();

    ai1.checkException();
    ai2.checkException();

    waitForFlushBeforeExecuteTextSearch(accessor, 60000);
    executeTextSearch(accessor);
  }

}
