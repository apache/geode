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

import java.util.stream.IntStream;

import junitparams.Parameters;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * This test class adds more basic tests of lucene functionality for partitioned regions. These
 * tests should work across all types of PRs and topologies.
 *
 */
@Category({LuceneTest.class})
@RunWith(GeodeParamsRunner.class)
public class RebalanceDUnitTest extends LuceneQueriesAccessorBase {

  protected static int NUM_BUCKETS = 10;


  @After
  public void cleanupRebalanceCallback() {
    removeCallback(dataStore1);
    removeCallback(dataStore2);
  }

  @Override
  protected RegionTestableType[] getListOfRegionTestTypes() {
    return new RegionTestableType[] {RegionTestableType.PARTITION,
        RegionTestableType.PARTITION_OVERFLOW_TO_DISK};
  }


  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void returnCorrectResultsWhenRebalanceHappensOnIndexUpdate(
      RegionTestableType regionTestType) throws InterruptedException {
    addCallbackToTriggerRebalance(dataStore1);

    putEntriesAndValidateQueryResults(regionTestType);
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void returnCorrectResultsWhenMoveBucketHappensOnIndexUpdate(
      RegionTestableType regionTestType) throws InterruptedException {
    final DistributedMember member2 =
        dataStore2.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    addCallbackToMoveBucket(dataStore1, member2);

    putEntriesAndValidateQueryResults(regionTestType);
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void returnCorrectResultsWhenMoveBucketHappensOnQuery(RegionTestableType regionTestType)
      throws InterruptedException {
    final DistributedMember member2 =
        dataStore2.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    addCallbackToMovePrimaryOnQuery(dataStore1, member2);

    putEntriesAndValidateQueryResults(regionTestType);
  }


  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void returnCorrectResultsWhenBucketIsMovedAndMovedBackOnIndexUpdate(
      RegionTestableType regionTestType) throws InterruptedException {
    final DistributedMember member1 =
        dataStore1.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    final DistributedMember member2 =
        dataStore2.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    addCallbackToMoveBucket(dataStore1, member2);
    addCallbackToMoveBucket(dataStore2, member1);

    putEntriesAndValidateQueryResults(regionTestType);
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void returnCorrectResultsWhenRebalanceHappensAfterUpdates(
      RegionTestableType regionTestType) throws InterruptedException {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().setFields("text").create(INDEX_NAME, REGION_NAME);
    };
    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    accessor.invoke(() -> initAccessor(createIndex, regionTestType));

    putEntryInEachBucket();

    dataStore2.invoke(() -> initDataStore(createIndex, regionTestType));
    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));
    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));

    rebalanceRegion(dataStore2);

    executeTextSearch(accessor, "world", "text", NUM_BUCKETS);
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void returnCorrectResultsWhenRebalanceHappensWhileSenderIsPaused(
      RegionTestableType regionTestType) throws InterruptedException {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().setFields("text").create(INDEX_NAME, REGION_NAME);
    };
    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    accessor.invoke(() -> initAccessor(createIndex, regionTestType));
    dataStore1.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));

    putEntryInEachBucket();

    dataStore2.invoke(() -> initDataStore(createIndex, regionTestType));
    rebalanceRegion(dataStore2);
    dataStore1.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));

    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));
    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));

    executeTextSearch(accessor, "world", "text", NUM_BUCKETS);
  }

  protected void putEntriesAndValidateQueryResults(RegionTestableType regionTestType) {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().setFields("text").create(INDEX_NAME, REGION_NAME);
    };
    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    accessor.invoke(() -> initAccessor(createIndex, regionTestType));
    dataStore1.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));

    putEntryInEachBucket();

    dataStore2.invoke(() -> initDataStore(createIndex, regionTestType));
    dataStore1.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));

    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));

    // dataStore3.invoke(() -> initDataStore(create, regionType));
    executeTextSearch(accessor, "world", "text", NUM_BUCKETS);
  }

  // Duplicated for now, try to abstract this out...
  protected void putEntryInEachBucket() {
    accessor.invoke(() -> {
      final Cache cache = getCache();
      Region<Object, Object> region = cache.getRegion(REGION_NAME);
      IntStream.range(0, NUM_BUCKETS).forEach(i -> region.put(i, new TestObject("hello world")));
    });
  }

}
