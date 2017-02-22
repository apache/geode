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

import static org.apache.geode.cache.lucene.test.IndexRepositorySpy.doOnce;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;

import java.util.stream.IntStream;

import org.apache.geode.cache.lucene.internal.LuceneIndexFactorySpy;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage.BecomePrimaryBucketResponse;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.After;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.lucene.test.IndexRepositorySpy;
import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * This test class adds more basic tests of lucene functionality for partitioned regions. These
 * tests should work across all types of PRs and topologies.
 *
 */
@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class RebalanceDUnitTest extends LuceneQueriesAccessorBase {

  protected static int NUM_BUCKETS = 10;


  @After
  public void cleanupRebalanceCallback() {
    removeCallback(dataStore1);
    removeCallback(dataStore2);
  }

  protected Object[] getListOfClientServerTypes() {
    return new Object[] {
        new RegionTestableType[] {RegionTestableType.PARTITION_PROXY, RegionTestableType.PARTITION},
        new RegionTestableType[] {RegionTestableType.PARTITION_PROXY_WITH_OVERFLOW,
            RegionTestableType.PARTITION_OVERFLOW_TO_DISK}};
  }


  @Test
  @Parameters(method = "getListOfClientServerTypes")
  public void returnCorrectResultsWhenRebalanceHappensOnIndexUpdate(RegionTestableType clientType,
      RegionTestableType regionType) throws InterruptedException {
    addCallbackToTriggerRebalance(dataStore1);

    putEntriesAndValidateQueryResults(clientType, regionType);
  }

  @Test
  @Parameters(method = "getListOfClientServerTypes")
  public void returnCorrectResultsWhenMoveBucketHappensOnIndexUpdate(RegionTestableType clientType,
      RegionTestableType regionType) throws InterruptedException {
    final DistributedMember member2 =
        dataStore2.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    addCallbackToMoveBucket(dataStore1, member2);

    putEntriesAndValidateQueryResults(clientType, regionType);
  }

  @Test
  @Parameters(method = "getListOfClientServerTypes")
  public void returnCorrectResultsWhenMoveBucketHappensOnQuery(RegionTestableType clientType,
      RegionTestableType regionType) throws InterruptedException {
    final DistributedMember member2 =
        dataStore2.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    addCallbackToMovePrimaryOnQuery(dataStore1, member2);

    putEntriesAndValidateQueryResults(clientType, regionType);
  }


  @Test
  @Parameters(method = "getListOfClientServerTypes")
  public void returnCorrectResultsWhenBucketIsMovedAndMovedBackOnIndexUpdate(
      RegionTestableType clientType, RegionTestableType regionType) throws InterruptedException {
    final DistributedMember member1 =
        dataStore1.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    final DistributedMember member2 =
        dataStore2.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    addCallbackToMoveBucket(dataStore1, member2);
    addCallbackToMoveBucket(dataStore2, member1);

    putEntriesAndValidateQueryResults(clientType, regionType);
  }

  @Test
  @Parameters(method = "getListOfClientServerTypes")
  public void returnCorrectResultsWhenRebalanceHappensAfterUpdates(RegionTestableType clientType,
      RegionTestableType regionType) throws InterruptedException {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex, regionType));
    accessor.invoke(() -> initAccessor(createIndex, clientType));

    putEntryInEachBucket();

    dataStore2.invoke(() -> initDataStore(createIndex, regionType));
    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));
    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));

    rebalanceRegion(dataStore2);

    executeTextSearch(accessor, "world", "text", NUM_BUCKETS);
  }

  @Test
  @Parameters(method = "getListOfClientServerTypes")
  public void returnCorrectResultsWhenRebalanceHappensWhileSenderIsPaused(
      RegionTestableType clientType, RegionTestableType regionType) throws InterruptedException {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex, regionType));
    accessor.invoke(() -> initAccessor(createIndex, clientType));
    dataStore1.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));

    putEntryInEachBucket();

    dataStore2.invoke(() -> initDataStore(createIndex, regionType));
    rebalanceRegion(dataStore2);
    dataStore1.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));

    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));
    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));

    executeTextSearch(accessor, "world", "text", NUM_BUCKETS);
  }

  protected void putEntriesAndValidateQueryResults(RegionTestableType clientType,
      RegionTestableType regionType) {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex, regionType));
    accessor.invoke(() -> initAccessor(createIndex, clientType));
    dataStore1.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));

    putEntryInEachBucket();

    dataStore2.invoke(() -> initDataStore(createIndex, regionType));
    dataStore1.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));

    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));

    // dataStore3.invoke(() -> initDataStore(createIndex, regionType));
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
