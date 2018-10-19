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
package org.apache.geode.cache.lucene;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.LuceneTest;


@Category({LuceneTest.class})
@RunWith(JUnitParamsRunner.class)
public class EvictionDUnitTest extends LuceneQueriesAccessorBase {

  protected static final float INITIAL_EVICTION_HEAP_PERCENTAGE = 50.9f;
  protected static final float EVICTION_HEAP_PERCENTAGE_FAKE_NOTIFICATION = 85.0f;
  protected static final int TEST_MAX_MEMORY = 100;
  protected static final int MEMORY_USED_FAKE_NOTIFICATION = 90;

  protected RegionTestableType[] getPartitionRedundantOverflowEvictionRegionType() {
    return new RegionTestableType[] {
        RegionTestableType.PARTITION_PERSISTENT_REDUNDANT_EVICTION_OVERFLOW};
  }

  protected RegionTestableType[] getPartitionRedundantLocalDestroyEvictionRegionType() {
    return new RegionTestableType[] {RegionTestableType.PARTITION_REDUNDANT_EVICTION_LOCAL_DESTROY,
        RegionTestableType.PARTITION_REDUNDANT_PERSISTENT_EVICTION_LOCAL_DESTROY,
        RegionTestableType.PARTITION_EVICTION_LOCAL_DESTROY,
        RegionTestableType.PARTITION_PERSISTENT_EVICTION_LOCAL_DESTROY};
  }

  @Test
  @Parameters(method = "getPartitionRedundantLocalDestroyEvictionRegionType")
  public void regionWithEvictionWithLocalDestroyMustNotbeAbleToCreateLuceneIndexes(
      RegionTestableType regionTestType) {
    SerializableRunnableIF createIndex = getSerializableRunnableIFCreateIndex();

    dataStore1.invoke(() -> {
      try {
        initDataStore(createIndex, regionTestType);
      } catch (UnsupportedOperationException e) {
        assertEquals(
            "Lucene indexes on regions with eviction and action local destroy are not supported",
            e.getMessage());
        assertNull(getCache().getRegion(REGION_NAME));
      }
    });

  }

  private SerializableRunnableIF getSerializableRunnableIFCreateIndex() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().setFields("text").create(INDEX_NAME, REGION_NAME);
    };
  }

  @Test
  @Parameters(method = "getPartitionRedundantOverflowEvictionRegionType")
  public void regionsWithEvictionWithOverflowMustBeAbleToCreateLuceneIndexes(
      RegionTestableType regionTestType) {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().setFields("text").create(INDEX_NAME, REGION_NAME);
    };

    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));

    accessor.invoke(() -> initDataStore(createIndex, regionTestType));

    accessor.invoke(() -> {
      Cache cache = getCache();
      Region region = cache.getRegion(REGION_NAME);
      IntStream.range(0, NUM_BUCKETS).forEach(i -> region.put(i, new TestObject("hello world")));
    });
    waitForFlushBeforeExecuteTextSearch(accessor, 60000);
    dataStore1.invoke(() -> {
      try {
        getCache().getResourceManager().setEvictionHeapPercentage(INITIAL_EVICTION_HEAP_PERCENTAGE);
        final PartitionedRegion partitionedRegion = (PartitionedRegion) getRootRegion(REGION_NAME);
        raiseFakeNotification();
        await().untilAsserted(() -> {
          assertTrue(partitionedRegion.getDiskRegionStats().getNumOverflowOnDisk() > 0);
        });
      } finally {
        cleanUpAfterFakeNotification();
      }
    });

    accessor.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      LuceneQuery<Integer, TestObject> query = luceneService.createLuceneQueryFactory()
          .setLimit(100).create(INDEX_NAME, REGION_NAME, "world", "text");
      List<LuceneResultStruct<Integer, TestObject>> resultList = query.findResults();
      assertEquals(NUM_BUCKETS, resultList.size());
    });

  }

  protected void raiseFakeNotification() {
    ((GemFireCacheImpl) getCache()).getHeapEvictor().setTestAbortAfterLoopCount(1);
    HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "memoryEventTolerance", "0");

    getCache().getResourceManager()
        .setEvictionHeapPercentage(EVICTION_HEAP_PERCENTAGE_FAKE_NOTIFICATION);
    HeapMemoryMonitor heapMemoryMonitor =
        ((GemFireCacheImpl) getCache()).getInternalResourceManager().getHeapMonitor();
    heapMemoryMonitor.setTestMaxMemoryBytes(TEST_MAX_MEMORY);

    heapMemoryMonitor.updateStateAndSendEvent(MEMORY_USED_FAKE_NOTIFICATION);
  }

  protected void cleanUpAfterFakeNotification() {
    ((GemFireCacheImpl) getCache()).getHeapEvictor().setTestAbortAfterLoopCount(Integer.MAX_VALUE);
    HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
    System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "memoryEventTolerance");
  }

}
