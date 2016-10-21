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

import static org.apache.geode.cache.lucene.test.IndexRepositorySpy.*;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.*;
import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.test.IndexRegionSpy;
import org.apache.geode.cache.lucene.test.IndexRepositorySpy;
import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage.BecomePrimaryBucketResponse;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

import com.jayway.awaitility.Awaitility;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class LuceneQueriesPeerPRRedundancyDUnitTest extends LuceneQueriesPRBase {

  @Override
  protected void initDataStore(final SerializableRunnableIF createIndex) throws Exception {
    createIndex.run();
    Region region = getCache().createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
        .setPartitionAttributes(getPartitionAttributes()).create(REGION_NAME);
  }

  @Override
  protected void initAccessor(final SerializableRunnableIF createIndex) throws Exception {
    createIndex.run();
    Region region = getCache().createRegionFactory(RegionShortcut.PARTITION_PROXY_REDUNDANT)
        .setPartitionAttributes(getPartitionAttributes()).create(REGION_NAME);
  }

  @Test
  public void returnCorrectResultsWhenMovePrimaryHappensOnIndexUpdate()
      throws InterruptedException {
    final DistributedMember member2 =
        dataStore2.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    addCallbackToMovePrimary(dataStore1, member2);

    putEntriesAndValidateResultsWithRedundancy();
  }

  @Category(FlakyTest.class) // GEODE-1956
  @Test
  public void returnCorrectResultsWhenCloseCacheHappensOnIndexUpdate() throws InterruptedException {
    dataStore1.invoke(() -> {
      IndexRepositorySpy spy = IndexRepositorySpy.injectSpy();

      spy.beforeWriteIndexRepository(doAfterN(key -> getCache().close(), 2));
    });

    putEntriesAndValidateResultsWithRedundancy();

    // Wait until the cache is closed in datastore1
    dataStore1.invoke(
        () -> Awaitility.await().atMost(60, TimeUnit.SECONDS).until(basicGetCache()::isClosed));
  }

  @Category(FlakyTest.class) // GEODE-1824
  @Test
  public void returnCorrectResultsWhenCloseCacheHappensOnPartialIndexWrite()
      throws InterruptedException {
    final DistributedMember member2 =
        dataStore2.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    dataStore1.invoke(() -> {
      IndexRegionSpy.beforeWrite(getCache(), doAfterN(key -> getCache().close(), 100));
    });

    putEntriesAndValidateResultsWithRedundancy();

    // Wait until the cache is closed in datastore1
    dataStore1.invoke(
        () -> Awaitility.await().atMost(60, TimeUnit.SECONDS).until(basicGetCache()::isClosed));
  }

  private void putEntriesAndValidateResultsWithRedundancy() {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex));
    dataStore2.invoke(() -> initDataStore(createIndex));
    accessor.invoke(() -> initAccessor(createIndex));
    dataStore1.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));
    dataStore2.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));
    accessor.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));

    putEntryInEachBucket();

    dataStore1.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));
    dataStore2.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));
    accessor.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));

    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore2, 60000));

    executeTextSearch(accessor, "world", "text", NUM_BUCKETS);
  }

  protected void addCallbackToMovePrimary(VM vm, final DistributedMember destination) {
    vm.invoke(() -> {
      IndexRepositorySpy spy = IndexRepositorySpy.injectSpy();

      spy.beforeWriteIndexRepository(doOnce(key -> moveBucket(destination, key)));
    });
  }

  private void moveBucket(final DistributedMember destination, final Object key) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(REGION_NAME);

    BecomePrimaryBucketResponse response =
        BecomePrimaryBucketMessage.send((InternalDistributedMember) destination, region,
            region.getKeyInfo(key).getBucketId(), true);
    assertNotNull(response);
    assertTrue(response.waitForResponse());
  }
}
