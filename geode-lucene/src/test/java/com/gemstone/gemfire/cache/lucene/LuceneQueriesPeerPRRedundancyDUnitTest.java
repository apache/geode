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
package com.gemstone.gemfire.cache.lucene;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.partitioned.BecomePrimaryBucketMessage;
import com.gemstone.gemfire.internal.cache.partitioned.BecomePrimaryBucketMessage.BecomePrimaryBucketResponse;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(DistributedTest.class)
public class LuceneQueriesPeerPRRedundancyDUnitTest extends LuceneQueriesPRBase {

  @Override protected void initDataStore(final SerializableRunnableIF createIndex) throws Exception {
    createIndex.run();
    getCache().createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).create(REGION_NAME);
  }

  @Override protected void initAccessor(final SerializableRunnableIF createIndex) throws Exception {
    initDataStore(createIndex);
  }

  @Test
  public void returnCorrectResultsWhenMovePrimaryHappensOnIndexUpdate() throws InterruptedException {
    final DistributedMember member2 = dataStore2.invoke(() -> getCache().getDistributedSystem().getDistributedMember());
    addCallbackToMovePrimary(dataStore1, member2);

    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    };
    dataStore1.invoke(() -> initDataStore(createIndex));
    dataStore2.invoke(() -> initDataStore(createIndex));
    accessor.invoke(() -> initAccessor(createIndex));
    dataStore1.invoke(() -> LuceneTestUtilities.pauseSender(getCache()));

    put113Entries();

    dataStore1.invoke(() -> LuceneTestUtilities.resumeSender(getCache()));

    assertTrue(waitForFlushBeforeExecuteTextSearch(dataStore1, 60000));

    executeTextSearch(accessor, "world", "text", 113);
  }

  protected void addCallbackToMovePrimary(VM vm, final DistributedMember destination) {
    vm.invoke(() -> {
      IndexRepositorySpy spy = IndexRepositorySpy.injectSpy();

      spy.beforeWrite(doOnce(key -> moveBucket(destination, key)));
    });
  }

  private void moveBucket(final DistributedMember destination, final Object key) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(REGION_NAME);

    BecomePrimaryBucketResponse response = BecomePrimaryBucketMessage.send(
      (InternalDistributedMember) destination, region, region.getKeyInfo(key).getBucketId(), true);
    assertNotNull(response);
    assertTrue(response.waitForResponse());
  }
}
