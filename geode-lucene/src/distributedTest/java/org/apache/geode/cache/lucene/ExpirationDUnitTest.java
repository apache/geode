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
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
@RunWith(JUnitParamsRunner.class)
public class ExpirationDUnitTest extends LuceneQueriesAccessorBase {

  protected static final int EXTRA_WAIT_TIME_SEC = 15;

  protected RegionTestableType[] getPartitionRegionsWithExpirationSet() {
    return new RegionTestableType[] {RegionTestableType.PARTITION_WITH_EXPIRATION_DESTROY,
        RegionTestableType.PARTITION_REDUNDANT_WITH_EXPIRATION_DESTROY,
        RegionTestableType.PARTITION_REDUNDANT_PERSISTENT_WITH_EXPIRATION_DESTROY};
  }

  @Test
  @Parameters(method = "getPartitionRegionsWithExpirationSet")
  public void regionWithExpirationSetMustAlsoRemoveLuceneIndexEntries(
      RegionTestableType regionTestType) {
    SerializableRunnableIF createIndex = () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndexFactory().setFields("text").create(INDEX_NAME, REGION_NAME);
    };

    dataStore1.invoke(() -> initDataStore(createIndex, regionTestType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionTestType));
    accessor.invoke(() -> initDataStore(createIndex, regionTestType));

    accessor.invoke(() -> {
      Cache cache = getCache();
      Region region = cache.getRegion(REGION_NAME);
      IntStream.range(0, NUM_BUCKETS).forEach(i -> region.put(i, new TestObject("hello world")));
    });

    assertTrue(waitForFlushBeforeExecuteTextSearch(accessor, 60000));

    accessor.invoke(() -> await()
        .untilAsserted(() -> {
          LuceneService luceneService = LuceneServiceProvider.get(getCache());
          LuceneQuery<Integer, TestObject> luceneQuery = luceneService.createLuceneQueryFactory()
              .setLimit(100).create(INDEX_NAME, REGION_NAME, "world", "text");

          Collection luceneResultList = luceneQuery.findKeys();
          assertEquals(0, luceneResultList.size());
        }));
  }

}
