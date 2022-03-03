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
package org.apache.geode.cache.lucene.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.internal.cache.InternalCache;

public class IndexRepositoryFactoryIntegrationTest {
  private Cache cache;
  public static final String INDEX_NAME = "testIndex";
  public static final String REGION_NAME = "testRegion";
  public static final int NUMBER_OF_BUCKETS = 4;
  private IndexRepositoryFactory spyFactory;
  private LuceneQuery<Object, Object> luceneQuery;

  @Before
  public void setUp() {
    cache = new CacheFactory().create();
    String fieldName = "field1";
    LuceneServiceProvider.get(cache)
        .createIndexFactory()
        .setFields(fieldName)
        .create(INDEX_NAME, REGION_NAME);

    cache.createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(new PartitionAttributesFactory<>()
            .setTotalNumBuckets(NUMBER_OF_BUCKETS)
            .create())
        .create(REGION_NAME);

    spyFactory = spy(new IndexRepositoryFactory());
    PartitionedRepositoryManager.indexRepositoryFactory = spyFactory;

    luceneQuery = LuceneServiceProvider.get(cache)
        .createLuceneQueryFactory()
        .create(INDEX_NAME, REGION_NAME, "hello", fieldName);
  }

  @After
  public void tearDown() {
    ExecutorService lonerDistributionThreads =
        ((InternalCache) cache).getDistributionManager().getExecutors().getThreadPool();
    PartitionedRepositoryManager.indexRepositoryFactory = new IndexRepositoryFactory();
    if (cache != null) {
      cache.close();
    }
    // Wait until the thread pool that uses the modified IndexRepositoryFactory behaviour has
    // terminated before allowing further tests, to prevent mocking exceptions
    await().until(lonerDistributionThreads::isTerminated);
  }

  @Test
  public void shouldRetryWhenIOExceptionEncounteredOnceDuringComputingRepository()
      throws IOException, LuceneQueryException {
    // To ensure that the specific bucket used in the query throws the IOException to trigger the
    // retry, throw once for every bucket in the region

    doAnswer(new Answer<Object>() {
      private final AtomicInteger times = new AtomicInteger(0);

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (times.getAndIncrement() < NUMBER_OF_BUCKETS) {
          throw new IOException();
        }
        return invocation.callRealMethod();
      }
    }).when(spyFactory).getIndexWriter(any(), any());

    luceneQuery.findKeys();

    // The invocation should throw once for each bucket, then retry once for each bucket
    verify(spyFactory, times(NUMBER_OF_BUCKETS * 2)).getIndexWriter(any(), any());
  }

  @Test
  public void shouldThrowInternalGemfireErrorWhenIOExceptionEncounteredConsistentlyDuringComputingRepository()
      throws IOException {
    doThrow(new IOException()).when(spyFactory).getIndexWriter(any(), any());

    assertThatThrownBy(luceneQuery::findKeys).isInstanceOf(FunctionException.class)
        .hasCauseInstanceOf(InternalGemFireError.class);
  }
}
