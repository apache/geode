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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.test.TestObject;
import org.apache.geode.internal.cache.PartitionedRegion;

public class IndexRepositoryFactoryIntegrationTest {
  public Cache cache;
  public static final String INDEX_NAME = "testIndex";
  public static final String REGION_NAME = "testRegion";
  private IndexRepositoryFactory spyFactory;
  private LuceneQuery<Object, Object> luceneQuery;

  @Before
  public void setUp() {
    cache = new CacheFactory().create();
    LuceneServiceProvider.get(cache).createIndexFactory().setFields("field1", "field2")
        .create(INDEX_NAME, REGION_NAME);

    Region<Object, Object> dataRegion =
        cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);

    dataRegion.put("A", new TestObject());

    spyFactory = spy(new IndexRepositoryFactory());
    PartitionedRepositoryManager.indexRepositoryFactory = spyFactory;

    luceneQuery = LuceneServiceProvider.get(cache).createLuceneQueryFactory()
        .create(INDEX_NAME, REGION_NAME, "hello", "field1");
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void shouldRetryWhenIOExceptionEncounteredOnceDuringComputingRepository()
      throws IOException, LuceneQueryException {
    // To ensure that the specific bucket used in the query throws the IOException to trigger the
    // retry, throw once for every bucket in the region
    int timesToThrow = ((PartitionedRegion) cache.getRegion(REGION_NAME)).getTotalNumberOfBuckets();

    doAnswer(new Answer<Object>() {
      private int times = 0;

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (times < timesToThrow) {
          times++;
          throw new IOException();
        }
        return invocation.callRealMethod();
      }
    }).when(spyFactory).getIndexWriter(any(), any());

    luceneQuery.findKeys();
  }

  @Test
  public void shouldThrowInternalGemfireErrorWhenIOExceptionEncounteredConsistentlyDuringComputingRepository()
      throws IOException {
    doThrow(new IOException()).when(spyFactory).getIndexWriter(any(), any());

    assertThatThrownBy(luceneQuery::findKeys).isInstanceOf(FunctionException.class)
        .hasCauseInstanceOf(InternalGemFireError.class);
  }
}
