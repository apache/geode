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
package org.apache.geode.cache.lucene.internal.distributed;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class TopEntriesFunctionCollectorJUnitTest {

  private EntryScore<String> r1_1;
  private EntryScore<String> r1_2;
  private EntryScore<String> r2_1;
  private EntryScore<String> r2_2;
  private TopEntriesCollector result1;
  private TopEntriesCollector result2;

  @Before
  public void initializeCommonObjects() {
    r1_1 = new EntryScore<>("3", .9f);
    r1_2 = new EntryScore<>("1", .8f);
    r2_1 = new EntryScore<>("2", 0.85f);
    r2_2 = new EntryScore<>("4", 0.1f);

    result1 = new TopEntriesCollector(null);
    result1.collect(r1_1);
    result1.collect(r1_2);

    result2 = new TopEntriesCollector(null);
    result2.collect(r2_1);
    result2.collect(r2_2);
  }

  @Test
  public void testGetResultsBlocksTillEnd() throws Exception {
    final TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector();
    TopEntries merged = collector.getResult();
    assertEquals(0, merged.size());
  }

  @Test
  public void testGetResultsTimedWait() throws Exception {
    final TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector();
    collector.addResult(null, result1);
    collector.addResult(null, result2);

    TopEntries merged = collector.getResult(1, TimeUnit.SECONDS);
    assertEquals(4, merged.size());
    LuceneTestUtilities.verifyResultOrder(merged.getHits(), r1_1, r2_1, r1_2, r2_2);
  }

  @Test
  public void mergeShardAndLimitResults() throws Exception {
    LuceneFunctionContext<TopEntriesCollector> context =
        new LuceneFunctionContext<>(null, null, null, 3);

    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector(context);
    collector.addResult(null, result1);
    collector.addResult(null, result2);
    collector.endResults();

    TopEntries merged = collector.getResult();
    Assert.assertNotNull(merged);
    assertEquals(3, merged.size());
    LuceneTestUtilities.verifyResultOrder(merged.getHits(), r1_1, r2_1, r1_2);
  }

  @Test
  public void mergeResultsDefaultCollectorManager() throws Exception {
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector();
    collector.addResult(null, result1);
    collector.addResult(null, result2);
    collector.endResults();

    TopEntries merged = collector.getResult();
    Assert.assertNotNull(merged);
    assertEquals(4, merged.size());
    LuceneTestUtilities.verifyResultOrder(merged.getHits(), r1_1, r2_1, r1_2, r2_2);
  }

  @Test
  public void getResultsTwice() throws Exception {
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector();
    collector.addResult(null, result1);
    collector.addResult(null, result2);
    collector.endResults();

    TopEntries merged = collector.getResult();
    Assert.assertNotNull(merged);
    assertEquals(4, merged.size());
    LuceneTestUtilities.verifyResultOrder(merged.getHits(), r1_1, r2_1, r1_2, r2_2);

    merged = collector.getResult();
    Assert.assertNotNull(merged);
    assertEquals(4, merged.size());
    LuceneTestUtilities.verifyResultOrder(merged.getHits(), r1_1, r2_1, r1_2, r2_2);
  }

  @Test
  public void mergeResultsCustomCollectorManager() throws Exception {
    TopEntries resultEntries = new TopEntries();
    TopEntriesCollector mockCollector = mock(TopEntriesCollector.class);
    Mockito.doReturn(resultEntries).when(mockCollector).getEntries();

    CollectorManager<TopEntriesCollector> mockManager = mock(CollectorManager.class);
    Mockito.doReturn(mockCollector).when(mockManager)
        .reduce(Mockito.argThat(argument -> {
          Collection<TopEntriesCollector> collectors = argument;
          return collectors.contains(result1) && collectors.contains(result2);
        }));

    LuceneFunctionContext<TopEntriesCollector> context =
        new LuceneFunctionContext<>(null, null, mockManager);
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector(context);
    collector.addResult(null, result1);
    collector.addResult(null, result2);
    collector.endResults();

    TopEntries merged = collector.getResult();
    assertEquals(resultEntries, merged);
  }

  @Test
  public void mergeAfterClearResults() throws Exception {
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector();
    collector.addResult(null, result1);
    collector.clearResults();
    collector.addResult(null, result2);
    collector.endResults();

    TopEntries merged = collector.getResult();
    Assert.assertNotNull(merged);
    assertEquals(2, merged.size());
    LuceneTestUtilities.verifyResultOrder(merged.getHits(), r2_1, r2_2);
  }

  @Test(expected = RuntimeException.class)
  public void testExceptionDuringMerge() throws Exception {
    TopEntriesCollectorManager mockManager = mock(TopEntriesCollectorManager.class);
    Mockito.doThrow(new RuntimeException()).when(mockManager).reduce(any(Collection.class));

    LuceneFunctionContext<TopEntriesCollector> context =
        new LuceneFunctionContext<>(null, null, mockManager);
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector(context);
    collector.endResults();
    collector.getResult();
  }

  @Test
  public void testCollectorName() {
    InternalCache mockCache = mock(InternalCache.class);
    Mockito.doReturn("server").when(mockCache).getName();

    TopEntriesFunctionCollector function = new TopEntriesFunctionCollector(null, mockCache);
    assertEquals("server", function.id());
  }
}
