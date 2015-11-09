/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.distributed;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TopEntriesFunctionCollectorJUnitTest {
  EntryScore r1_1, r1_2, r2_1, r2_2;
  TopEntriesCollector result1, result2;

  @Before
  public void initializeCommonObjects() {
    r1_1 = new EntryScore("3", .9f);
    r1_2 = new EntryScore("1", .8f);
    r2_1 = new EntryScore("2", 0.85f);
    r2_2 = new EntryScore("4", 0.1f);

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
    final CountDownLatch insideThread = new CountDownLatch(1);
    final CountDownLatch resultReceived = new CountDownLatch(1);
    Thread resultClient = new Thread(new Runnable() {
      @Override
      public void run() {
        insideThread.countDown();
        collector.getResult();
        resultReceived.countDown();
      }
    });
    resultClient.start();

    insideThread.await(1, TimeUnit.SECONDS);
    assertEquals(0, insideThread.getCount());
    assertEquals(1, resultReceived.getCount());

    collector.endResults();
    resultReceived.await(1, TimeUnit.SECONDS);
    assertEquals(0, resultReceived.getCount());
  }

  @Test
  public void testGetResultsTimedWait() throws Exception {
    final TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector();
    collector.addResult(null, result1);
    collector.addResult(null, result2);

    final CountDownLatch insideThread = new CountDownLatch(1);
    final CountDownLatch resultReceived = new CountDownLatch(1);

    final AtomicReference<TopEntries> result = new AtomicReference<>();

    Thread resultClient = new Thread(new Runnable() {
      @Override
      public void run() {
        insideThread.countDown();
        result.set(collector.getResult(1, TimeUnit.SECONDS));
        resultReceived.countDown();
      }
    });
    resultClient.start();

    insideThread.await(1, TimeUnit.SECONDS);
    assertEquals(0, insideThread.getCount());
    assertEquals(1, resultReceived.getCount());

    collector.endResults();

    resultReceived.await(1, TimeUnit.SECONDS);
    assertEquals(0, resultReceived.getCount());

    TopEntries merged = result.get();
    assertEquals(4, merged.size());
    TopEntriesJUnitTest.verifyResultOrder(merged.getHits(), r1_1, r2_1, r1_2, r2_2);
  }

  @Test(expected = FunctionException.class)
  public void testGetResultsWaitInterrupted() throws Exception {
    interruptWhileWaiting(false);
  }

  @Test(expected = FunctionException.class)
  public void testGetResultsTimedWaitInterrupted() throws Exception {
    interruptWhileWaiting(false);
  }

  private void interruptWhileWaiting(final boolean timedWait) throws InterruptedException, Exception {
    GemFireCacheImpl mockCache = mock(GemFireCacheImpl.class);
    final TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector(null, mockCache);

    final CountDownLatch insideThread = new CountDownLatch(1);
    final CountDownLatch endGetResult = new CountDownLatch(1);
    final AtomicReference<Exception> exception = new AtomicReference<>();

    Thread resultClient = new Thread(new Runnable() {
      @Override
      public void run() {
        insideThread.countDown();
        try {
          if (timedWait) {
            collector.getResult(1, TimeUnit.SECONDS);
          } else {
            collector.getResult();
          }
        } catch (FunctionException e) {
          exception.set(e);
          endGetResult.countDown();
        }
      }
    });
    resultClient.start();

    insideThread.await(1, TimeUnit.SECONDS);
    assertEquals(0, insideThread.getCount());
    assertEquals(1, endGetResult.getCount());

    CancelCriterion mockCriterion = mock(CancelCriterion.class);
    when(mockCache.getCancelCriterion()).thenReturn(mockCriterion);
    resultClient.interrupt();
    endGetResult.await(1, TimeUnit.SECONDS);
    assertEquals(0, endGetResult.getCount());
    throw exception.get();
  }

  @Test(expected = FunctionException.class)
  public void expectErrorAfterWaitTime() throws Exception {
    final TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector(null);

    final CountDownLatch insideThread = new CountDownLatch(1);
    final CountDownLatch endGetResult = new CountDownLatch(1);
    final AtomicReference<Exception> exception = new AtomicReference<>();

    Thread resultClient = new Thread(new Runnable() {
      @Override
      public void run() {
        insideThread.countDown();
        try {
          collector.getResult(10, TimeUnit.MILLISECONDS);
        } catch (FunctionException e) {
          exception.set(e);
          endGetResult.countDown();
        }
      }
    });
    resultClient.start();

    insideThread.await(1, TimeUnit.SECONDS);
    assertEquals(0, insideThread.getCount());
    assertEquals(1, endGetResult.getCount());

    endGetResult.await(1, TimeUnit.SECONDS);
    assertEquals(0, endGetResult.getCount());
    throw exception.get();
  }

  @Test
  public void mergeShardAndLimitResults() throws Exception {
    LuceneFunctionContext<TopEntriesCollector> context = new LuceneFunctionContext<>(null, null, null, 3);
    
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector(context);
    collector.addResult(null, result1);
    collector.addResult(null, result2);
    collector.endResults();

    TopEntries merged = collector.getResult();
    Assert.assertNotNull(merged);
    assertEquals(3, merged.size());
    TopEntriesJUnitTest.verifyResultOrder(merged.getHits(), r1_1, r2_1, r1_2);
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
    TopEntriesJUnitTest.verifyResultOrder(merged.getHits(), r1_1, r2_1, r1_2, r2_2);
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
    TopEntriesJUnitTest.verifyResultOrder(merged.getHits(), r1_1, r2_1, r1_2, r2_2);
    
    merged = collector.getResult();
    Assert.assertNotNull(merged);
    assertEquals(4, merged.size());
    TopEntriesJUnitTest.verifyResultOrder(merged.getHits(), r1_1, r2_1, r1_2, r2_2);
  }
  
  @Test
  public void mergeResultsCustomCollectorManager() throws Exception {
    TopEntries resultEntries = new TopEntries();
    TopEntriesCollector mockCollector = mock(TopEntriesCollector.class);
    Mockito.doReturn(resultEntries).when(mockCollector).getEntries();

    CollectorManager<TopEntriesCollector> mockManager = mock(CollectorManager.class);
    Mockito.doReturn(mockCollector).when(mockManager)
        .reduce(Mockito.argThat(new ArgumentMatcher<Collection<TopEntriesCollector>>() {
          @Override
          public boolean matches(Object argument) {
            Collection<TopEntriesCollector> collectors = (Collection<TopEntriesCollector>) argument;
            return collectors.contains(result1) && collectors.contains(result2);
          }
        }));

    LuceneFunctionContext<TopEntriesCollector> context = new LuceneFunctionContext<>(null, null, mockManager);
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
    TopEntriesJUnitTest.verifyResultOrder(merged.getHits(), r2_1, r2_2);
  }

  @Test(expected = FunctionException.class)
  public void testExceptionDuringMerge() throws Exception {
    TopEntriesCollectorManager mockManager = mock(TopEntriesCollectorManager.class);
    Mockito.doThrow(new IOException()).when(mockManager).reduce(any(Collection.class));

    LuceneFunctionContext<TopEntriesCollector> context = new LuceneFunctionContext<>(null, null, mockManager);
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector(context);
    collector.endResults();
    collector.getResult();
  }

  @Test(expected = IllegalStateException.class)
  public void addResultDisallowedAfterEndResult() {
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector();
    collector.endResults();
    collector.addResult(null, new TopEntriesCollector(null));
  }

  @Test(expected = IllegalStateException.class)
  public void clearResultDisallowedAfterEndResult() {
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector();
    collector.endResults();
    collector.clearResults();
  }

  @Test
  public void testCollectorName() {
    GemFireCacheImpl mockCache = mock(GemFireCacheImpl.class);
    Mockito.doReturn("server").when(mockCache).getName();

    TopEntriesFunctionCollector function = new TopEntriesFunctionCollector(null, mockCache);
    assertEquals("server", function.id);
  }
}
