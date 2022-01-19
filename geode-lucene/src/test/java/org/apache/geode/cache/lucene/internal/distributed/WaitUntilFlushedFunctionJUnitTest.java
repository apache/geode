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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class WaitUntilFlushedFunctionJUnitTest {

  String regionPath = SEPARATOR + "region";
  String indexName = "index";
  final EntryScore<String> r1_1 = new EntryScore<>("key-1-1", .5f);
  final EntryScore<String> r1_2 = new EntryScore<>("key-1-2", .4f);
  final EntryScore<String> r1_3 = new EntryScore<>("key-1-3", .3f);
  final EntryScore<String> r2_1 = new EntryScore<>("key-2-1", .45f);
  final EntryScore<String> r2_2 = new EntryScore<>("key-2-2", .35f);

  InternalRegionFunctionContext mockContext;
  ResultSender mockResultSender;
  Region<Object, Object> mockRegion;
  AsyncEventQueueImpl mockAEQ;
  InternalLuceneService mockService;
  LuceneIndexImpl mockIndex;
  WaitUntilFlushedFunctionContext waitArgs;
  private InternalCache mockCache;

  @Test
  public void testExecution() throws Exception {
    when(mockContext.getDataSet()).thenReturn(mockRegion);
    when(mockContext.getArguments()).thenReturn(waitArgs);
    when(mockContext.getResultSender()).thenReturn(mockResultSender);
    when(mockCache.getAsyncEventQueue(any())).thenReturn(mockAEQ);
    when(mockAEQ.waitUntilFlushed(10000, TimeUnit.MILLISECONDS)).thenReturn(true);

    WaitUntilFlushedFunction function = new WaitUntilFlushedFunction();
    function.execute(mockContext);

    ArgumentCaptor<Boolean> resultCaptor = ArgumentCaptor.forClass(Boolean.class);
    verify(mockResultSender).lastResult(resultCaptor.capture());
    Boolean result = resultCaptor.getValue();

    assertTrue(result);
  }


  @Test
  public void testQueryFunctionId() {
    String id = new WaitUntilFlushedFunction().getId();
    assertEquals(WaitUntilFlushedFunction.class.getName(), id);
  }

  @Before
  public void createMocksAndCommonObjects() throws Exception {
    mockContext = mock(InternalRegionFunctionContext.class);
    mockResultSender = mock(ResultSender.class);
    mockRegion = mock(Region.class);
    mockAEQ = mock(AsyncEventQueueImpl.class);

    mockIndex = mock(LuceneIndexImpl.class);
    mockService = mock(InternalLuceneService.class);
    mockCache = mock(InternalCache.class);

    waitArgs = new WaitUntilFlushedFunctionContext(indexName, 10000, TimeUnit.MILLISECONDS);

    when(mockRegion.getCache()).thenReturn(mockCache);
    when(mockRegion.getFullPath()).thenReturn(regionPath);
    when(mockCache.getService(any())).thenReturn(mockService);
    when(mockService.getIndex(eq("index"), eq(regionPath))).thenReturn(mockIndex);
  }
}
