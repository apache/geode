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
package com.gemstone.gemfire.cache.lucene.internal;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.test.fake.Fakes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneIndexImplJUnitTest {
  public static final String REGION = "region";
  public static final String INDEX = "index";
  public static final int MAX_WAIT = 30000;
  private Cache cache;
  LuceneIndex index;

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  
  @Before
  public void createLuceneIndex() {
    cache = Fakes.cache();
    index = new LuceneIndexForPartitionedRegion(INDEX, REGION, cache);
  }
  
  @Test
  public void waitUnitFlushedWithMissingAEQThrowsIllegalArgument() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    index.waitUntilFlushed(MAX_WAIT);
  }
  
  @Test
  public void waitUnitFlushedWaitsForFlush() throws Exception {
    final String expectedIndexName = LuceneServiceImpl.getUniqueIndexName(INDEX, REGION);
    final AsyncEventQueue queue = mock(AsyncEventQueue.class);
    when(cache.getAsyncEventQueue(eq(expectedIndexName))).thenReturn(queue);
    
    AtomicInteger callCount = new AtomicInteger();
    when(queue.size()).thenAnswer(invocation -> {
      if (callCount.get() == 0) {
        // when the waitUnitFlushed() called the 2nd time, queue.size() will return 0
        callCount.incrementAndGet();
        return 2;
      } else {
        // when the waitUnitFlushed() called the 2nd time, queue.size() will return 0
        return 0;
      }
    });
    index.waitUntilFlushed(MAX_WAIT);
    verify(cache).getAsyncEventQueue(eq(expectedIndexName));
  }

}
