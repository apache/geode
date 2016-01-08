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

package com.gemstone.gemfire.cache.lucene.internal;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.AssertionFailedError;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit test that async event listener dispatched the events
 * to the appropriate repository.
 */
@Category(UnitTest.class)
public class LuceneEventListenerJUnitTest {

  @Test
  public void testProcessBatch() throws IOException, BucketNotFoundException {
    RepositoryManager manager = Mockito.mock(RepositoryManager.class);
    IndexRepository repo1 = Mockito.mock(IndexRepository.class);
    IndexRepository repo2 = Mockito.mock(IndexRepository.class);
    Region region1 = Mockito.mock(Region.class);
    Region region2 = Mockito.mock(Region.class);
    
    Object callback1 = new Object();
    
    Mockito.when(manager.getRepository(eq(region1), any(), eq(callback1))).thenReturn(repo1);
    Mockito.when(manager.getRepository(eq(region2), any(), eq(null))).thenReturn(repo2);

    LuceneEventListener listener = new LuceneEventListener(manager);

    List<AsyncEvent> events = new ArrayList<AsyncEvent>();

    int numEntries = 100;
    for (int i = 0; i < numEntries; i++) {
      AsyncEvent event = Mockito.mock(AsyncEvent.class);

      Region region = i % 2 == 0 ? region1 : region2;
      Object callback = i % 2 == 0 ? callback1 : null;
      Mockito.when(event.getRegion()).thenReturn(region);
      Mockito.when(event.getKey()).thenReturn(i);
      Mockito.when(event.getCallbackArgument()).thenReturn(callback);

      switch (i % 3) {
      case 0:
        Mockito.when(event.getOperation()).thenReturn(Operation.CREATE);
        Mockito.when(event.getDeserializedValue()).thenReturn(i);
        break;
      case 1:
        Mockito.when(event.getOperation()).thenReturn(Operation.UPDATE);
        Mockito.when(event.getDeserializedValue()).thenReturn(i);
        break;
      case 2:
        Mockito.when(event.getOperation()).thenReturn(Operation.DESTROY);
        Mockito.when(event.getDeserializedValue()).thenThrow(new AssertionFailedError());
        break;
      }

      events.add(event);
    }

    listener.processEvents(events);

    verify(repo1, atLeast(numEntries / 6)).create(any(), any());
    verify(repo1, atLeast(numEntries / 6)).delete(any());
    verify(repo1, atLeast(numEntries / 6)).update(any(), any());
    verify(repo2, atLeast(numEntries / 6)).create(any(), any());
    verify(repo2, atLeast(numEntries / 6)).delete(any());
    verify(repo2, atLeast(numEntries / 6)).update(any(), any());
    verify(repo1, times(1)).commit();
    verify(repo2, times(1)).commit();
  }
}
