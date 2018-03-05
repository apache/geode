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
package org.apache.geode.cache.lucene.internal;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit test that async event listener dispatched the events to the appropriate repository.
 */
@Category(UnitTest.class)
public class LuceneEventListenerJUnitTest {

  @After
  public void clearExceptionListener() {
    LuceneEventListener.setExceptionObserver(null);
  }

  @Test
  public void pdxReadSerializedFlagShouldBeResetBackToOriginalValueAfterProcessingEvents() {
    boolean originalPdxReadSerialized = DefaultQuery.getPdxReadSerialized();
    try {
      DefaultQuery.setPdxReadSerialized(true);
      LuceneEventListener luceneEventListener = new LuceneEventListener(null);
      luceneEventListener.process(new LinkedList());
      assertTrue(DefaultQuery.getPdxReadSerialized());
    } finally {
      DefaultQuery.setPdxReadSerialized(originalPdxReadSerialized);
    }
  }

  @Test
  public void testProcessBatch() throws Exception {
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

      switch (i % 4) {
        case 0:
        case 1:
          final EntrySnapshot entry = mock(EntrySnapshot.class);
          when(entry.getRawValue(true)).thenReturn(i);
          when(region.getEntry(eq(i))).thenReturn(entry);
          break;
        case 2:
        case 3:
          // Do nothing, get value will return a destroy
          break;
      }

      events.add(event);
    }

    listener.processEvents(events);

    verify(repo1, atLeast(numEntries / 4)).delete(any());
    verify(repo1, atLeast(numEntries / 4)).update(any(), any());
    verify(repo2, atLeast(numEntries / 4)).delete(any());
    verify(repo2, atLeast(numEntries / 4)).update(any(), any());
    verify(repo1, times(1)).commit();
    verify(repo2, times(1)).commit();
  }

  @Test
  public void shouldHandleBucketNotFoundExceptionWithoutLoggingError()
      throws BucketNotFoundException {
    RepositoryManager manager = Mockito.mock(RepositoryManager.class);
    Logger log = Mockito.mock(Logger.class);
    Mockito.when(manager.getRepository(any(), any(), any()))
        .thenThrow(BucketNotFoundException.class);
    LuceneEventListener listener = new LuceneEventListener(manager);
    listener.logger = log;
    AsyncEvent event = Mockito.mock(AsyncEvent.class);
    boolean result = listener.processEvents(Arrays.asList(new AsyncEvent[] {event}));
    assertFalse(result);
    verify(log, never()).error(anyString(), any(Exception.class));
  }

  @Test
  public void shouldThrowAndCaptureIOException() throws BucketNotFoundException {
    RepositoryManager manager = Mockito.mock(RepositoryManager.class);
    Mockito.when(manager.getRepository(any(), any(), any())).thenThrow(IOException.class);
    AtomicReference<Throwable> lastException = new AtomicReference<>();
    LuceneEventListener.setExceptionObserver(lastException::set);
    LuceneEventListener listener = new LuceneEventListener(manager);
    AsyncEvent event = Mockito.mock(AsyncEvent.class);
    try {
      listener.processEvents(Arrays.asList(new AsyncEvent[] {event}));
      fail("should have thrown an exception");
    } catch (InternalGemFireError expected) {
      assertEquals(expected, lastException.get());
    }
  }
}
