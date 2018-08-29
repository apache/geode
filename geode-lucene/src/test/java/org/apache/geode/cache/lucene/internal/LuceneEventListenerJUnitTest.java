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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * Unit test that async event listener dispatched the events to the appropriate repository.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LuceneEventListener.class, LogService.class})
@Category({LuceneTest.class})
@PowerMockIgnore({"*.UnitTest", "*.LuceneTest"})
public class LuceneEventListenerJUnitTest {

  private RepositoryManager manager;
  private LuceneEventListener listener;
  private InternalCache cache;

  @Before
  public void setup() {
    cache = Fakes.cache();
    manager = Mockito.mock(RepositoryManager.class);
    listener = new LuceneEventListener(cache, manager);
  }

  @After
  public void clearExceptionListener() {
    LuceneEventListener.setExceptionObserver(null);
  }

  @Test
  public void pdxReadSerializedFlagShouldBeResetBackToOriginalValueAfterProcessingEvents() {
    ArgumentCaptor valueCapture = ArgumentCaptor.forClass(Boolean.class);
    doNothing().when(cache).setPdxReadSerializedOverride((Boolean) valueCapture.capture());

    boolean originalPdxReadSerialized = cache.getPdxReadSerializedOverride();
    try {
      cache.setPdxReadSerializedOverride(true);
      Assert.assertTrue((Boolean) valueCapture.getValue());
      listener.process(new LinkedList<>());
      Assert.assertTrue(!(Boolean) valueCapture.getValue());
    } finally {
      cache.setPdxReadSerializedOverride(originalPdxReadSerialized);
    }
  }

  @Test
  public void testProcessBatch() throws Exception {

    IndexRepository repo1 = Mockito.mock(IndexRepository.class);
    IndexRepository repo2 = Mockito.mock(IndexRepository.class);
    Region region1 = Mockito.mock(Region.class);
    Region region2 = Mockito.mock(Region.class);

    Object callback1 = new Object();

    Mockito.when(manager.getRepository(eq(region1), any(), eq(callback1))).thenReturn(repo1);
    Mockito.when(manager.getRepository(eq(region2), any(), eq(null))).thenReturn(repo2);
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
    Mockito.when(manager.getRepository(any(), any(), any()))
        .thenThrow(BucketNotFoundException.class);

    PowerMockito.mockStatic(LogService.class);
    Logger logger = Mockito.mock(Logger.class);
    Mockito.when(LogService.getLogger()).thenReturn(logger);

    AsyncEvent event = Mockito.mock(AsyncEvent.class);
    boolean result = listener.processEvents(Arrays.asList(event));
    assertFalse(result);
    verify(logger, never()).error(anyString(), any(Exception.class));
  }

  @Test
  public void shouldThrowAndCaptureIOException() throws BucketNotFoundException {
    doAnswer((m) -> {
      throw new IOException();
    }).when(manager).getRepository(any(), any(), any());
    AtomicReference<Throwable> lastException = new AtomicReference<>();
    LuceneEventListener.setExceptionObserver(lastException::set);
    AsyncEvent event = Mockito.mock(AsyncEvent.class);
    try {
      listener.processEvents(Arrays.asList(event));
      fail("should have thrown an exception");
    } catch (InternalGemFireError expected) {
      assertEquals(expected, lastException.get());
    }
  }
}
