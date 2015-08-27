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
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit test that async event listener dispatched the events
 * to the appropriate repository.
 */
@Category(UnitTest.class)
public class LuceneEventListenerJUnitTest {

  @Test
  public void testProcessBatch() throws IOException {
    RepositoryManager manager = Mockito.mock(RepositoryManager.class);
    IndexRepository repo1 = Mockito.mock(IndexRepository.class);
    IndexRepository repo2 = Mockito.mock(IndexRepository.class);
    Region region1 = Mockito.mock(Region.class);
    Region region2 = Mockito.mock(Region.class);

    Mockito.when(manager.getRepository(eq(region1), any())).thenReturn(repo1);
    Mockito.when(manager.getRepository(eq(region2), any())).thenReturn(repo2);

    LuceneEventListener listener = new LuceneEventListener(manager);

    List<AsyncEvent> events = new ArrayList<AsyncEvent>();

    int numEntries = 100;
    for (int i = 0; i < numEntries; i++) {
      AsyncEvent event = Mockito.mock(AsyncEvent.class);

      Region region = i % 2 == 0 ? region1 : region2;
      Mockito.when(event.getRegion()).thenReturn(region);
      Mockito.when(event.getKey()).thenReturn(i);

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
