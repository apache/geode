package org.apache.geode.cache.query.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.internal.cache.InternalCache;

/**
 * although max_execution_time is set as 10ms, the monitor thread can sleep more than the specified
 * time, so query will be cancelled at un-deterministic time after 10ms. We cannot assert on
 * specific time at which the query will be cancelled. We can only assert that the query will be
 * cancelled at one point after 10ms.
 */
public class QueryMonitorTest {

  private static InternalCache cache;
  private static QueryMonitor monitor;
  private static long max_execution_time = 5;

  @BeforeClass
  public static void setUp() {
    cache = mock(InternalCache.class);
    monitor = new QueryMonitor(cache, max_execution_time);
    Thread monitorThread = new Thread(() -> monitor.run(), "query monitor thread");
    monitorThread.setDaemon(true);
    monitorThread.start();
  }

  @Test
  public void queryIsCancelled() {
    List<DefaultQuery> queries = new ArrayList<>();
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      DefaultQuery query = new DefaultQuery("query" + i, cache, false);
      queries.add(query);
      Thread queryExecutionThread = createQueryExecutionThread(i);
      threads.add(queryExecutionThread);
      monitor.monitorQueryThread(queryExecutionThread, query);
    }

    for (DefaultQuery query : queries) {
      // make sure the isCancelled flag in Query is set correctly
      Awaitility.await().until(() -> query.isCanceled());
    }
    Awaitility.await().until(() -> monitor.getQueryMonitorThreadCount() == 0);
    // make sure all thread died
    for (Thread thread : threads) {
      Awaitility.await().until(() -> !thread.isAlive());
    }
  }

  @Test
  public void cqQueryIsNotMonitored() {
    DefaultQuery query = mock(DefaultQuery.class);
    when(query.isCqQuery()).thenReturn(true);
    monitor.monitorQueryThread(mock(Thread.class), query);
    assertThat(monitor.getQueryMonitorThreadCount()).isEqualTo(0);
  }

  private Thread createQueryExecutionThread(int i) {
    Thread thread = new Thread(() -> {
      // make sure the threadlocal variable is updated
      Awaitility.await().until(() -> QueryMonitor.isQueryExecutionCanceled());
    });
    thread.setName("query" + i);
    return thread;
  }

}
