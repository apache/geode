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
package org.apache.geode.cache.query.internal;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;

/**
 * QueryMonitor class, monitors the query execution time. Instantiated based on the system property
 * MAX_QUERY_EXECUTION_TIME. At most there will be one query monitor-thread that cancels the long
 * running queries.
 *
 * The queries to be monitored is added into the ordered queue, ordered based on its start/arrival
 * time. The first one in the Queue is the older query that will be canceled first.
 *
 * The QueryMonitor cancels a query-execution thread if its taking more than the max time.
 *
 * @since GemFire 6.0
 */
public class QueryMonitor implements Runnable {
  private static final Logger logger = LogService.getLogger();

  private final InternalCache cache;
  /**
   * Holds the query execution status for the thread executing the query. FALSE if the query is not
   * canceled due to max query execution timeout. TRUE it the query is canceled due to max query
   * execution timeout timeout.
   */
  private static final ThreadLocal<AtomicBoolean> queryCancelled =
      ThreadLocal.withInitial(() -> new AtomicBoolean(Boolean.FALSE));

  private final long maxQueryExecutionTime;

  private static final ConcurrentLinkedQueue queryThreads = new ConcurrentLinkedQueue();

  private Thread monitoringThread;

  private final AtomicBoolean stopped = new AtomicBoolean(Boolean.FALSE);

  // Variables for cancelling queries due to low memory
  private static volatile Boolean LOW_MEMORY = Boolean.FALSE;

  private static volatile long LOW_MEMORY_USED_BYTES = 0;

  public QueryMonitor(InternalCache cache, long maxQueryExecutionTime) {
    this.cache = cache;
    this.maxQueryExecutionTime = maxQueryExecutionTime;
  }

  /**
   * Add query to be monitored.
   *
   * @param queryThread Thread executing the query.
   * @param query Query.
   */
  public void monitorQueryThread(Thread queryThread, DefaultQuery query) {
    // cq query is not monitored
    if (query.isCqQuery()) {
      return;
    }

    if (LOW_MEMORY) {
      String reason = String.format(
          "Query execution canceled due to memory threshold crossed in system, memory used: %s bytes.",
          LOW_MEMORY_USED_BYTES);
      query.setCanceled(new QueryExecutionLowMemoryException(reason));
      throw new QueryExecutionLowMemoryException(reason);
    }
    QueryThreadTask queryTask = new QueryThreadTask(queryThread, query, queryCancelled.get());
    synchronized (queryThreads) {
      queryThreads.add(queryTask);
      queryThreads.notifyAll();
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Adding thread to QueryMonitor. QueryMonitor size is:{}, Thread (id): {} query: {} thread is : {}",
          queryThreads.size(), queryThread.getId(), query.getQueryString(), queryThread);
    }

  }

  /**
   * Stops monitoring the query. Removes the passed thread from QueryMonitor queue.
   */
  public void stopMonitoringQueryThread(Thread queryThread, DefaultQuery query) {
    // Re-Set the queryExecution status on the LocalThread.
    QueryExecutionTimeoutException testException = null;
    boolean[] queryCompleted = query.getQueryCompletedForMonitoring();

    synchronized (queryCompleted) {
      queryCancelled.get().getAndSet(Boolean.FALSE);
      query.setQueryCompletedForMonitoring(true);
      // Remove the query task from the queue.
      queryThreads.remove(new QueryThreadTask(queryThread, null, null));
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Removed thread from QueryMonitor. QueryMonitor size is:{}, Thread ID is: {}  thread is : {}",
          queryThreads.size(), queryThread.getId(), queryThread);
    }

    if (testException != null) {
      throw testException;
    }
  }

  /**
   * This method is called to check if the query execution is canceled. The QueryMonitor cancels the
   * query execution if it takes more than the max query execution time set or in low memory
   * situations where critical heap percentage has been set on the resource manager
   *
   * The max query execution time is set using the system property
   * gemfire.Cache.MAX_QUERY_EXECUTION_TIME
   */
  public static void isQueryExecutionCanceled() {
    if (queryCancelled.get() != null && queryCancelled.get().get()) {
      throw new QueryExecutionCanceledException();
    }
  }

  /**
   * Stops query monitoring.
   */
  public void stopMonitoring() {
    // synchronized in the rare case where query monitor was created but not yet run
    synchronized (this.stopped) {
      if (this.monitoringThread != null) {
        this.monitoringThread.interrupt();
      }
      this.stopped.set(Boolean.TRUE);
    }
  }

  /**
   * Starts monitoring the query. If query runs longer than the set MAX_QUERY_EXECUTION_TIME,
   * interrupts the thread executing the query.
   */
  @Override
  public void run() {
    // if the query monitor is stopped before run has been called, we should not run
    synchronized (this.stopped) {
      if (this.stopped.get()) {
        queryThreads.clear();
        return;
      }
      this.monitoringThread = Thread.currentThread();
    }
    try {
      QueryThreadTask queryTask;
      long sleepTime;
      while (true) {
        // Get the first query task from the queue. This query will have the shortest
        // remaining time that needs to canceled first.
        queryTask = (QueryThreadTask) queryThreads.peek();
        if (queryTask == null) {
          synchronized (queryThreads) {
            queryThreads.wait();
          }
          continue;
        }

        long executionTime = System.currentTimeMillis() - queryTask.StartTime;
        // Check if the sleepTime is greater than the remaining query execution time.
        if (executionTime < this.maxQueryExecutionTime) {
          sleepTime = this.maxQueryExecutionTime - executionTime;
          // Its been noted that the sleep is not guaranteed to wait for the specified
          // time (as stated in Suns doc too), it depends on the OSs thread scheduling
          // behavior, hence thread may sleep for longer than the specified time.
          // Specifying shorter time also hasn't worked.
          Thread.sleep(sleepTime);
          continue;
        }
        // Query execution has taken more than the max time, Set queryCancelled flag
        // to true.
        boolean[] queryCompleted = queryTask.query.getQueryCompletedForMonitoring();
        synchronized (queryCompleted) {
          // check if query is already completed
          if (!queryCompleted[0]) {
            queryTask.query.setCanceled(new QueryExecutionTimeoutException(String
                .format("Query execution cancelled after exceeding max execution time %sms.",
                    this.maxQueryExecutionTime)));
            queryTask.queryCancelled.set(Boolean.TRUE);
            // remove the query threads from monitoring queue
            queryThreads.poll();
            logger.info(String.format(
                "%s is set as canceled after %s milliseconds", queryTask.toString(),
                executionTime));
          }
        }
      }
    } catch (InterruptedException ignore) {
      if (logger.isDebugEnabled()) {
        logger.debug("Query Monitoring thread got interrupted.");
      }
    } finally {
      queryThreads.clear();
    }
  }

  /**
   * Assumes LOW_MEMORY will only be set if query monitor is enabled
   */
  public static boolean isLowMemory() {
    return LOW_MEMORY;
  }

  public static long getMemoryUsedDuringLowMemory() {
    return LOW_MEMORY_USED_BYTES;
  }

  public void setLowMemory(boolean lowMemory, long usedBytes) {
    if (cache != null && !cache.isQueryMonitorDisabledForLowMemory()) {
      QueryMonitor.LOW_MEMORY_USED_BYTES = usedBytes;
      QueryMonitor.LOW_MEMORY = lowMemory;
    }
  }

  public void cancelAllQueriesDueToMemory() {
    synchronized (queryThreads) {
      QueryThreadTask queryTask = (QueryThreadTask) queryThreads.poll();
      while (queryTask != null) {
        cancelQueryDueToLowMemory(queryTask, LOW_MEMORY_USED_BYTES);
        queryTask = (QueryThreadTask) queryThreads.poll();
      }
      queryThreads.clear();
      queryThreads.notifyAll();
    }
  }

  private void cancelQueryDueToLowMemory(QueryThreadTask queryTask, long memoryThreshold) {
    boolean[] queryCompleted = queryTask.query.getQueryCompletedForMonitoring();
    synchronized (queryCompleted) {
      if (!queryCompleted[0]) {
        // cancel if query is not completed
        String reason = String.format(
            "Query execution canceled due to memory threshold crossed in system, memory used: %s bytes.",
            memoryThreshold);
        queryTask.query.setCanceled(
            new QueryExecutionLowMemoryException(reason));
        queryTask.queryCancelled.set(Boolean.TRUE);
      }
    }
  }

  /** FOR TEST PURPOSE */
  public static int getQueryMonitorThreadCount() {
    return queryThreads.size();
  }

  /**
   * Query Monitoring task, placed in the queue.
   */
  private static class QueryThreadTask {

    // package-private to avoid synthetic accessor
    final long StartTime;

    // package-private to avoid synthetic accessor
    final Thread queryThread;

    // package-private to avoid synthetic accessor
    final DefaultQuery query;

    // package-private to avoid synthetic accessor
    final AtomicBoolean queryCancelled;

    QueryThreadTask(Thread queryThread, DefaultQuery query, AtomicBoolean queryCancelled) {
      this.StartTime = System.currentTimeMillis();
      this.queryThread = queryThread;
      this.query = query;
      this.queryCancelled = queryCancelled;
    }

    @Override
    public int hashCode() {
      assert this.queryThread != null;
      return this.queryThread.hashCode();
    }

    /**
     * The query task in the queue is identified by the thread. To remove the task in the queue
     * using the thread reference.
     */
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof QueryThreadTask)) {
        return false;
      }
      QueryThreadTask o = (QueryThreadTask) other;
      return this.queryThread.equals(o.queryThread);
    }

    @Override
    public String toString() {
      return new StringBuilder().append("QueryThreadTask[StartTime:").append(this.StartTime)
          .append(", queryThread:").append(this.queryThread).append(", threadId:")
          .append(this.queryThread.getId()).append(", query:").append(this.query.getQueryString())
          .append(", queryCancelled:").append(this.queryCancelled).append(']')
          .toString();
    }
  }
}
