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

package org.apache.geode.cache.query.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

/**
 * QueryMonitor class, monitors the query execution time. 
 * Instantiated based on the system property MAX_QUERY_EXECUTION_TIME. At most 
 * there will be one query monitor-thread that cancels the long running queries.
 * 
 * The queries to be monitored is added into the ordered queue, ordered based
 * on its start/arrival time. The first one in the Queue is the older query 
 * that will be canceled first.
 * 
 * The QueryMonitor cancels a query-execution thread if its taking more than
 * the max time. 
 * 
 * @since GemFire 6.0
 */
public class QueryMonitor implements Runnable {
  private static final Logger logger = LogService.getLogger();

  /**
   * Holds the query execution status for the thread executing the query.
   * FALSE if the query is not canceled due to max query execution timeout.
   * TRUE it the query is canceled due to max query execution timeout timeout.
   */
  private static ThreadLocal<AtomicBoolean> queryExecutionStatus = new ThreadLocal<AtomicBoolean>() {
    @Override 
    protected AtomicBoolean initialValue() {
      return new AtomicBoolean(Boolean.FALSE);
    }    
  };
  
  private final long maxQueryExecutionTime;

  private static final ConcurrentLinkedQueue queryThreads = new ConcurrentLinkedQueue();

  private Thread monitoringThread;
  private final AtomicBoolean stopped = new AtomicBoolean(Boolean.FALSE);

  /** For DUnit test purpose */
  private ConcurrentMap queryMonitorTasks = null;
  
  //Variables for cancelling queries due to low memory
  private volatile static Boolean LOW_MEMORY = Boolean.FALSE;
  private volatile static long LOW_MEMORY_USED_BYTES = 0;
  
  public QueryMonitor(long maxQueryExecutionTime) {
    this.maxQueryExecutionTime = maxQueryExecutionTime;
  }

  /**
   * Add query to be monitored.
   * @param queryThread Thread executing the query.
   * @param query Query.
   */
  public void monitorQueryThread(Thread queryThread, Query query) {
    if (LOW_MEMORY) {
      String reason = LocalizedStrings.QueryMonitor_LOW_MEMORY_CANCELED_QUERY.toLocalizedString(LOW_MEMORY_USED_BYTES);
      ((DefaultQuery) query).setCanceled(true, new QueryExecutionLowMemoryException(reason));
      throw new QueryExecutionLowMemoryException(reason);
    }
    QueryThreadTask queryTask = new QueryThreadTask(queryThread, query, queryExecutionStatus.get());
    synchronized (queryThreads){
      queryThreads.add(queryTask);
      queryThreads.notify();
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Adding thread to QueryMonitor. QueryMonitor size is:{}, Thread (id): {} query: {} thread is : {}", 
          queryThreads.size(), queryThread.getId(), query.getQueryString(), queryThread);
    }

    /** For dunit test purpose */
    if (GemFireCacheImpl.getInstance() != null && GemFireCacheImpl.getInstance().TEST_MAX_QUERY_EXECUTION_TIME > 0) {
      if (this.queryMonitorTasks == null){
        this.queryMonitorTasks = new ConcurrentHashMap();
      }
      this.queryMonitorTasks.put(queryThread, queryTask);
    }    
  }
  
  /**
   * Stops monitoring the query.
   * Removes the passed thread from QueryMonitor queue.
   * @param queryThread
   */
  public void stopMonitoringQueryThread(Thread queryThread, Query query) {
    // Re-Set the queryExecution status on the LocalThread.
    QueryExecutionTimeoutException testException = null;
    DefaultQuery q = (DefaultQuery)query;
    boolean[] queryCompleted = q.getQueryCompletedForMonitoring();
    
    synchronized(queryCompleted) {
      queryExecutionStatus.get().getAndSet(Boolean.FALSE);

      // START - DUnit Test purpose.
      if (GemFireCacheImpl.getInstance() != null && GemFireCacheImpl.getInstance().TEST_MAX_QUERY_EXECUTION_TIME > 0){
        long maxTimeSet = GemFireCacheImpl.getInstance().TEST_MAX_QUERY_EXECUTION_TIME;
        QueryThreadTask queryTask = (QueryThreadTask)queryThreads.peek();

        long currentTime = System.currentTimeMillis();
        
        // This is to check if the QueryMonitoring thread slept longer than the expected time.
        // Its seen that in some cases based on OS thread scheduling the thread can sleep much
        // longer than the specified time.
        if (queryTask != null) {
          if ((currentTime - queryTask.StartTime) > maxTimeSet){
            // The sleep() is unpredictable.
            testException = new QueryExecutionTimeoutException("The QueryMonitor thread may be sleeping longer than" +
                " the set sleep time. This will happen as the sleep is based on OS thread scheduling," +
            " verify the time spent by the executor thread.");
          }
        }
      }
      // END - DUnit Test purpose.

      q.setQueryCompletedForMonitoring(true);
      // Remove the query task from the queue.
      queryThreads.remove(new QueryThreadTask(queryThread, null, null));
    }
    
    if (logger.isDebugEnabled()) {
      logger.debug("Removed thread from QueryMonitor. QueryMonitor size is:{}, Thread ID is: {}  thread is : {}", 
          queryThreads.size(), queryThread.getId(), queryThread);
    }
    
    if (testException != null){
      throw testException;
    }
  }

  /**
   * This method is called to check if the query execution is canceled.
   * The QueryMonitor cancels the query execution if it takes more than 
   * the max query execution time set or in low memory situations where
   * critical heap percentage has been set on the resource manager
   *  
   * The max query execution time is set using the system property 
   * gemfire.Cache.MAX_QUERY_EXECUTION_TIME 
   */  
  public static void isQueryExecutionCanceled(){    
    if (queryExecutionStatus.get() != null && queryExecutionStatus.get().get()){
      throw new QueryExecutionCanceledException();
    }
  }
 
  /**
   * Stops query monitoring.
   */
  public void stopMonitoring(){
    //synchronized in the rare case where query monitor was created but not yet run
    synchronized(stopped) {
      if (this.monitoringThread != null) {
        this.monitoringThread.interrupt();
      }
      stopped.set(Boolean.TRUE);
    }
  }

  /**
   * Starts monitoring the query.
   * If query runs longer than the set MAX_QUERY_EXECUTION_TIME, interrupts the
   * thread executing the query.
   */
  public void run(){
    //if the query monitor is stopped before run has been called, we should not run
    synchronized (stopped) {
      if (stopped.get()) {
        queryThreads.clear();
        return;
      }
      this.monitoringThread = Thread.currentThread();
    }
    try {
      QueryThreadTask queryTask = null;
      long sleepTime = 0;
      while(true){
        // Get the first query task from the queue. This query will have the shortest 
        // remaining time that needs to canceled first.
        queryTask = (QueryThreadTask)queryThreads.peek();
        if (queryTask == null){
          // Empty queue. 
          synchronized (this.queryThreads){
            this.queryThreads.wait();
          }
          continue;
        }

        long currentTime = System.currentTimeMillis();

        // Check if the sleepTime is greater than the remaining query execution time. 
        if ((currentTime - queryTask.StartTime) < this.maxQueryExecutionTime){
          sleepTime = this.maxQueryExecutionTime - (currentTime - queryTask.StartTime);
          // Its been noted that the sleep is not guaranteed to wait for the specified 
          // time (as stated in Suns doc too), it depends on the OSs thread scheduling
          // behavior, hence thread may sleep for longer than the specified time. 
          // Specifying shorter time also hasn't worked.
          Thread.sleep(sleepTime);
          continue;
        }

        // Query execution has taken more than the max time, Set queryExecutionStatus flag 
        // to canceled (TRUE).
        boolean[] queryCompleted = ((DefaultQuery)queryTask.query).getQueryCompletedForMonitoring();
        synchronized(queryCompleted) {
          if (!queryCompleted[0]) { // Check if the query is already completed.
            ((DefaultQuery)queryTask.query).setCanceled(true, new QueryExecutionTimeoutException(LocalizedStrings.QueryMonitor_LONG_RUNNING_QUERY_CANCELED.toLocalizedString(
                GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME)));
            queryTask.queryExecutionStatus.set(Boolean.TRUE);
            // Remove the task from queue.
            queryThreads.poll();
          }
        }
        
        logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCache_LONG_RUNNING_QUERY_EXECUTION_CANCELED, 
            new Object[] {queryTask.query.getQueryString(), queryTask.queryThread.getId()}));

        if (logger.isDebugEnabled()){
          logger.debug("Query Execution for the thread {} got canceled.", queryTask.queryThread);
        }
      }
    } catch (InterruptedException ex) {
      if (logger.isDebugEnabled()){
        logger.debug("Query Monitoring thread got interrupted.");
      }
    } finally {
      this.queryThreads.clear();
    }
  }
  
  //Assumes LOW_MEMORY will only be set if query monitor is enabled
  public static boolean isLowMemory() {
    return LOW_MEMORY;
  }
  
  public static long getMemoryUsedDuringLowMemory() {
    return LOW_MEMORY_USED_BYTES;
  }
  
  public static void setLowMemory(boolean lowMemory, long usedBytes) {
    if (GemFireCacheImpl.getInstance() != null && !GemFireCacheImpl.getInstance().isQueryMonitorDisabledForLowMemory()) {
      QueryMonitor.LOW_MEMORY_USED_BYTES = usedBytes;
      QueryMonitor.LOW_MEMORY = lowMemory;
    }
  }

  public void cancelAllQueriesDueToMemory() {
    synchronized (this.queryThreads) {
      QueryThreadTask queryTask = (QueryThreadTask) queryThreads.poll();
      while (queryTask != null) {
        cancelQueryDueToLowMemory(queryTask, LOW_MEMORY_USED_BYTES);
        queryTask = (QueryThreadTask) queryThreads.poll();
      }
      queryThreads.clear();
      queryThreads.notify();
    }
  }
  
  private void cancelQueryDueToLowMemory(QueryThreadTask queryTask, long memoryThreshold) {
    boolean[] queryCompleted = ((DefaultQuery) queryTask.query).getQueryCompletedForMonitoring();
    synchronized (queryCompleted) {
      if (!queryCompleted[0]) { //cancel if query is not completed
        String reason = LocalizedStrings.QueryMonitor_LOW_MEMORY_CANCELED_QUERY.toLocalizedString(memoryThreshold);
        ((DefaultQuery) queryTask.query).setCanceled(true, new QueryExecutionLowMemoryException(reason));
        queryTask.queryExecutionStatus.set(Boolean.TRUE);
      }
    }
  }
  
  // FOR TEST PURPOSE
  public int getQueryMonitorThreadCount() {
    return this.queryThreads.size();
  }

  /**
   * Query Monitoring task, placed in the queue.
   *
   */
  private class QueryThreadTask {

    private final long StartTime;

    private final Thread queryThread;

    private final Query query;

    private final AtomicBoolean queryExecutionStatus;
    
    
    QueryThreadTask(Thread queryThread, Query query, AtomicBoolean queryExecutionStatus){
      this.StartTime = System.currentTimeMillis();
      this.queryThread = queryThread;
      this.query = query;
      this.queryExecutionStatus = queryExecutionStatus;
    }

    @Override
    public int hashCode() {
      assert this.queryThread != null;
      return this.queryThread.hashCode();
    }
    
    /**
     * The query task in the queue is identified by the thread.
     * To remove the task in the queue using the thread reference.
     */
    @Override
    public boolean equals(Object other){
      if (!(other instanceof QueryThreadTask)) {
        return false;
      }
      QueryThreadTask o = (QueryThreadTask)other;
      return this.queryThread.equals(o.queryThread);
    }

    @Override
    public String toString(){
      return new StringBuffer()
      .append("QueryThreadTask[StartTime:").append(this.StartTime)
      .append(", queryThread:").append(this.queryThread)
      .append(", threadId:").append(this.queryThread.getId())
      .append(", query:").append(this.query.getQueryString())
      .append(", queryExecutionStatus:").append(this.queryExecutionStatus)
      .append("]").toString();
    }
    
  }
}
