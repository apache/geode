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
package org.apache.geode.internal.cache;

import static java.lang.Integer.getInteger;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.CompiledSelect;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.NWayMergeResults;
import org.apache.geode.cache.query.internal.QueryExecutionContext;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This class takes the responsibility of executing the query on a data store for the buckets
 * specified in bucketList. It contains a {@code PRQueryExecutor} thread-pool executor that takes a
 * {@code Callable} task identified by {@code PartitionedRegion}, queryString and bucketId.
 *
 * The QueryTasks add results directly to a results queue. The BucketQueryResult is used not only to
 * indicate completion, and holds an exception if there one occurred while processing a query.
 */
public class PRQueryProcessor {
  private static final Logger logger = LogService.getLogger();

  static final int BUCKET_QUERY_TIMEOUT = 60;

  public static final int NUM_THREADS =
      getInteger(GeodeGlossary.GEMFIRE_PREFIX + "PRQueryProcessor.numThreads", 1);

  /* For Test purpose */
  @MutableForTesting
  public static int TEST_NUM_THREADS = 0;

  private PartitionedRegionDataStore partitionedRegionDataStore;
  private PartitionedRegion partitionedRegion;
  private final DefaultQuery query;
  private final Object[] parameters;
  private final List<Integer> buckets;
  private volatile ObjectType resultType = null;

  private boolean isIndexUsedForLocalQuery = false;

  public PRQueryProcessor(PartitionedRegionDataStore partitionedRegionDataStore, DefaultQuery query,
      Object[] parameters,
      List<Integer> buckets) {
    Assert.assertTrue(!buckets.isEmpty(), "bucket list can not be empty. ");
    this.partitionedRegionDataStore = partitionedRegionDataStore;
    this.buckets = buckets;
    this.query = query;
    this.parameters = parameters;

    partitionedRegionDataStore.partitionedRegion.getCache().getLocalQueryService();
    PRQueryExecutor.initializeExecutorService();
  }

  public PRQueryProcessor(PartitionedRegion partitionedRegion, DefaultQuery query,
      Object[] parameters,
      List<Integer> buckets) {
    Assert.assertTrue(!buckets.isEmpty(), "bucket list can not be empty. ");
    this.partitionedRegion = partitionedRegion;
    this.buckets = buckets;
    this.query = query;
    this.parameters = parameters;

    PRQueryExecutor.initializeExecutorService();
  }

  /**
   * Executes a pre-compiled query on a data store. Adds result objects to resultQueue
   *
   * @return boolean true if the result is a struct type
   * @throws ForceReattemptException if query should be tried again
   */
  public boolean executeQuery(Collection<Collection<?>> resultCollector)
      throws QueryException, InterruptedException, ForceReattemptException {
    if (NUM_THREADS > 1 || TEST_NUM_THREADS > 1) {
      executeWithThreadPool(resultCollector);
    } else {
      executeSequentially(resultCollector, buckets);
    }
    return resultType.isStructType();
  }

  private void executeWithThreadPool(Collection<Collection<?>> resultCollector)
      throws QueryException, InterruptedException, ForceReattemptException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    List<QueryTask> callableTasks = buildCallableTaskList(resultCollector);
    ExecutorService execService = PRQueryExecutor.getExecutorService();

    boolean reattemptNeeded = false;
    ForceReattemptException fre = null;

    if (!callableTasks.isEmpty()) {
      List<Future<QueryTask.BucketQueryResult>> futures =
          execService.invokeAll(callableTasks, 300, TimeUnit.SECONDS);

      Iterator<Future<QueryTask.BucketQueryResult>> itr = futures.iterator();
      while (itr.hasNext() && !execService.isShutdown() && !execService.isTerminated()) {
        Future<QueryTask.BucketQueryResult> fut = itr.next();

        final QueryTask.BucketQueryResult bqr;
        try {
          bqr = fut.get(BUCKET_QUERY_TIMEOUT, TimeUnit.SECONDS);
          bqr.handleAndThrowException();
          if (bqr.retry) {
            reattemptNeeded = true;
          }
        } catch (TimeoutException e) {
          throw new InternalGemFireException(
              String.format("Timed out while executing query, time exceeded %s",
                  BUCKET_QUERY_TIMEOUT),
              e);
        } catch (ExecutionException ee) {
          Throwable cause = ee.getCause();
          if (cause instanceof QueryException) {
            throw (QueryException) cause;
          } else {
            throw new InternalGemFireException(
                "Got unexpected exception while executing query on partitioned region bucket",
                cause);
          }
        }
      }

      CompiledSelect cs = query.getSimpleSelect();

      if (cs != null && (cs.isOrderBy() || cs.isGroupBy())) {
        ExecutionContext context =
            new QueryExecutionContext(parameters, partitionedRegion.getCache());
        int limit = query.getLimit(parameters);
        Collection<?> mergedResults = coalesceOrderedResults(resultCollector, context, cs, limit);
        resultCollector.clear();
        resultCollector.add(mergedResults);
      }
    }

    if (execService == null || execService.isShutdown() || execService.isTerminated()) {
      partitionedRegionDataStore.partitionedRegion.checkReadiness();
    }

    if (reattemptNeeded) {
      throw fre;
    }
  }

  private void executeSequentially(Collection<Collection<?>> resultCollector, List<Integer> buckets)
      throws QueryException, ForceReattemptException {
    ExecutionContext context =
        new QueryExecutionContext(parameters, partitionedRegion.getCache(), query);

    CompiledSelect cs = query.getSimpleSelect();
    int limit = query.getLimit(parameters);
    if (cs != null && cs.isOrderBy()) {
      for (Integer bucketID : buckets) {
        List<Integer> singleBucket = Collections.singletonList(bucketID);
        context.setBucketList(singleBucket);
        executeQueryOnBuckets(resultCollector, context);
      }
      Collection<?> mergedResults = coalesceOrderedResults(resultCollector, context, cs, limit);
      resultCollector.clear();
      resultCollector.add(mergedResults);

    } else {
      context.setBucketList(buckets);
      executeQueryOnBuckets(resultCollector, context);
    }
  }

  private <E> Collection<E> coalesceOrderedResults(Collection<Collection<?>> results,
      ExecutionContext context, CompiledSelect cs, int limit) {
    List<Collection<E>> sortedResults = new ArrayList<>(uncheckedCast(results));
    return new NWayMergeResults<>(sortedResults, cs.isDistinct(), limit, cs.getOrderByAttrs(),
        context, cs.getElementTypeForOrderByQueries());
  }

  private void executeQueryOnBuckets(Collection<Collection<?>> resultCollector,
      ExecutionContext context)
      throws ForceReattemptException, QueryException {
    // Check if QueryMonitor is enabled, if so add query to be monitored.
    QueryMonitor queryMonitor = null;
    if (GemFireCacheImpl.getInstance() != null) {
      queryMonitor = GemFireCacheImpl.getInstance().getQueryMonitor();
    }

    try {
      if (queryMonitor != null) {
        // Add current thread to be monitored by QueryMonitor.
        queryMonitor.monitorQueryExecution(context);
      }

      Object results = query.executeUsingContext(context);

      synchronized (resultCollector) {
        resultType = ((SelectResults<?>) results).getCollectionType().getElementType();
        resultCollector.add((Collection<?>) results);
      }
      isIndexUsedForLocalQuery = ((QueryExecutionContext) context).isIndexUsed();

    } catch (BucketMovedException bme) {
      if (logger.isDebugEnabled()) {
        logger.debug("Query targeted local bucket not found. {}", bme.getMessage(), bme);
      }
      throw new ForceReattemptException("Query targeted local bucket not found." + bme.getMessage(),
          bme);
    } catch (RegionDestroyedException rde) {
      throw new QueryInvocationTargetException(
          "The Region on which query is executed may have been destroyed." + rde.getMessage(), rde);
    } catch (QueryException qe) {
      // Check if PR is locally destroyed.
      if (partitionedRegion.isLocallyDestroyed || partitionedRegion.isClosed) {
        throw new ForceReattemptException(
            "Local Partition Region or the targeted bucket has been moved");
      }
      throw qe;
    } finally {
      if (queryMonitor != null) {
        queryMonitor.stopMonitoringQueryExecution(context);
      }
    }
  }

  private @NotNull List<QueryTask> buildCallableTaskList(Collection<Collection<?>> resultsColl) {
    List<QueryTask> callableTasks = new ArrayList<>();
    for (Integer bId : buckets) {
      callableTasks.add(new QueryTask(query, parameters, bId, resultsColl));
    }
    return callableTasks;
  }

  public boolean isIndexUsed() {
    return isIndexUsedForLocalQuery;
  }

  public static void shutdown() {
    PRQueryExecutor.shutdown();
  }

  public static void shutdownNow() {
    PRQueryExecutor.shutdownNow();
  }

  /**
   * A ThreadPool ( Fixed Size ) with an executor service to execute the query execution spread over
   * buckets.
   */
  static class PRQueryExecutor {

    @MakeNotStatic
    private static ExecutorService execService = null;

    /**
     * Closes the executor service. This is called from
     * {@link PartitionedRegion#afterRegionsClosedByCacheClose()}
     */
    static synchronized void shutdown() {
      if (execService != null) {
        execService.shutdown();
      }
    }

    static synchronized void shutdownNow() {
      if (execService != null) {
        execService.shutdownNow();
      }
    }

    static synchronized ExecutorService getExecutorService() {
      if (execService == null) {
        initializeExecutorService();
      }
      assert execService != null;
      return execService;
    }

    /**
     * Creates the Executor Service.
     */
    static synchronized void initializeExecutorService() {
      if (execService == null || execService.isShutdown() || execService.isTerminated()) {
        int numThreads = (TEST_NUM_THREADS > 1 ? TEST_NUM_THREADS : NUM_THREADS);
        execService = LoggingExecutors.newFixedThreadPool(numThreads, "PRQueryProcessor", true);
      }
    }
  }

  /**
   * Status token placed in results stream to track completion of query results for a given bucket
   */
  public static class EndOfBucket implements DataSerializableFixedID {

    private int bucketId;

    /** Required by DataSerializer */
    public EndOfBucket() {}

    public EndOfBucket(int bucketId) {
      this.bucketId = bucketId;
    }

    public int getBucketId() {
      return bucketId;
    }

    @Override
    public String toString() {
      return "EndOfBucket(" + bucketId + ")";
    }

    @Override
    public int getDSFID() {
      return END_OF_BUCKET;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      bucketId = in.readInt();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      out.writeInt(bucketId);
    }

    @Override
    public KnownVersion[] getSerializationVersions() {
      return null;
    }
  }

  /**
   * Implementation of call-able task to execute query on a bucket region. This task will be
   * generated by the PRQueryProcessor.
   *
   */
  @SuppressWarnings("synthetic-access")
  private class QueryTask implements Callable<QueryTask.BucketQueryResult> {
    private final DefaultQuery query;
    private final Object[] parameters;
    private final Integer bucketId;
    private final Collection<Collection<?>> results;

    public QueryTask(DefaultQuery query, Object[] parameters, Integer bucketId,
        final Collection<Collection<?>> results) {
      this.query = query;
      this.bucketId = bucketId;
      this.results = results;
      this.parameters = parameters;
    }

    @Override
    public BucketQueryResult call() throws Exception {
      BucketQueryResult bukResult = new BucketQueryResult(bucketId);
      try {
        List<Integer> bucketList = Collections.singletonList(bucketId);
        ExecutionContext context =
            new QueryExecutionContext(parameters, partitionedRegion.getCache(), query);
        context.setBucketList(bucketList);
        executeQueryOnBuckets(results, context);
      } catch (ForceReattemptException | QueryException | CacheRuntimeException fre) {
        bukResult.setException(fre);
      }
      return bukResult;
    }

    /**
     * Encapsulates the result for the query on the bucket.
     *
     */
    private class BucketQueryResult {

      private final int bucketId;
      private Exception exception = null;
      public boolean retry = false;

      public BucketQueryResult(int bukId) {
        bucketId = bukId;
      }

      public Exception getException() {
        return exception;
      }

      public boolean exceptionOccurred() {
        return exception != null;
      }

      public void setException(Exception e) {
        exception = e;
      }

      public Integer getBucketId() {
        return bucketId;
      }

      public boolean isReattemptNeeded() {
        return exception instanceof ForceReattemptException;
      }

      public void handleAndThrowException() throws QueryException {
        if (exception != null) {
          if (exception instanceof QueryException) {
            throw (QueryException) exception;
          } else if (exception instanceof CacheRuntimeException) {
            throw (CacheRuntimeException) exception;
          }
        }
      }
    }
  }
}
