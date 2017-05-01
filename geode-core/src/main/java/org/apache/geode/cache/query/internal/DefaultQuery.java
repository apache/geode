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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.ServerProxy;
import org.apache.geode.cache.client.internal.UserAttributes;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.QueryStatistics;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PRQueryProcessor;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * Thread-safe implementation of org.apache.persistence.query.Query
 */
public class DefaultQuery implements Query {

  private final CompiledValue compiledQuery;

  private final String queryString;

  private final InternalCache cache;

  private ServerProxy serverProxy;

  protected AtomicLong numExecutions = new AtomicLong(0);

  private final AtomicLong totalExecutionTime = new AtomicLong(0);

  private final QueryStatistics stats;

  private boolean traceOn = false;

  private static final Object[] EMPTY_ARRAY = new Object[0];

  public static boolean QUERY_VERBOSE =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "Query.VERBOSE");

  /**
   * System property to cleanup the compiled query. The compiled query will be removed if it is not
   * used for more than the set value. By default its set to 10 minutes, the time is set in
   * MilliSecs.
   */
  public static final int COMPILED_QUERY_CLEAR_TIME = Integer.getInteger(
      DistributionConfig.GEMFIRE_PREFIX + "Query.COMPILED_QUERY_CLEAR_TIME", 10 * 60 * 1000);

  public static int TEST_COMPILED_QUERY_CLEAR_TIME = -1;

  /**
   * Use to represent null result. Used while adding PR results to the results-queue, which is a
   * blocking queue.
   */
  public static final Object NULL_RESULT = new Object();

  private volatile boolean isCanceled = false;

  private CacheRuntimeException canceledException;

  /**
   * This is declared as array so that it can be synchronized between two threads to validate the
   * state.
   */
  private final boolean[] queryCompletedForMonitoring = new boolean[] {false};

  private ProxyCache proxyCache;

  private boolean isCqQuery = false;

  private boolean isQueryWithFunctionContext = false;

  /**
   * Holds the CQ reference. In cases of peer PRs this will be set to null even though isCqQuery is
   * set to true.
   */
  private InternalCqQuery cqQuery = null;

  private volatile boolean lastUsed = true;

  public static TestHook testHook;

  private static final ThreadLocal<Boolean> pdxReadSerialized =
      ThreadLocal.withInitial(() -> Boolean.FALSE);

  /** indicates query executed remotely */
  private boolean isRemoteQuery = false;

  // to prevent objects from getting deserialized
  private boolean keepSerialized = false;

  public static final Set<String> reservedKeywords = new HashSet<>();

  static {
    reservedKeywords.add("hint");
    reservedKeywords.add("all");
    reservedKeywords.add("map");
    reservedKeywords.add("count");
    reservedKeywords.add("sum");
    reservedKeywords.add("nvl");
    reservedKeywords.add("unique");
    reservedKeywords.add("except");
    reservedKeywords.add("declare");
    reservedKeywords.add("for");
    reservedKeywords.add("list");
    reservedKeywords.add("min");
    reservedKeywords.add("element");
    reservedKeywords.add("false");
    reservedKeywords.add("abs");
    reservedKeywords.add("true");
    reservedKeywords.add("bag");
    reservedKeywords.add("time");
    reservedKeywords.add("define");
    reservedKeywords.add("and");
    reservedKeywords.add("asc");
    reservedKeywords.add("desc");
    reservedKeywords.add("select");
    reservedKeywords.add("intersect");
    reservedKeywords.add("flatten");
    reservedKeywords.add("float");
    reservedKeywords.add("import");
    reservedKeywords.add("exists");
    reservedKeywords.add("distinct");
    reservedKeywords.add("boolean");
    reservedKeywords.add("string");
    reservedKeywords.add("group");
    reservedKeywords.add("interval");
    reservedKeywords.add("orelse");
    reservedKeywords.add("where");
    reservedKeywords.add("trace");
    reservedKeywords.add("first");
    reservedKeywords.add("set");
    reservedKeywords.add("octet");
    reservedKeywords.add("nil");
    reservedKeywords.add("avg");
    reservedKeywords.add("order");
    reservedKeywords.add("long");
    reservedKeywords.add("limit");
    reservedKeywords.add("mod");
    reservedKeywords.add("type");
    reservedKeywords.add("undefine");
    reservedKeywords.add("in");
    reservedKeywords.add("null");
    reservedKeywords.add("some");
    reservedKeywords.add("to_date");
    reservedKeywords.add("short");
    reservedKeywords.add("enum");
    reservedKeywords.add("timestamp");
    reservedKeywords.add("having");
    reservedKeywords.add("dictionary");
    reservedKeywords.add("char");
    reservedKeywords.add("listtoset");
    reservedKeywords.add("array");
    reservedKeywords.add("union");
    reservedKeywords.add("or");
    reservedKeywords.add("max");
    reservedKeywords.add("from");
    reservedKeywords.add("query");
    reservedKeywords.add("collection");
    reservedKeywords.add("like");
    reservedKeywords.add("date");
    reservedKeywords.add("byte");
    reservedKeywords.add("any");
    reservedKeywords.add("is_undefined");
    reservedKeywords.add("double");
    reservedKeywords.add("int");
    reservedKeywords.add("andthen");
    reservedKeywords.add("last");
    reservedKeywords.add("struct");
    reservedKeywords.add("undefined");
    reservedKeywords.add("is_defined");
    reservedKeywords.add("not");
    reservedKeywords.add("by");
    reservedKeywords.add("as");
  }

  /**
   * Caches the fields not found in any Pdx version. This threadlocal will be cleaned up after query
   * execution completes in {@linkplain #executeUsingContext(ExecutionContext)}
   */
  private static final ThreadLocal<Map<String, Set<String>>> pdxClassToFieldsMap =
      new ThreadLocal() {
        @Override
        protected Map<String, Set<String>> initialValue() {
          return new HashMap<>();
        }
      };

  public static Map<String, Set<String>> getPdxClasstofieldsmap() {
    return pdxClassToFieldsMap.get();
  }


  /**
   * Caches the methods not found in any Pdx version. This threadlocal will be cleaned up after
   * query execution completes in {@linkplain #executeUsingContext(ExecutionContext)}
   */
  private static final ThreadLocal<Map<String, Set<String>>> pdxClassToMethodsMap =
      new ThreadLocal() {
        @Override
        protected Map<String, Set<String>> initialValue() {
          return new HashMap<String, Set<String>>();
        }
      };

  public static void setPdxClasstoMethodsmap(Map<String, Set<String>> map) {
    pdxClassToMethodsMap.set(map);
  }

  public static Map<String, Set<String>> getPdxClasstoMethodsmap() {
    return pdxClassToMethodsMap.get();
  }


  /**
   * Should be constructed from DefaultQueryService
   * 
   * @see QueryService#newQuery
   */
  public DefaultQuery(String queryString, InternalCache cache, boolean isForRemote) {
    this.queryString = queryString;
    QCompiler compiler = new QCompiler();
    this.compiledQuery = compiler.compileQuery(queryString);
    CompiledSelect cs = getSimpleSelect();
    if (cs != null && !isForRemote && (cs.isGroupBy() || cs.isOrderBy())) {
      QueryExecutionContext ctx = new QueryExecutionContext(null, cache);
      try {
        cs.computeDependencies(ctx);
      } catch (QueryException qe) {
        throw new QueryInvalidException("", qe);
      }
    }
    this.traceOn = compiler.isTraceRequested() || QUERY_VERBOSE;
    this.cache = cache;
    this.stats = new DefaultQueryStatistics();
  }

  public static boolean getPdxReadSerialized() {
    return pdxReadSerialized.get();
  }

  public static void setPdxReadSerialized(boolean readSerialized) {
    pdxReadSerialized.set(readSerialized);
  }

  /**
   * helper method for setPdxReadSerialized
   */
  public static void setPdxReadSerialized(Cache cache, boolean readSerialized) {
    if (cache != null && !cache.getPdxReadSerialized()) {
      setPdxReadSerialized(readSerialized);
    }
  }

  /**
   * Get statistics information for this query.
   */
  @Override
  public QueryStatistics getStatistics() {
    return this.stats;
  }

  @Override
  public String getQueryString() {
    return this.queryString;
  }

  @Override
  public Object execute() throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    return execute(EMPTY_ARRAY);
  }

  /**
   * namespace or parameters can be null
   */
  @Override
  public Object execute(Object[] params) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {

    // Local Query.
    if (params == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.DefaultQuery_PARAMETERS_CANNOT_BE_NULL.toLocalizedString());
    }

    // If pool is associated with the Query; execute the query on pool. ServerSide query.
    if (this.serverProxy != null) {
      // Execute Query using pool.
      return executeOnServer(params);
    }

    long startTime = 0L;
    if (this.traceOn && this.cache != null) {
      startTime = NanoTimer.getTime();
    }

    QueryObserver indexObserver = null;
    QueryMonitor queryMonitor = null;
    QueryExecutor qe = checkQueryOnPR(params);

    Object result = null;
    try {
      // Setting the readSerialized flag for local queries
      setPdxReadSerialized(this.cache, true);
      ExecutionContext context = new QueryExecutionContext(params, this.cache, this);
      indexObserver = this.startTrace();
      if (qe != null) {
        if (DefaultQuery.testHook != null) {
          DefaultQuery.testHook.doTestHook(1);
        }

        result = qe.executeQuery(this, params, null);
        // For local queries returning pdx objects wrap the resultset with
        // ResultsCollectionPdxDeserializerWrapper
        // which deserializes these pdx objects.
        if (needsPDXDeserializationWrapper(true /* is query on PR */)
            && result instanceof SelectResults) {
          // we use copy on read false here because the copying has already taken effect earlier in
          // the PartitionedRegionQueryEvaluator
          result = new ResultsCollectionPdxDeserializerWrapper((SelectResults) result, false);
        }
        return result;
      }

      queryMonitor = this.cache.getQueryMonitor();

      // If QueryMonitor is enabled add query to be monitored.
      if (queryMonitor != null) {
        // Add current thread to be monitored by QueryMonitor.
        // In case of partitioned region it will be added before the query execution
        // starts on the Local Buckets.
        queryMonitor.monitorQueryThread(Thread.currentThread(), this);
      }

      context.setCqQueryContext(this.isCqQuery);
      result = executeUsingContext(context);
      // Only wrap/copy results when copy on read is set and an index is used
      // This is because when an index is used, the results are actual references to values in the
      // cache
      // Currently as 7.0.1 when indexes are not used, iteration uses non tx entries to retrieve the
      // value.
      // The non tx entry already checks copy on read and returns a copy.
      // We only wrap actual results and not UNDEFINED.

      // Takes into consideration that isRemoteQuery is already being checked with the if checks
      // this flag is true if copy on read is set to true and we are copying at the entry level for
      // queries is set to false (default)
      // OR copy on read is true and we used an index where copy on entry level for queries is set
      // to true.
      // Due to bug#46970 index usage does not actually copy at the entry level so that is why we
      // have the OR condition
      boolean needsCopyOnReadWrapper =
          this.cache.getCopyOnRead() && !DefaultQueryService.COPY_ON_READ_AT_ENTRY_LEVEL
              || (((QueryExecutionContext) context).isIndexUsed()
                  && DefaultQueryService.COPY_ON_READ_AT_ENTRY_LEVEL);
      // For local queries returning pdx objects wrap the resultset with
      // ResultsCollectionPdxDeserializerWrapper
      // which deserializes these pdx objects.
      if (needsPDXDeserializationWrapper(false /* is query on PR */)
          && result instanceof SelectResults) {
        result = new ResultsCollectionPdxDeserializerWrapper((SelectResults) result,
            needsCopyOnReadWrapper);
      } else if (!isRemoteQuery() && this.cache.getCopyOnRead()
          && result instanceof SelectResults) {
        if (needsCopyOnReadWrapper) {
          result = new ResultsCollectionCopyOnReadWrapper((SelectResults) result);
        }
      }
      return result;
    } catch (QueryExecutionCanceledException ignore) {
      // query execution canceled exception will be thrown from the QueryMonitor
      // canceled exception should not be null at this point as it should be set
      // when query is canceled.
      if (this.canceledException != null) {
        throw this.canceledException;
      } else {
        throw new QueryExecutionCanceledException(
            "Query was canceled. It may be due to low memory or the query was running longer than the MAX_QUERY_EXECUTION_TIME.");
      }
    } finally {
      setPdxReadSerialized(this.cache, false);
      if (queryMonitor != null) {
        queryMonitor.stopMonitoringQueryThread(Thread.currentThread(), this);
      }
      this.endTrace(indexObserver, startTime, result);
    }
  }

  /**
   * For Order by queries ,since they are already ordered by the comparator && it takes care of
   * conversion, we do not have to wrap it in a wrapper
   */
  private boolean needsPDXDeserializationWrapper(boolean isQueryOnPR) {
    return !isRemoteQuery() && !this.cache.getPdxReadSerialized();
  }

  private Object executeOnServer(Object[] parameters) {
    long startTime = CachePerfStats.getStatTime();
    Object result = null;
    try {
      if (this.proxyCache != null) {
        if (this.proxyCache.isClosed()) {
          throw new CacheClosedException("Cache is closed for this user.");
        }
        UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
      }
      result = this.serverProxy.query(this.queryString, parameters);
    } finally {
      UserAttributes.userAttributes.set(null);
      long endTime = CachePerfStats.getStatTime();
      updateStatistics(endTime - startTime);
    }
    return result;
  }

  /**
   * Execute a PR Query on the specified bucket. Assumes query already meets restrictions for PR
   * Query, and the first iterator in the FROM clause can be replaced with the BucketRegion.
   */
  public Object prExecuteOnBucket(Object[] parameters, PartitionedRegion pr, BucketRegion bukRgn)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    if (parameters == null) {
      parameters = EMPTY_ARRAY;
    }

    long startTime = 0L;
    if (this.traceOn && this.cache != null) {
      startTime = NanoTimer.getTime();
    }

    IndexTrackingQueryObserver indexObserver = null;
    String otherObserver = null;
    if (this.traceOn) {
      QueryObserver qo = QueryObserverHolder.getInstance();
      if (qo instanceof IndexTrackingQueryObserver) {
        indexObserver = (IndexTrackingQueryObserver) qo;
      } else if (!QueryObserverHolder.hasObserver()) {
        indexObserver = new IndexTrackingQueryObserver();
        QueryObserverHolder.setInstance(indexObserver);
      } else {
        otherObserver = qo.getClass().getName();
      }
    }

    ExecutionContext context = new QueryExecutionContext(parameters, this.cache, this);
    context.setBucketRegion(pr, bukRgn);
    context.setCqQueryContext(this.isCqQuery);

    // Check if QueryMonitor is enabled, if enabled add query to be monitored.
    QueryMonitor queryMonitor = this.cache.getQueryMonitor();

    // PRQueryProcessor executes the query using single thread(in-line) or ThreadPool.
    // In case of threadPool each individual threads needs to be added into
    // QueryMonitor Service.
    if (queryMonitor != null && PRQueryProcessor.NUM_THREADS > 1) {
      // Add current thread to be monitored by QueryMonitor.
      queryMonitor.monitorQueryThread(Thread.currentThread(), this);
    }

    Object result = null;
    try {
      result = executeUsingContext(context);
    } finally {
      if (queryMonitor != null && PRQueryProcessor.NUM_THREADS > 1) {
        queryMonitor.stopMonitoringQueryThread(Thread.currentThread(), this);
      }

      int resultSize = 0;
      if (this.traceOn) {
        if (result instanceof Collection) {
          resultSize = ((Collection) result).size();
        }
      }

      String queryVerboseMsg = DefaultQuery.getLogMessage(indexObserver, startTime, otherObserver,
          resultSize, this.queryString, bukRgn);

      if (this.traceOn) {
        if (this.cache.getLogger().fineEnabled()) {
          this.cache.getLogger().fine(queryVerboseMsg);
        }
      }
    }
    return result;
  }

  public Object executeUsingContext(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    QueryObserver observer = QueryObserverHolder.getInstance();

    long startTime = CachePerfStats.getStatTime();
    TXStateProxy tx = ((TXManagerImpl) this.cache.getCacheTransactionManager()).internalSuspend();
    try {
      observer.startQuery(this);
      observer.beforeQueryEvaluation(this.compiledQuery, context);

      if (DefaultQuery.testHook != null) {
        DefaultQuery.testHook.doTestHook(6);
      }
      Object results = null;
      try {
        // two-pass evaluation.
        // first pre-compute dependencies, cached in the context.
        this.compiledQuery.computeDependencies(context);
        if (testHook != null) {
          testHook.doTestHook(1);
        }
        results = this.compiledQuery.evaluate(context);
      } catch (QueryExecutionCanceledException ignore) {
        // query execution canceled exception will be thrown from the QueryMonitor
        // canceled exception should not be null at this point as it should be set
        // when query is canceled.
        if (this.canceledException != null) {
          throw this.canceledException;
        } else {
          throw new QueryExecutionCanceledException(
              "Query was canceled. It may be due to low memory or the query was running longer than the MAX_QUERY_EXECUTION_TIME.");
        }
      } finally {
        observer.afterQueryEvaluation(results);
      }
      return results;
    } finally {
      observer.endQuery();
      long endTime = CachePerfStats.getStatTime();
      updateStatistics(endTime - startTime);
      pdxClassToFieldsMap.remove();
      pdxClassToMethodsMap.remove();
      ((TXManagerImpl) this.cache.getCacheTransactionManager()).internalResume(tx);
    }
  }

  private QueryExecutor checkQueryOnPR(Object[] parameters)
      throws RegionNotFoundException, PartitionOfflineException {

    // check for PartitionedRegions. If a PartitionedRegion is referred to in the query,
    // then the following restrictions apply:
    // 1) the query must be just a SELECT expression; (preceded by zero or more IMPORT statements)
    // 2) the first FROM clause iterator cannot contain a subquery;
    // 3) PR reference can only be in the first FROM clause

    List<QueryExecutor> prs = new ArrayList<>();
    for (final Object o : getRegionsInQuery(parameters)) {
      String regionPath = (String) o;
      Region rgn = this.cache.getRegion(regionPath);
      if (rgn == null) {
        this.cache.getCancelCriterion().checkCancelInProgress(null);
        throw new RegionNotFoundException(
            LocalizedStrings.DefaultQuery_REGION_NOT_FOUND_0.toLocalizedString(regionPath));
      }
      if (rgn instanceof QueryExecutor) {
        ((PartitionedRegion) rgn).checkPROffline();
        prs.add((QueryExecutor) rgn);
      }
    }
    if (prs.size() == 1) {
      return prs.get(0);
    } else if (prs.size() > 1) {
      // colocation checks; valid for more the one PRs

      // First query has to be executed in a Function.
      if (!this.isQueryWithFunctionContext()) {
        throw new UnsupportedOperationException(
            LocalizedStrings.DefaultQuery_A_QUERY_ON_A_PARTITIONED_REGION_0_MAY_NOT_REFERENCE_ANY_OTHER_REGION_1
                .toLocalizedString(new Object[] {prs.get(0).getName(), prs.get(1).getName()}));
      }

      // If there are more than one PRs they have to be co-located.
      QueryExecutor other = null;
      for (QueryExecutor eachPR : prs) {
        boolean colocated = false;

        for (QueryExecutor allPRs : prs) {
          if (eachPR == allPRs) {
            continue;
          }
          other = allPRs;
          if (((PartitionedRegion) eachPR).getColocatedByList().contains(allPRs)
              || ((PartitionedRegion) allPRs).getColocatedByList().contains(eachPR)) {
            colocated = true;
            break;
          }
        } // allPrs

        if (!colocated) {
          throw new UnsupportedOperationException(
              LocalizedStrings.DefaultQuery_A_QUERY_ON_A_PARTITIONED_REGION_0_MAY_NOT_REFERENCE_ANY_OTHER_NON_COLOCATED_PARTITIONED_REGION_1
                  .toLocalizedString(new Object[] {eachPR.getName(), other.getName()}));
        }

      } // eachPR

      // this is a query on a PR, check to make sure it is only a SELECT
      CompiledSelect select = getSimpleSelect();
      if (select == null) {
        throw new UnsupportedOperationException(
            LocalizedStrings.DefaultQuery_QUERY_MUST_BE_A_SIMPLE_SELECT_WHEN_REFERENCING_A_PARTITIONED_REGION
                .toLocalizedString());
      }

      // make sure the where clause references no regions
      Set regions = new HashSet();
      CompiledValue whereClause = select.getWhereClause();
      if (whereClause != null) {
        whereClause.getRegionsInQuery(regions, parameters);
        if (!regions.isEmpty()) {
          throw new UnsupportedOperationException(
              LocalizedStrings.DefaultQuery_THE_WHERE_CLAUSE_CANNOT_REFER_TO_A_REGION_WHEN_QUERYING_ON_A_PARTITIONED_REGION
                  .toLocalizedString());
        }
      }
      List fromClause = select.getIterators();

      // the first iterator in the FROM clause must be just a reference to the Partitioned Region
      Iterator fromClauseIterator = fromClause.iterator();
      CompiledIteratorDef itrDef = (CompiledIteratorDef) fromClauseIterator.next();

      // By process of elimination, we know that the first iterator contains a reference
      // to the PR. Check to make sure there are no subqueries in this first iterator
      itrDef.visitNodes(new CompiledValue.NodeVisitor() {
        @Override
        public boolean visit(CompiledValue node) {
          if (node instanceof CompiledSelect) {
            throw new UnsupportedOperationException(
                LocalizedStrings.DefaultQuery_WHEN_QUERYING_A_PARTITIONEDREGION_THE_FIRST_FROM_CLAUSE_ITERATOR_MUST_NOT_CONTAIN_A_SUBQUERY
                    .toLocalizedString());
          }
          return true;
        }
      });

      // the rest of the FROM clause iterators must not reference any regions
      if (!this.isQueryWithFunctionContext()) {
        while (fromClauseIterator.hasNext()) {
          itrDef = (CompiledIteratorDef) fromClauseIterator.next();
          itrDef.getRegionsInQuery(regions, parameters);
          if (!regions.isEmpty()) {
            throw new UnsupportedOperationException(
                LocalizedStrings.DefaultQuery_WHEN_QUERYING_A_PARTITIONED_REGION_THE_FROM_CLAUSE_ITERATORS_OTHER_THAN_THE_FIRST_ONE_MUST_NOT_REFERENCE_ANY_REGIONS
                    .toLocalizedString());
          }
        }

        // check the projections, must not reference any regions
        List projs = select.getProjectionAttributes();
        if (projs != null) {
          for (Object proj1 : projs) {
            Object[] rawProj = (Object[]) proj1;
            CompiledValue proj = (CompiledValue) rawProj[1];
            proj.getRegionsInQuery(regions, parameters);
            if (!regions.isEmpty()) {
              throw new UnsupportedOperationException(
                  LocalizedStrings.DefaultQuery_WHEN_QUERYING_A_PARTITIONED_REGION_THE_PROJECTIONS_MUST_NOT_REFERENCE_ANY_REGIONS
                      .toLocalizedString());
            }
          }
        }
        // check the orderByAttrs, must not reference any regions
        List<CompiledSortCriterion> orderBys = select.getOrderByAttrs();
        if (orderBys != null) {
          for (CompiledSortCriterion orderBy : orderBys) {
            orderBy.getRegionsInQuery(regions, parameters);
            if (!regions.isEmpty()) {
              throw new UnsupportedOperationException(
                  LocalizedStrings.DefaultQuery_WHEN_QUERYING_A_PARTITIONED_REGION_THE_ORDERBY_ATTRIBUTES_MUST_NOT_REFERENCE_ANY_REGIONS
                      .toLocalizedString());
            }
          }
        }
      }
      return prs.get(0); // PR query is okay
    }
    return null;
  }

  private void updateStatistics(long executionTime) {
    this.numExecutions.incrementAndGet();
    this.totalExecutionTime.addAndGet(executionTime);
    this.cache.getCachePerfStats().endQueryExecution(executionTime);
  }

  // TODO: Implement the function. Toggle the isCompiled flag accordingly
  @Override
  public void compile() throws TypeMismatchException, NameResolutionException {
    throw new UnsupportedOperationException(
        LocalizedStrings.DefaultQuery_NOT_YET_IMPLEMENTED.toLocalizedString());
  }

  @Override
  public boolean isCompiled() {
    return false;
  }

  public boolean isTraced() {
    return this.traceOn;
  }

  class DefaultQueryStatistics implements QueryStatistics {

    /**
     * Returns the total amount of time (in nanoseconds) spent executing the query.
     */
    @Override
    public long getTotalExecutionTime() {
      return DefaultQuery.this.totalExecutionTime.get();
    }

    /**
     * Returns the total number of times the query has been executed.
     */
    @Override
    public long getNumExecutions() {
      return DefaultQuery.this.numExecutions.get();
    }
  }

  /**
   * Returns an unmodifiable Set containing the Region names which are present in the Query. A
   * region which is associated with the query as a bind parameter will not be included in the list.
   * The Region names returned by the query do not indicate anything about the state of the region
   * or whether the region actually exists in the GemfireCache etc.
   *
   * @param parameters the parameters to be passed in to the query when executed
   * @return Unmodifiable List containing the region names.
   */
  public Set getRegionsInQuery(Object[] parameters) {
    Set regions = new HashSet();
    this.compiledQuery.getRegionsInQuery(regions, parameters);
    return Collections.unmodifiableSet(regions);
  }

  /**
   * Returns the CompiledSelect if this query consists of only a SELECT expression (possibly with
   * IMPORTS as well). Otherwise, returns null
   */
  public CompiledSelect getSimpleSelect() {
    if (this.compiledQuery instanceof CompiledSelect) {
      return (CompiledSelect) this.compiledQuery;
    }
    return null;
  }

  public CompiledSelect getSelect() {
    return (CompiledSelect) this.compiledQuery;
  }

  /**
   * @return int identifying the limit. A value of -1 indicates that no limit is imposed or the
   *         query is not a select query
   */
  public int getLimit(Object[] bindArguments) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    return this.compiledQuery instanceof CompiledSelect
        ? ((CompiledSelect) this.compiledQuery).getLimitValue(bindArguments) : -1;
  }

  void setServerProxy(ServerProxy serverProxy) {
    this.serverProxy = serverProxy;
  }

  /**
   * Check to see if the query execution got canceled. The query gets canceled by the QueryMonitor
   * if it takes more than the max query execution time or low memory situations
   */
  public boolean isCanceled() {
    return this.isCanceled;
  }

  public CacheRuntimeException getQueryCanceledException() {
    return this.canceledException;
  }

  boolean[] getQueryCompletedForMonitoring() {
    return this.queryCompletedForMonitoring;
  }

  // TODO: parameter value is always true
  void setQueryCompletedForMonitoring(boolean value) {
    this.queryCompletedForMonitoring[0] = value;
  }

  /**
   * The query gets canceled by the QueryMonitor with the reason being specified
   * <p>
   * TODO: parameter isCanceled is always true
   */
  public void setCanceled(boolean isCanceled, CacheRuntimeException canceledException) {
    this.isCanceled = isCanceled;
    this.canceledException = canceledException;
  }

  public void setIsCqQuery(boolean isCqQuery) {
    this.isCqQuery = isCqQuery;
  }

  public boolean isCqQuery() {
    return this.isCqQuery;
  }

  public void setCqQuery(InternalCqQuery cqQuery) {
    this.cqQuery = cqQuery;
  }

  public void setLastUsed(boolean lastUsed) {
    this.lastUsed = lastUsed;
  }

  public boolean getLastUsed() {
    return this.lastUsed;
  }

  public InternalCqQuery getCqQuery() {
    return this.cqQuery;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Query String = ");
    sb.append(this.queryString);
    sb.append(';');
    sb.append("isCancelled = ");
    sb.append(this.isCanceled);
    sb.append("; Total Executions = ");
    sb.append(this.numExecutions);
    sb.append("; Total Execution Time = ");
    sb.append(this.totalExecutionTime);
    return sb.toString();
  }

  void setProxyCache(ProxyCache proxyCache) {
    this.proxyCache = proxyCache;
  }

  /**
   * Used for test purpose.
   */
  public static void setTestCompiledQueryClearTime(int val) {
    DefaultQuery.TEST_COMPILED_QUERY_CLEAR_TIME = val;
  }

  private static String getLogMessage(QueryObserver observer, long startTime, int resultSize,
      String query) {
    float time = (NanoTimer.getTime() - startTime) / 1.0e6f;

    String usedIndexesString = null;
    if (observer != null && observer instanceof IndexTrackingQueryObserver) {
      IndexTrackingQueryObserver indexObserver = (IndexTrackingQueryObserver) observer;
      Map usedIndexes = indexObserver.getUsedIndexes();
      indexObserver.reset();
      StringBuilder sb = new StringBuilder();
      sb.append(" indexesUsed(");
      sb.append(usedIndexes.size());
      sb.append(')');
      if (usedIndexes.size() > 0) {
        sb.append(':');
        for (Iterator itr = usedIndexes.entrySet().iterator(); itr.hasNext();) {
          Map.Entry entry = (Map.Entry) itr.next();
          sb.append(entry.getKey()).append(entry.getValue());
          if (itr.hasNext()) {
            sb.append(',');
          }
        }
      }
      usedIndexesString = sb.toString();
    } else if (DefaultQuery.QUERY_VERBOSE) {
      usedIndexesString = " indexesUsed(NA due to other observer in the way: "
          + observer.getClass().getName() + ')';
    }

    String rowCountString = null;
    if (resultSize != -1) {
      rowCountString = " rowCount = " + resultSize + ';';
    }
    return "Query Executed in " + time + " ms;" + (rowCountString != null ? rowCountString : "")
        + (usedIndexesString != null ? usedIndexesString : "") + " \"" + query + '"';
  }

  private static String getLogMessage(IndexTrackingQueryObserver indexObserver, long startTime,
      String otherObserver, int resultSize, String query, BucketRegion bucket) {
    float time = 0.0f;

    if (startTime > 0L) {
      time = (NanoTimer.getTime() - startTime) / 1.0e6f;
    }

    String usedIndexesString = null;
    if (indexObserver != null) {
      Map usedIndexes = indexObserver.getUsedIndexes(bucket.getFullPath());
      StringBuilder sb = new StringBuilder();
      sb.append(" indexesUsed(");
      sb.append(usedIndexes.size());
      sb.append(')');
      if (!usedIndexes.isEmpty()) {
        sb.append(':');
        for (Iterator itr = usedIndexes.entrySet().iterator(); itr.hasNext();) {
          Map.Entry entry = (Map.Entry) itr.next();
          sb.append(entry.getKey()).append("(Results: ").append(entry.getValue())
              .append(", Bucket: ").append(bucket.getId()).append(")");
          if (itr.hasNext()) {
            sb.append(',');
          }
        }
      }
      usedIndexesString = sb.toString();
    } else if (DefaultQuery.QUERY_VERBOSE) {
      usedIndexesString =
          " indexesUsed(NA due to other observer in the way: " + otherObserver + ')';
    }

    String rowCountString = " rowCount = " + resultSize + ';';
    return "Query Executed" + (startTime > 0L ? " in " + time + " ms;" : ";") + rowCountString
        + (usedIndexesString != null ? usedIndexesString : "") + " \"" + query + '"';
  }

  @Override
  public Object execute(RegionFunctionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    return execute(context, EMPTY_ARRAY);
  }

  @Override
  public Object execute(RegionFunctionContext context, Object[] params)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {

    // Supported only with RegionFunctionContext
    if (context == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.DefaultQuery_FUNCTIONCONTEXT_CANNOT_BE_NULL.toLocalizedString());
    }
    this.isQueryWithFunctionContext = true;

    if (params == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.DefaultQuery_PARAMETERS_CANNOT_BE_NULL.toLocalizedString());
    }

    long startTime = 0L;
    if (this.traceOn && this.cache != null) {
      startTime = NanoTimer.getTime();
    }

    QueryObserver indexObserver = null;
    QueryExecutor qe = checkQueryOnPR(params);

    Object result = null;
    try {
      indexObserver = startTrace();
      if (qe != null) {
        LocalDataSet localDataSet =
            (LocalDataSet) PartitionRegionHelper.getLocalDataForContext(context);
        Set<Integer> buckets = localDataSet.getBucketSet();
        result = qe.executeQuery(this, params, buckets);
        return result;
      } else {
        // Not supported on regions other than PartitionRegion.
        throw new IllegalArgumentException(
            LocalizedStrings.DefaultQuery_API_ONLY_FOR_PR.toLocalizedString());
      }

    } finally {
      this.endTrace(indexObserver, startTime, result);
    }
  }

  /**
   * For queries which are executed from a Function "with a Filter".
   * 
   * @return returns if this query is coming from a {@link Function}.
   */
  public boolean isQueryWithFunctionContext() {
    return this.isQueryWithFunctionContext;
  }

  public QueryObserver startTrace() {
    QueryObserver queryObserver = null;
    if (this.traceOn && this.cache != null) {

      QueryObserver qo = QueryObserverHolder.getInstance();
      if (qo instanceof IndexTrackingQueryObserver) {
        queryObserver = qo;
      } else if (!QueryObserverHolder.hasObserver()) {
        queryObserver = new IndexTrackingQueryObserver();
        QueryObserverHolder.setInstance(queryObserver);
      } else {
        queryObserver = qo;
      }
    }
    return queryObserver;
  }

  public void endTrace(QueryObserver indexObserver, long startTime, Object result) {
    if (this.traceOn && this.cache != null) {
      int resultSize = -1;

      if (result instanceof Collection) {
        resultSize = ((Collection) result).size();
      }

      String queryVerboseMsg =
          DefaultQuery.getLogMessage(indexObserver, startTime, resultSize, this.queryString);
      this.cache.getLogger().info(queryVerboseMsg);
    }
  }

  public void endTrace(QueryObserver indexObserver, long startTime, Collection<Collection> result) {
    if (this.cache != null && this.cache.getLogger().infoEnabled() && this.traceOn) {
      int resultSize = 0;

      for (Collection aResult : result) {
        resultSize += aResult.size();
      }

      String queryVerboseMsg =
          DefaultQuery.getLogMessage(indexObserver, startTime, resultSize, this.queryString);
      if (this.cache.getLogger().infoEnabled()) {
        this.cache.getLogger().info(queryVerboseMsg);
      }
    }
  }

  public boolean isRemoteQuery() {
    return this.isRemoteQuery;
  }

  public void setRemoteQuery(boolean isRemoteQuery) {
    this.isRemoteQuery = isRemoteQuery;
  }

  /**
   * set keepSerialized flag for remote queries of type 'select *' having independent operators
   */
  void keepResultsSerialized(CompiledSelect cs, ExecutionContext context) {
    if (isRemoteQuery()) {
      // for dependent iterators, deserialization is required
      if (cs.getIterators().size() == context.getAllIndependentIteratorsOfCurrentScope().size()
          && cs.getWhereClause() == null && cs.getProjectionAttributes() == null && !cs.isDistinct()
          && cs.getOrderByAttrs() == null) {
        setKeepSerialized();
      }
    }
  }

  public boolean isKeepSerialized() {
    return this.keepSerialized;
  }

  private void setKeepSerialized() {
    this.keepSerialized = true;
  }

  public interface TestHook {
    void doTestHook(int spot);

    void doTestHook(String spot);
  }
}
