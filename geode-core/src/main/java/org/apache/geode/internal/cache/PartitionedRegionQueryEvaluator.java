/*
 * /*
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CopyHelper;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.CompiledGroupBySelect;
import org.apache.geode.cache.query.internal.CompiledSelect;
import org.apache.geode.cache.query.internal.CompiledSortCriterion;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.CumulativeNonDistinctResults;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.IndexTrackingQueryObserver.IndexInfo;
import org.apache.geode.cache.query.internal.NWayMergeResults;
import org.apache.geode.cache.query.internal.OrderByComparator;
import org.apache.geode.cache.query.internal.PRQueryTraceInfo;
import org.apache.geode.cache.query.internal.QueryExecutionContext;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.ResultsSet;
import org.apache.geode.cache.query.internal.SortedResultsBag;
import org.apache.geode.cache.query.internal.SortedStructBag;
import org.apache.geode.cache.query.internal.StructSet;
import org.apache.geode.cache.query.internal.utils.PDXUtils;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.partitioned.PartitionMessage;
import org.apache.geode.internal.cache.partitioned.QueryMessage;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.partitioned.StreamingPartitionOperation;
import org.apache.geode.internal.logging.LogService;

/**
 * This class sends the query on various <code>PartitionedRegion</code> data store nodes and
 * collects the results back, does the union of all the results.
 *
 * revamped with streaming of results retry logic
 */
public class PartitionedRegionQueryEvaluator extends StreamingPartitionOperation {
  private static final Logger logger = LogService.getLogger();

  /**
   * An ArrayList which might be unconsumable.
   *
   * @since GemFire 6.6.2
   */
  public static class MemberResultsList extends ArrayList {
    private boolean isLastChunkReceived = false;

    public boolean isLastChunkReceived() {
      return isLastChunkReceived;
    }

    public void setLastChunkReceived(boolean isLastChunkReceived) {
      this.isLastChunkReceived = isLastChunkReceived;
    }
  }

  /**
   * Simple testing interface
   *
   * @since GemFire 6.0
   */
  public interface TestHook {
    void hook(final int spot) throws RuntimeException;
  }

  private static final int MAX_PR_QUERY_RETRIES =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "MAX_PR_QUERY_RETRIES", 10).intValue();

  private final PartitionedRegion pr;
  private volatile Map<InternalDistributedMember, List<Integer>> node2bucketIds;
  private final DefaultQuery query;
  private final Object[] parameters;
  private SelectResults cumulativeResults;
  /**
   * Member to result map, with member as key and values are collection of query results. The value
   * collection is collection of SelectResults (local query) or Collection of Lists (from remote
   * queries).
   */
  private final ConcurrentMap<InternalDistributedMember, Collection<Collection>> resultsPerMember;
  private ConcurrentLinkedQueue<PRQueryTraceInfo> prQueryTraceInfoList = null;
  private final Set<Integer> bucketsToQuery;
  private final IntOpenHashSet successfulBuckets;
  // set of members failed to execute query
  private Set<InternalDistributedMember> failedMembers;

  /**
   * Construct a PartitionedRegionQueryEvaluator
   *
   * @param sys the distributed system
   * @param pr the partitioned region
   * @param query the query
   * @param parameters the parameters for executing the query
   * @param cumulativeResults where to add the results as they come in
   */
  public PartitionedRegionQueryEvaluator(InternalDistributedSystem sys, PartitionedRegion pr,
      DefaultQuery query, Object[] parameters, SelectResults cumulativeResults,
      Set<Integer> bucketsToQuery) {
    super(sys, pr.getPRId());
    this.pr = pr;
    this.query = query;
    this.parameters = parameters;
    this.cumulativeResults = cumulativeResults;
    this.bucketsToQuery = bucketsToQuery;
    this.successfulBuckets = new IntOpenHashSet(this.bucketsToQuery.size());
    this.resultsPerMember =
        new ConcurrentHashMap<InternalDistributedMember, Collection<Collection>>();
    this.node2bucketIds = Collections.emptyMap();
    if (query != null && query.isTraced()) {
      prQueryTraceInfoList = new ConcurrentLinkedQueue();
    }
  }

  @Override
  protected DistributionMessage createRequestMessage(Set recipients, ReplyProcessor21 processor) {
    throw new UnsupportedOperationException();
  }

  protected PartitionMessage createRequestMessage(InternalDistributedMember recipient,
      ReplyProcessor21 processor, List bucketIds) {
    return new QueryMessage(recipient, this.pr.getPRId(), processor, this.query, this.parameters,
        bucketIds);
  }


  /**
   * @return false to abort
   */
  @Override
  protected boolean processData(List objects, InternalDistributedMember sender, int sequenceNum,
      boolean lastInSequence) {
    // check if sender is pre gfe_90. In that case the results coming from them are not sorted
    // we will have to sort it
    boolean sortNeeded = false;
    List<CompiledSortCriterion> orderByAttribs = null;
    if (sender.getVersionObject().compareTo(Version.GFE_90) < 0) {
      CompiledSelect cs = this.query.getSimpleSelect();
      if (cs != null && cs.isOrderBy()) {
        sortNeeded = true;
        orderByAttribs = cs.getOrderByAttrs();
      }


    }
    Collection results = this.resultsPerMember.get(sender);
    if (results == null) {
      synchronized (this.resultsPerMember) {
        results = this.resultsPerMember.get(sender);
        if (results == null) {
          results = new MemberResultsList();
          this.resultsPerMember.put(sender, results);
        }
      }
    }

    // We cannot do an if check for trace objects because it is possible
    // that a remote node has system Query.VERBOSE flag on
    // and yet the executing node does not.
    // We have to be sure to pull all trace infos out and not pull results
    if (objects.size() > 0) {
      Object traceObject = objects.get(0);
      if (traceObject instanceof PRQueryTraceInfo) {
        if (DefaultQuery.testHook != null) {
          DefaultQuery.testHook
              .doTestHook(DefaultQuery.TestHook.SPOTS.PULL_OFF_PR_QUERY_TRACE_INFO, null);
        }
        PRQueryTraceInfo queryTrace = (PRQueryTraceInfo) objects.remove(0);
        queryTrace.setSender(sender);
        if (prQueryTraceInfoList != null) {
          prQueryTraceInfoList.add(queryTrace);
        }
      }
    }
    // Only way objects is null is if we are a QUERY_MSG_TYPE and the msg was canceled, that is set
    // to true if objects were dropped due to low memory
    if (logger.isDebugEnabled()) {
      logger.debug("Results per member, for {} size: {}", sender, objects.size());
    }
    if (sortNeeded) {
      objects = sortIncomingData(objects, orderByAttribs);
    }

    synchronized (results) {
      if (!QueryMonitor.isLowMemory() && !this.query.isCanceled()) {
        results.add(objects);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("query canceled while gathering results, aborting");
        }
        if (QueryMonitor.isLowMemory()) {
          String reason =
              "Query execution canceled due to low memory while gathering results from partitioned regions";
          query.setQueryCanceledException(new QueryExecutionLowMemoryException(reason));
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("query cancelled while gathering results, aborting due to exception "
                + query.getQueryCanceledException());
          }
        }
        return false;
      }

      if (lastInSequence) {
        ((MemberResultsList) results).setLastChunkReceived(true);
      }
    }

    return true;
  }

  // TODO Asif: optimize it by creating a Sorted SelectResults Object at the time of fromData , so
  // that processData already receives ordered data.
  private List sortIncomingData(List objects, List<CompiledSortCriterion> orderByAttribs) {
    ObjectType resultType = cumulativeResults.getCollectionType().getElementType();
    ExecutionContext local = new ExecutionContext(null, this.pr.cache);
    Comparator comparator = new OrderByComparator(orderByAttribs, resultType, local);
    boolean nullAtStart = !orderByAttribs.get(0).getCriterion();
    final SelectResults newResults;
    // Asif: There is a bug in the versions < 9.0, such that the struct results coming from the
    // bucket nodes , do not contain approrpiate ObjectTypes. All the projection fields have
    // have the types as ObjectType. The resultset being created here has the right more selective
    // type.
    // so the addition of objects throw exception due to type mismatch. To handle this problem,
    // instead
    // of adding the struct objects as is, add fieldValues.
    if (resultType != null && resultType.isStructType()) {
      SortedStructBag sortedStructBag =
          new SortedStructBag(comparator, (StructType) resultType, nullAtStart);
      for (Object o : objects) {
        Struct s = (Struct) o;
        sortedStructBag.addFieldValues(s.getFieldValues());
      }
      newResults = sortedStructBag;
    } else {
      newResults = new SortedResultsBag(comparator, resultType, nullAtStart);
      newResults.addAll(objects);
    }


    objects = newResults.asList();
    return objects;
  }


  /**
   * Returns normally if succeeded to get data, otherwise throws an exception
   *
   * @param th a test hook
   * @return true if parts of the query need to be retried, otherwise false
   */
  public boolean executeQueryOnRemoteAndLocalNodes(final TestHook th)
      throws InterruptedException, QueryException {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    HashMap<InternalDistributedMember, List<Integer>> n2b =
        new HashMap<InternalDistributedMember, List<Integer>>(this.node2bucketIds);
    n2b.remove(this.pr.getMyId());
    // Shobhit: IF query is originated from a Function and we found some buckets on
    // remote node we should throw exception mentioning data movement during function execution.
    // According to discussions we dont know if this is possible as buckets are not moved until
    // function execution is completed.
    if (this.query.isQueryWithFunctionContext() && !n2b.isEmpty()) {
      if (isDebugEnabled) {
        logger.debug("Remote buckets found for query executed in a Function.");
      }
      throw new QueryInvocationTargetException(
          "Data movement detected accross PartitionRegion nodes while executing the Query with function filter.");
    }

    if (isDebugEnabled) {
      logger.debug("Sending query execution request to {} remote members for the query:{}",
          n2b.size(), this.query.getQueryString());
    }
    StreamingQueryPartitionResponse processor = null;
    boolean requiresRetry = false;

    if (n2b.isEmpty()) {
      if (isDebugEnabled) {
        logger.debug("No remote members with buckets to query.");
      }
    } else {
      // send separate message to each recipient since each one has a
      // different list of bucket ids
      processor = createStreamingQueryPartitionResponse(this.sys, n2b);
      for (Iterator<Map.Entry<InternalDistributedMember, List<Integer>>> itr =
          n2b.entrySet().iterator(); itr.hasNext();) {
        Map.Entry<InternalDistributedMember, List<Integer>> me = itr.next();
        final InternalDistributedMember rcp = me.getKey();
        final List<Integer> bucketIds = me.getValue();
        PartitionMessage m = createRequestMessage(rcp, processor, bucketIds);
        m.setTransactionDistributed(this.sys.getCache().getTxManager().isDistributed());
        Set notReceivedMembers = sendMessage(m);
        if (th != null) {
          th.hook(4);
        }
        if (notReceivedMembers != null && !notReceivedMembers.isEmpty()) {
          requiresRetry = true;
          processor.removeFailedSenders(notReceivedMembers);
          if (isDebugEnabled) {
            logger.debug("Failed sending to members {} retry required", notReceivedMembers);
          }
        }
      }
      if (th != null) {
        th.hook(5);
      }

    }

    Throwable localFault = null;
    boolean localNeedsRetry = false;

    // Shobhit: Check if query is only for local buckets else return.
    if (this.node2bucketIds.containsKey(this.pr.getMyId())) {
      if (isDebugEnabled) {
        logger.debug("Started query execution on local data for query:{}",
            this.query.getQueryString());
      }

      try {
        localNeedsRetry = executeQueryOnLocalNode();
        if (th != null) {
          th.hook(0);
        }
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable t) {
        SystemFailure.checkFailure();
        // We have to wait for remote exceptions
        // if request was sent to remote nodes.
        localFault = t;
      }
    } else {
      if (isDebugEnabled) {
        logger.debug("No local buckets to query.");
      }
    }

    if (processor != null) {
      try {
        // should we allow this to timeout?
        failedMembers = processor.waitForCacheOrQueryException();
        for (InternalDistributedMember member : failedMembers) {
          memberStreamCorrupted(member);
        }
        requiresRetry |= !failedMembers.isEmpty();

        if (isDebugEnabled) {
          logger.debug("Following remote members failed {} and retry flag is set to: {}",
              failedMembers, requiresRetry);
        }
      } catch (org.apache.geode.cache.TimeoutException e) { // Shobhit: Swallow remote exception if
                                                            // local exception is there.
        if (localFault == null) {
          throw new QueryException(e);
        }
      } catch (ReplyException e) {
        if (localFault == null) {
          throw e;
        }
      } catch (Error e) {
        if (localFault == null) {
          throw e;
        }
      } catch (RuntimeException e) {
        if (localFault == null) {
          throw e;
        }
      }
    }

    if (query.isCanceled()) {
      throw query.getQueryCanceledException();
    }

    if (localFault != null) {
      if (localFault instanceof QueryException) {
        throw (QueryException) localFault;
      } else if (localFault instanceof InterruptedException) {
        throw (InterruptedException) localFault;
      } else if (localFault instanceof Error) {
        throw (Error) localFault;
      } else if (localFault instanceof RuntimeException) {
        throw (RuntimeException) localFault;
      }
    }
    return requiresRetry | localNeedsRetry;
  }

  protected Set sendMessage(DistributionMessage m) {
    return this.sys.getDistributionManager().putOutgoing(m);
  }

  protected StreamingQueryPartitionResponse createStreamingQueryPartitionResponse(
      InternalDistributedSystem system, HashMap<InternalDistributedMember, List<Integer>> n2b) {
    return new StreamingQueryPartitionResponse(system, n2b.keySet());
  }


  /**
   * Executes a query over the provided buckets in a <code>PartitionedRegion</code>.
   *
   * This method will automatically retry the query on buckets on which problems where detected
   * during query processing.
   *
   * If there are no exceptions results are placed in the provided SelectResults instance
   *
   * @param th a hook used for testing purposes, for normal operation provide a null
   * @throws QueryException if data loss is detected during the query, when the number of retries
   *         has exceeded the system wide maximum, or when there are logic errors that cause bucket
   *         data to be omitted from the results.
   */
  public SelectResults queryBuckets(final TestHook th) throws QueryException, InterruptedException {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    if (isDebugEnabled) {
      logger.debug("PRQE query :{}", this.query.getQueryString());
    }
    Assert.assertTrue(!(this.bucketsToQuery == null || this.bucketsToQuery.isEmpty()),
        "bucket set is empty.");
    this.node2bucketIds = buildNodeToBucketMap();
    Assert.assertTrue(!this.node2bucketIds.isEmpty(),
        " There are no data stores hosting any of the buckets.");

    boolean needsRetry = true;
    int retry = 0;
    while (needsRetry && retry < MAX_PR_QUERY_RETRIES) {
      // Shobhit: Now on if buckets to be queried are on remote as well as local node,
      // request will be sent to remote node first to run query in parallel on local and
      // remote node.
      // Note: if Any Exception is thrown on local and some remote node, local exception
      // will be given priority and will be visible to query executor and remote exception
      // will be swallowed.
      needsRetry = executeQueryOnRemoteAndLocalNodes(th);
      if (th != null) {
        th.hook(1);
      }

      if (needsRetry) {
        // Shobhit: Only one chance is allowed for Function queries.
        if (query.isQueryWithFunctionContext()) {
          if (isDebugEnabled) {
            logger.debug("No of retry attempts are: {}", retry);
          }
          break;
        }
        Map b2n = buildNodeToBucketMapForBuckets(calculateRetryBuckets());
        if (th != null) {
          th.hook(2);
        }
        this.node2bucketIds = b2n;
        if (isDebugEnabled) {
          logger.debug("PR Query retry: {} total: {}", retry,
              this.pr.getCachePerfStats().getPRQueryRetries());
        }
        this.pr.getCachePerfStats().incPRQueryRetries();
        retry++;
        // Shobhit: Wait for sometime as rebalancing might be happening
        waitBeforeRetry();
      }
      if (th != null) {
        th.hook(3);
      }
    }

    if (needsRetry) {
      String msg = "Failed to query all the partitioned region " + "dataset (buckets) after "
          + retry + " attempts.";

      if (isDebugEnabled) {
        logger.debug("{} Unable to query some of the buckets from the set :{}", msg,
            this.calculateRetryBuckets());
      }
      throw new QueryException(msg);
    }

    return addResultsToResultSet();
  }

  /**
   * Wait for 10 ms between reattempts.
   */
  private void waitBeforeRetry() {
    boolean interrupted = Thread.interrupted();
    try {
      Thread.sleep(10);
    } catch (InterruptedException intEx) {
      interrupted = true;
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private Set<Integer> calculateRetryBuckets() {
    Iterator<Map.Entry<InternalDistributedMember, List<Integer>>> memberToBucketList =
        node2bucketIds.entrySet().iterator();
    final HashSet<Integer> retryBuckets = new HashSet<Integer>();
    while (memberToBucketList.hasNext()) {
      Map.Entry<InternalDistributedMember, List<Integer>> e = memberToBucketList.next();
      InternalDistributedMember m = e.getKey();
      if (!this.resultsPerMember.containsKey(m)
          || (!((MemberResultsList) this.resultsPerMember.get(m)).isLastChunkReceived())) {
        retryBuckets.addAll(e.getValue());
        this.resultsPerMember.remove(m);
      }
    }

    if (logger.isDebugEnabled()) {
      StringBuffer logStr = new StringBuffer();
      logStr.append("Query ").append(this.query.getQueryString())
          .append(" needs to retry bucketsIds: [");
      for (Integer i : retryBuckets) {
        logStr.append("," + i);
      }
      logStr.append("]");
      logger.debug(logStr);
    }

    return retryBuckets;
  }

  private SelectResults addResultsToResultSet() throws QueryException {
    int numElementsInResult = 0;

    boolean isDistinct = false;
    boolean isCount = false;

    int limit = -1; // -1 indicates no limit wsa specified in the query
    // passed as null. Not sure if it can happen in real life situation.
    // So instead of modifying test , using a null check in constructor
    CompiledSelect cs = null;



    if (this.query != null) {
      cs = this.query.getSimpleSelect();
      limit = this.query.getLimit(parameters);
      isDistinct = (cs != null) ? cs.isDistinct() : true;
      isCount = (cs != null) ? cs.isCount() : false;
    }

    if (isCount && !isDistinct) {
      addTotalCountForMemberToResults(limit);
      return this.cumulativeResults;
    }

    boolean isGroupByResults = cs.getType() == CompiledValue.GROUP_BY_SELECT;
    if (isGroupByResults) {
      SelectResults baseResults = null;
      CompiledGroupBySelect cgs = (CompiledGroupBySelect) cs;
      if (cgs.getOrderByAttrs() != null && !cgs.getOrderByAttrs().isEmpty()) {
        baseResults = this.buildSortedResult(cs, limit);
      } else {
        baseResults = this.buildCumulativeResults(isDistinct, limit);
      }
      ExecutionContext context = new ExecutionContext(null, pr.cache);
      context.setIsPRQueryNode(true);
      return cgs.applyAggregateAndGroupBy(baseResults, context);
    } else {

      if (this.cumulativeResults.getCollectionType().isOrdered() && cs.getOrderByAttrs() != null) {
        // If its a sorted result set, sort local and remote results using query.
        return buildSortedResult(cs, limit);
      } else {
        return buildCumulativeResults(isDistinct, limit);
      }
    }
  }

  private SelectResults buildCumulativeResults(boolean isDistinct, int limit) {
    // Indicates whether to check for PdxInstance and convert them to
    // domain object.
    // In case of local queries the domain objects are stored in the result set
    // for client/server queries PdxInstance are stored in result set.
    boolean getDomainObjectForPdx;
    // indicated results from remote nodes need to be deserialized
    // for local queries
    boolean getDeserializedObject = false;
    int numElementsInResult = 0;

    ObjectType elementType = this.cumulativeResults.getCollectionType().getElementType();
    boolean isStruct = elementType != null && elementType.isStructType();
    final DistributedMember me = this.pr.getMyId();

    if (DefaultQuery.testHook != null) {
      DefaultQuery.testHook
          .doTestHook(DefaultQuery.TestHook.SPOTS.BEFORE_BUILD_CUMULATIVE_RESULT, null);
    }

    boolean localResults = false;

    List<CumulativeNonDistinctResults.Metadata> collectionsMetadata = null;
    List<Collection> results = null;

    if (isDistinct) {
      if (isStruct) {
        StructType stype = (StructType) elementType;
        this.cumulativeResults = new StructSet(stype);
      } else {
        this.cumulativeResults = new ResultsSet(elementType);
      }
    } else {
      collectionsMetadata = new ArrayList<CumulativeNonDistinctResults.Metadata>();
      results = new ArrayList<Collection>();
    }

    for (Map.Entry<InternalDistributedMember, Collection<Collection>> e : this.resultsPerMember
        .entrySet()) {
      checkIfQueryShouldBeCancelled();
      // If its a local query, the results should contain domain objects.
      // in case of client/server query the objects from PdxInstances were
      // retrieved on the client side.
      if (e.getKey().equals(me)) {
        // In case of local node query results, the objects are already in
        // domain object form
        getDomainObjectForPdx = false;
        localResults = true;
        // for select * queries on local node return deserialized objects
      } else {
        // In case of remote nodes, the result objects are in PdxInstance form
        // get domain objects for local queries.
        getDomainObjectForPdx = !(this.pr.getCache().getPdxReadSerializedByAnyGemFireServices());
        // In case of select * without where clause the results from remote
        // nodes are sent in serialized form. For non client queries we need to
        // deserialize the value
        if (!getDeserializedObject && !((DefaultQuery) this.query).isKeepSerialized()) {
          getDeserializedObject = true;
        }
      }

      final boolean isDebugEnabled = logger.isDebugEnabled();
      if (!isDistinct) {
        CumulativeNonDistinctResults.Metadata wrapper = CumulativeNonDistinctResults
            .getCollectionMetadata(getDomainObjectForPdx, getDeserializedObject, localResults);

        for (Collection res : e.getValue()) {
          results.add(res);
          collectionsMetadata.add(wrapper);
        }
      } else {
        for (Collection res : e.getValue()) {
          checkIfQueryShouldBeCancelled();
          // final TaintableArrayList res = (TaintableArrayList) e.getValue();
          if (res != null) {
            if (isDebugEnabled) {
              logger.debug("Query Result from member :{}: {}", e.getKey(), res.size());
            }

            if (numElementsInResult == limit) {
              break;
            }
            boolean[] objectChangedMarker = new boolean[1];

            for (Object obj : res) {
              checkIfQueryShouldBeCancelled();
              int occurrence = 0;
              obj = PDXUtils.convertPDX(obj, isStruct, getDomainObjectForPdx, getDeserializedObject,
                  localResults, objectChangedMarker, true);
              boolean elementGotAdded =
                  isStruct ? ((StructSet) this.cumulativeResults).addFieldValues((Object[]) obj)
                      : this.cumulativeResults.add(obj);
              occurrence = elementGotAdded ? 1 : 0;
              // Asif: (Unique i.e first time occurrence) or subsequent occurrence
              // for non distinct query
              if (occurrence == 1) {
                ++numElementsInResult;
                // Asif:Check again to see if this addition caused limit to be
                // reached so that current loop will not iterate one more
                // time and then exit. It will exit immediately on this return
                if (numElementsInResult == limit) {
                  break;
                }
              }
            }
          }
        }
      }
    }

    if (prQueryTraceInfoList != null && this.query.isTraced() && logger.isInfoEnabled()) {
      if (DefaultQuery.testHook != null) {
        DefaultQuery.testHook
            .doTestHook(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_STRING, null);
      }
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("Trace Info for Query: %s",
          this.query.getQueryString())).append("\n");
      for (PRQueryTraceInfo queryTraceInfo : prQueryTraceInfoList) {
        sb.append(queryTraceInfo.createLogLine(me)).append("\n");
      }
      logger.info(sb.toString());;
    }
    if (!isDistinct) {
      this.cumulativeResults = new CumulativeNonDistinctResults(results, limit,
          this.cumulativeResults.getCollectionType().getElementType(), collectionsMetadata);

    }
    return this.cumulativeResults;
  }

  private void checkIfQueryShouldBeCancelled() {
    if (QueryMonitor.isLowMemory()) {
      String reason =
          "Query execution canceled due to low memory while gathering results from partitioned regions";
      query.setQueryCanceledException(new QueryExecutionLowMemoryException(reason));
      if (DefaultQuery.testHook != null) {
        DefaultQuery.testHook
            .doTestHook(DefaultQuery.TestHook.SPOTS.BEFORE_THROW_QUERY_CANCELED_EXCEPTION, null);
      }
      throw query.getQueryCanceledException();
    } else if (query.isCanceled()) {
      throw query.getQueryCanceledException();
    }
  }



  /**
   * Adds all counts from all member buckets to cumulative results.
   *
   */
  private void addTotalCountForMemberToResults(int limit) {
    int count = 0;
    for (Collection<Collection> results : this.resultsPerMember.values()) {
      for (Collection res : results) {
        if (res != null) {
          for (Object obj : res) {
            // Limit can be applied on count here as this is last
            // aggregation of bucket results.
            if (limit > -1 && count >= limit) {
              count = limit;
              break;
            }
            count += ((Integer) obj).intValue();
          }
          res.clear();
        }
      }
    }

    this.cumulativeResults.clear();
    this.cumulativeResults.add(count);
  }

  /**
   * Applies order-by on the results returned from PR nodes and puts the results in the cumulative
   * result set. The order-by is applied by running a generated query on the each result returned by
   * the remote nodes. Example generated query: SELECT DISTINCT * FROM $1 p ORDER BY p.ID Where
   * results are passed as bind parameter. This is added as quick turn-around, this is added based
   * on most commonly used queries, needs to be investigated further.
   */
  private SelectResults buildSortedResult(CompiledSelect cs, int limit) throws QueryException {

    try {
      ExecutionContext localContext = new QueryExecutionContext(this.parameters, this.pr.cache);


      List<Collection> allResults = new ArrayList<Collection>();
      for (Collection<Collection> memberResults : this.resultsPerMember.values()) {
        for (Collection res : memberResults) {
          if (res != null) {
            allResults.add(res);
          }
        }
      }

      this.cumulativeResults = new NWayMergeResults(allResults, cs.isDistinct(), limit,
          cs.getOrderByAttrs(), localContext, cs.getElementTypeForOrderByQueries());
      return this.cumulativeResults;
    } catch (Exception ex) {
      throw new QueryException(
          "Unable to apply order-by on the partition region cumulative results.", ex);
    }

  }

  /**
   * Generates a map with key as PR node and value as the list as a subset of the bucketIds hosted
   * by the node.
   *
   * @return the node-to-bucket map
   */

  // (package access for unit test purposes)
  Map<InternalDistributedMember, List<Integer>> buildNodeToBucketMap() throws QueryException {
    return buildNodeToBucketMapForBuckets(this.bucketsToQuery);
  }

  /**
   * @return Map of {@link InternalDistributedMember} to {@link ArrayList} of Integers
   */
  private Map<InternalDistributedMember, List<Integer>> buildNodeToBucketMapForBuckets(
      final Set<Integer> bucketIdsToConsider) throws QueryException {

    final HashMap<InternalDistributedMember, List<Integer>> ret =
        new HashMap<InternalDistributedMember, List<Integer>>();

    if (bucketIdsToConsider.isEmpty()) {
      return ret;
    }

    final List<Integer> bucketIds = new ArrayList<Integer>();
    PartitionedRegionDataStore dataStore = this.pr.getDataStore();
    final int totalBucketsToQuery = bucketIdsToConsider.size();
    if (dataStore != null) {
      for (Integer bid : bucketIdsToConsider) {
        if (dataStore.isManagingBucket(bid)) {
          bucketIds.add(Integer.valueOf(bid));
        }
      }
      if (bucketIds.size() > 0) {
        ret.put(pr.getMyId(), new ArrayList(bucketIds));
        // All the buckets are hosted locally.
        if (bucketIds.size() == totalBucketsToQuery) {
          return ret;
        }
      }
    }

    final List allNodes = getAllNodes(this.pr.getRegionAdvisor());
    /*
     * for(Map.Entry<InternalDistributedMember, Collection<Collection>> entry :
     * resultsPerMember.entrySet()) { InternalDistributedMember member = entry.getKey();
     * TaintableArrayList list = entry.getValue(); if(list.isTainted()) {
     * taintedMembers.add(member); } }
     */

    // Put the failed members on the end of the list.
    if (failedMembers != null && !failedMembers.isEmpty()) {
      allNodes.removeAll(failedMembers);
      allNodes.addAll(failedMembers);
    }

    for (Iterator dsItr = allNodes.iterator(); dsItr.hasNext()
        && (bucketIds.size() < totalBucketsToQuery);) {
      InternalDistributedMember nd = (InternalDistributedMember) dsItr.next();

      final List<Integer> buckets = new ArrayList<Integer>();
      for (Integer bid : bucketIdsToConsider) {
        if (!bucketIds.contains(bid)) {
          final Set owners = getBucketOwners(bid);
          if (owners.contains(nd)) {
            buckets.add(bid);
            bucketIds.add(bid);
          }
        }
      }
      if (!buckets.isEmpty()) {
        ret.put(nd, buckets);
      }
    }

    if (bucketIds.size() != totalBucketsToQuery) {
      bucketIdsToConsider.removeAll(bucketIds);
      throw new QueryException("Data loss detected, unable to find the hosting "
          + " node for some of the dataset. [dataset/bucket ids:" + bucketIdsToConsider + "]");
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Node to bucketId map: {}", ret);
    }
    return ret;
  }

  protected Set<InternalDistributedMember> getBucketOwners(Integer bid) {
    return pr.getRegionAdvisor().getBucketOwners(bid.intValue());
  }

  protected ArrayList getAllNodes(RegionAdvisor regionAdvisor) {
    ArrayList nodes = new ArrayList(regionAdvisor.adviseDataStore());
    Collections.shuffle(nodes);
    return nodes;
  }

  /**
   * Executes query on local data store.
   *
   * @throws QueryException, InterruptedException
   * @return true if the local query needs to be retried, otherwise false
   */
  private boolean executeQueryOnLocalNode() throws QueryException, InterruptedException {
    long startTime = 0;
    if (query.isTraced()) {
      startTime = NanoTimer.getTime();
    }
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    if (this.pr.getDataStore() != null) {
      this.pr.getDataStore().invokeBucketReadHook();
      final InternalDistributedMember me = this.pr.getMyId();

      List<Integer> bucketList = this.node2bucketIds.get(me);
      try {
        PRQueryProcessor qp = createLocalPRQueryProcessor(bucketList);
        MemberResultsList resultCollector = new MemberResultsList();

        // Execute Query.
        qp.executeQuery(resultCollector);

        // Only wrap/copy results when copy on read is set and an index is used on a local query
        // This is because when an index is used, the results are actual references to values in the
        // cache
        // Currently as 7.0.1 when indexes are not used, iteration uses non tx entries to retrieve
        // the value.
        // The non tx entry already checks copy on read and returns a copy.
        // The rest of the pr query will be copies from their respective nodes
        if (!this.query.isRemoteQuery() && pr.getCompressor() == null
            && pr.getCache().isCopyOnRead() && (!DefaultQueryService.COPY_ON_READ_AT_ENTRY_LEVEL
                || (qp.isIndexUsed() && DefaultQueryService.COPY_ON_READ_AT_ENTRY_LEVEL))) {
          MemberResultsList tmpResultCollector = new MemberResultsList();
          for (Object o : resultCollector) {
            Collection tmpResults;
            if (o instanceof Collection) {
              Collection results = (Collection) o;
              tmpResults = new ArrayList();

              for (Object collectionObject : results) {
                tmpResults.add(CopyHelper.copy(collectionObject));
              }
              tmpResultCollector.add(tmpResults);
            } else {
              tmpResultCollector.add(CopyHelper.copy(o));
            }
          }
          resultCollector = tmpResultCollector;
        }

        // Adds a query trace info object to the results list
        if (query.isTraced() && prQueryTraceInfoList != null) {
          if (DefaultQuery.testHook != null) {
            DefaultQuery.testHook
                .doTestHook(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FROM_LOCAL_NODE,
                    null);
          }
          PRQueryTraceInfo queryTraceInfo = new PRQueryTraceInfo();
          queryTraceInfo.setNumResults(queryTraceInfo.calculateNumberOfResults(resultCollector));
          queryTraceInfo.setTimeInMillis((NanoTimer.getTime() - startTime) / 1.0e6f);
          queryTraceInfo.setSender(me);
          // Due to the way trace info is populated, we will rely on the query execution logging
          // index usage for us.
          prQueryTraceInfoList.add(queryTraceInfo);
        }

        resultCollector.setLastChunkReceived(true);
        // Add results to the results-list. If prior successfully completed
        // results exist from previous executions on different buckets, add (to) those results as
        // well.
        MemberResultsList otherResults =
            (MemberResultsList) this.resultsPerMember.put(me, resultCollector);
        if (otherResults != null) {
          resultCollector.addAll(otherResults);
        }

      } catch (ForceReattemptException retryRequired) {
        if (logger.isDebugEnabled()) {
          logger.debug("Caught exception during local portion of query {}",
              this.query.getQueryString(), retryRequired);
        }
        return true;
      }
    }
    return false;
  }

  protected PRQueryProcessor createLocalPRQueryProcessor(List<Integer> bucketList) {
    return new PRQueryProcessor(this.pr, query, parameters, bucketList);
  }

  protected void memberStreamCorrupted(InternalDistributedMember sender) {
    this.resultsPerMember.remove(sender);
  }

  /**
   * To test the returned value from each member.
   */
  public Map getResultsPerMember() {
    return this.resultsPerMember;
  }

  /**
   * This class is used to accumulate information about indexes used in multipleThreads and results
   * gained from buckets. In future this can be used for adding for more information to final query
   * running info from pool threads.
   *
   * @since GemFire 6.6
   */
  public static class PRQueryResultCollector {

    private BlockingQueue resultQueue;
    private final Map<String, IndexInfo> usedIndexInfoMap;

    public PRQueryResultCollector() {
      this.resultQueue = new LinkedBlockingQueue();;
      this.usedIndexInfoMap = new Object2ObjectOpenHashMap<String, IndexInfo>(); // {indexName,
                                                                                 // IndexInfo} Map
    }

    public boolean isEmpty() {
      return this.resultQueue.isEmpty();
    }

    public void setResultQueue(BlockingQueue resultQueue) {
      this.resultQueue = resultQueue;
    }

    public Map getIndexInfoMap() {
      return usedIndexInfoMap;
    }

    public int size() {
      return resultQueue.size();
    }

    public Object get() throws InterruptedException {
      return resultQueue.take();
    }

    public void put(Object obj) throws InterruptedException {
      resultQueue.put(obj);
    }
  }

  public class StreamingQueryPartitionResponse
      extends StreamingPartitionOperation.StreamingPartitionResponse {

    public StreamingQueryPartitionResponse(InternalDistributedSystem system, Set members) {
      super(system, members);
    }

    @Override
    public void process(DistributionMessage msg) {
      // ignore messages from members not in the wait list
      if (!waitingOnMember(msg.getSender())) {
        return;
      }

      this.msgsBeingProcessed.incrementAndGet();
      try {
        StreamingReplyMessage m = (StreamingReplyMessage) msg;
        boolean isLast = true; // is last message for this member?
        List objects = m.getObjects();

        if (m.isCanceled()) {
          String reason =
              "Query execution canceled due to low memory while gathering results from partitioned regions";
          query.setQueryCanceledException(new QueryExecutionLowMemoryException(reason));
          this.abort = true;
        }

        // we will process null objects if it is a query msg and it is canceled. This allows us to
        // signal the query processor about dropped objects due to low memory
        if (objects != null) { // CONSTRAINT: objects should only be null if there's no data at all
          // Bug 37461: don't allow abort flag to be cleared
          boolean isAborted = this.abort; // volatile fetch
          if (!isAborted) {
            isAborted =
                !processChunk(objects, m.getSender(), m.getMessageNumber(), m.isLastMessage());
            if (isAborted) {
              this.abort = true; // volatile store
            }
          }
          isLast = isAborted || trackMessage(m); // interpret msgNum
          // @todo ezoerner send an abort message to data provider if
          // !doContinue (region was destroyed or cache closed);
          // also provide ability to explicitly cancel
        } else {
          // if a null chunk was received (no data), then
          // we're done with that member
          isLast = true;
        }
        if (isLast) { // commented by Suranjan watch this out
          super.process(msg, false); // removes from members and cause us to
                                     // ignore future messages received from that member
        }
      } finally {
        this.msgsBeingProcessed.decrementAndGet();
        checkIfDone(); // check to see if decrementing msgsBeingProcessed requires signalling to
                       // proceed
      }
    }

    public ObjectType getResultType() {
      return PartitionedRegionQueryEvaluator.this.cumulativeResults.getCollectionType()
          .getElementType();
    }
  }
}
