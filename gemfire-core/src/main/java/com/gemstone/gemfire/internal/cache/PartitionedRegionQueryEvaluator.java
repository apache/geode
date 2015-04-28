/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryExecutionLowMemoryException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.internal.CompiledID;
import com.gemstone.gemfire.cache.query.internal.CompiledIndexOperation;
import com.gemstone.gemfire.cache.query.internal.CompiledIteratorDef;
import com.gemstone.gemfire.cache.query.internal.CompiledLiteral;
import com.gemstone.gemfire.cache.query.internal.CompiledOperation;
import com.gemstone.gemfire.cache.query.internal.CompiledPath;
import com.gemstone.gemfire.cache.query.internal.CompiledSelect;
import com.gemstone.gemfire.cache.query.internal.CompiledSortCriterion;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.cache.query.internal.ExecutionContext;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver.IndexInfo;
import com.gemstone.gemfire.cache.query.internal.PRQueryTraceInfo;
import com.gemstone.gemfire.cache.query.internal.QueryExecutionContext;
import com.gemstone.gemfire.cache.query.internal.QueryMonitor;
import com.gemstone.gemfire.cache.query.internal.ResultsBag;
import com.gemstone.gemfire.cache.query.internal.RuntimeIterator;
import com.gemstone.gemfire.cache.query.internal.StructBag;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.partitioned.QueryMessage;
import com.gemstone.gemfire.internal.cache.partitioned.StreamingPartitionOperation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.internal.PdxString;

/**
 * This class sends the query on various <code>PartitionedRegion</code> data
 * store nodes and collects the results back, does the union of all the results.
 * 
 * @author rreja
 * @author Eric Zoerner
 *   revamped with streaming of results
 * @author Mitch Thomas
 *   retry logic
 */
public class PartitionedRegionQueryEvaluator extends StreamingPartitionOperation
{
  private static final Logger logger = LogService.getLogger();
  
  /**
   * @author Mitch Thomas
   * An ArraList which can be tainted
   * @since 6.0
   */
  public static class TaintableArrayList extends ArrayList {
    private boolean isPoison = false;
    public synchronized void taint() {
      this.isPoison = true;
      super.clear();
    }
    public boolean add(Object arg0) {
      synchronized(this) {
        if (this.isPoison) {
          return false;
        } else {
          return super.add(arg0);
        }
      }
    }
    public synchronized boolean isConsumable() {
      return !this.isPoison && size() > 0;
    }
    public synchronized boolean isTainted() {
      return this.isPoison;
    }
    
    public synchronized void untaint() {
      this.isPoison = false;
    }
  }

  /**
   * An ArraList which might be unconsumable.
   * @since 6.6.2
   * @author shobhit
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
   * @author Mitch Thomas
   * @since 6.0
   */
  public interface TestHook {
    public void hook(final int spot) throws RuntimeException;
  }
  private static final int MAX_PR_QUERY_RETRIES = Integer.getInteger("gemfire.MAX_PR_QUERY_RETRIES", 10).intValue();

  private final PartitionedRegion pr;
  private volatile Map node2bucketIds;
  private final DefaultQuery query;
  private final Object[] parameters;
  private SelectResults cumulativeResults;
  /** 
   * Member to result map, with member as key and values are collection of query results.
   * The value collection is collection of SelectResults (local query) or Collection of
   * Lists (from remote queries).
   */
  private final ConcurrentMap<InternalDistributedMember, Collection<Collection>> resultsPerMember;
  private ConcurrentLinkedQueue<PRQueryTraceInfo> prQueryTraceInfoList = null;
  private final Set<Integer> bucketsToQuery;
  private final IntOpenHashSet successfulBuckets;
  //set of members failed to execute query
  private Set<InternalDistributedMember> failedMembers;

  /**
   * Construct a PartitionedRegionQueryEvaluator
   * @param sys the distributed system
   * @param pr the partitioned region
   * @param query the query
   * @param parameters the parameters for executing the query
   * @param cumulativeResults where to add the results as they come in
   */
  public PartitionedRegionQueryEvaluator(InternalDistributedSystem sys,
                                         PartitionedRegion pr,
                                         DefaultQuery query, Object[] parameters,
                                         SelectResults cumulativeResults,
                                         Set<Integer> bucketsToQuery) {
    super(sys, pr.getPRId());
    this.pr = pr;
    this.query = query;
    this.parameters = parameters;
    this.cumulativeResults = cumulativeResults;
    this.bucketsToQuery = bucketsToQuery;
    this.successfulBuckets = new IntOpenHashSet(this.bucketsToQuery.size());
    this.resultsPerMember = new ConcurrentHashMap<InternalDistributedMember, Collection<Collection>>();
    this.node2bucketIds = Collections.EMPTY_MAP;
    if (query != null && query.isTraced()) {
      prQueryTraceInfoList = new ConcurrentLinkedQueue();
    }
  }
  
  @Override  
  protected DistributionMessage createRequestMessage(Set recipients, ReplyProcessor21 processor) {
    throw new UnsupportedOperationException();
  }
  
  protected DistributionMessage createRequestMessage(InternalDistributedMember recipient, ReplyProcessor21 processor, List bucketIds) {
    return new QueryMessage(recipient, this.pr.getPRId(), processor, this.query, this.parameters, bucketIds);
  }
  
 
  @Override
  public Set<InternalDistributedMember> getPartitionedDataFrom(Set recipients)
   throws com.gemstone.gemfire.cache.TimeoutException, InterruptedException, QueryException, ForceReattemptException {
     if (Thread.interrupted()) throw new InterruptedException();
     if (recipients.isEmpty())
       return Collections.emptySet();
     
     StreamingQueryPartitionResponse processor = new StreamingQueryPartitionResponse(this.sys, recipients);
     DistributionMessage m = createRequestMessage(recipients, processor);
     this.sys.getDistributionManager().putOutgoing(m);
     // should we allow this to timeout?
     Set<InternalDistributedMember> failedMembers = processor.waitForCacheOrQueryException();
     return failedMembers;
  }

  
  /**
   * @return false to abort
   */
  @Override  
  protected boolean processData(List objects, InternalDistributedMember sender,
                                int sequenceNum, boolean lastInSequence) {
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
    
    //We cannot do an if check for trace objects because it is possible
    //that a remote node has system Query.VERBOSE flag on
    //and yet the executing node does not.
    //We have to be sure to pull all trace infos out and not pull results
    if (objects.size() > 0) {
      Object traceObject = objects.get(0);
      if (traceObject instanceof PRQueryTraceInfo ) {
        if (DefaultQuery.testHook != null) {
          DefaultQuery.testHook.doTestHook("Pull off PR Query Trace Info");
        }
        PRQueryTraceInfo queryTrace = (PRQueryTraceInfo) objects.remove(0);
        queryTrace.setSender(sender);
        if (prQueryTraceInfoList != null) {
          prQueryTraceInfoList.add(queryTrace);
        }
      }
    }
    //Only way objects is null is if we are a QUERY_MSG_TYPE and the msg was canceled, that is set to true if objects were dropped due to low memory
    if (logger.isDebugEnabled()) {
      logger.debug("Results per member, for {} size: {}", sender, objects.size());
    }

    synchronized (results) {
      if (!QueryMonitor.isLowMemory()) {
        results.add(objects);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("query canceled while gathering results, aborting");
        }
        String reason = LocalizedStrings.QueryMonitor_LOW_MEMORY_WHILE_GATHERING_RESULTS_FROM_PARTITION_REGION
            .toLocalizedString();
        query.setCanceled(true, new QueryExecutionLowMemoryException(reason));
        return false;
      }

      if (lastInSequence) {
        ((MemberResultsList) results).setLastChunkReceived(true);
      }
    }
    
    //this.resultsPerMember.putIfAbsent(sender, objects);
    /*
    boolean toContinue = true;
    for (Iterator itr = objects.iterator(); itr.hasNext();) {
      final Object o = itr.next();
      if (o instanceof PRQueryProcessor.EndOfBucket) {
        int bucketId = ((PRQueryProcessor.EndOfBucket)o).getBucketId();
        synchronized (this.successfulBuckets) {
          this.successfulBuckets.add(bucketId);
        }
      }
      else {
        saveDataForMember(o, sender);
      }
    }
    */
    return true;
  }
  
  /**
    * Returns normally if succeeded to get data, otherwise throws an exception
    * @param th a test hook
    * @return true if parts of the query need to be retried, otherwise false
   */
  public boolean executeQueryOnRemoteAndLocalNodes(final TestHook th)
  throws InterruptedException, QueryException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    HashMap n2b = new HashMap(this.node2bucketIds);
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
      logger.debug("Sending query execution request to {} remote members for the query:{}", n2b.size(), this.query.getQueryString());
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
        processor = new StreamingQueryPartitionResponse(this.sys, n2b.keySet());
        for (Iterator itr = n2b.entrySet().iterator(); itr.hasNext();) {
          Map.Entry me = (Map.Entry) itr.next();
          final InternalDistributedMember rcp = (InternalDistributedMember) me.getKey();
          final List bucketIds = (List) me.getValue();
          DistributionMessage m = createRequestMessage(rcp, processor, bucketIds);
          Set notReceivedMembers = this.sys.getDistributionManager().putOutgoing(m);
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
    
    //Shobhit: Check if query is only for local buckets else return.
    if (this.node2bucketIds.containsKey(this.pr.getMyId())) {
      if (isDebugEnabled) {
        logger.debug("Started query execution on local data for query:{}", this.query.getQueryString());
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
        for(InternalDistributedMember member : failedMembers) {
          memberStreamCorrupted(member);
        }
        requiresRetry |= !failedMembers.isEmpty();

        if (isDebugEnabled) {
          logger.debug("Following remote members failed {} and retry flag is set to: {}", failedMembers, requiresRetry);
        }
      } catch (com.gemstone.gemfire.cache.TimeoutException e) {  //Shobhit: Swallow remote exception if
                                                                 //         local exception is there.
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
   
    if (query.isCanceled()){
      throw query.getQueryCanceledException();
    }

    if (localFault != null) {
      if (localFault instanceof QueryException) {
        throw (QueryException)localFault;
      } else if (localFault instanceof InterruptedException) {
        throw (InterruptedException)localFault;
      } else if (localFault instanceof Error) {
        throw (Error)localFault;
      } else if (localFault instanceof RuntimeException) {
        throw (RuntimeException)localFault;
      }
    }
    return requiresRetry | localNeedsRetry;
  }

  
  /**
   * Executes a query over the provided buckets in a <code>PartitionedRegion</code>.
   *
   * This method will automatically retry the query on buckets on which problems
   * where detected during query processing.
   *
   * If there are no exceptions results are placed in the provided SelectResults instance
   *
   * @param th a hook used for testing purposes, for normal operation provide a null
   * @throws QueryException if data loss is detected during the query, when the
   * number of retries has exceeded the system wide maximum, or when there are logic errors
   * that cause bucket data to be omitted from the results.
   * @throws InterruptedException
   */
  public void queryBuckets(final TestHook th) throws QueryException, InterruptedException {
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
      //Shobhit: Now on if buckets to be queried are on remote as well as local node,
      //request will be sent to remote node first to run query in parallel on local and
      //remote node.
      //Note: if Any Exception is thrown on local and some remote node, local exception
      //will be given priority and will be visible to query executor and remote exception
      //will be swallowed.
      needsRetry = executeQueryOnRemoteAndLocalNodes(th);
      if (th != null) {
        th.hook(1);
      }
      
      if (needsRetry) {
        //Shobhit: Only one chance is allowed for Function queries.
        if (query.isQueryWithFunctionContext()) {
          if (isDebugEnabled) {
            logger.debug("No of retry attempts are: {}", retry);
          }
          break;
        }
        Map b2n = buildNodeToBucketMapForBuckets(caclulateRetryBuckets());
        if (th != null) {
          th.hook(2);
        }
        this.node2bucketIds = b2n;
        if (isDebugEnabled) {
          logger.debug("PR Query retry: {} total: {}", retry, this.pr.getCachePerfStats().getPRQueryRetries());
        }
        this.pr.getCachePerfStats().incPRQueryRetries();
        retry++;
        //Shobhit: Wait for sometime as rebalancing might be happening
        waitBeforeRetry();
      }
      if (th != null) {
        th.hook(3);
      }
    }
    // the failed buckets are those in this.bucketsToQuery that are
    // not present in this.successfulBuckets
    /*
    synchronized (this.successfulBuckets) {
      this.bucketsToQuery.removeAll(this.successfulBuckets.toArray());
      this.successfulBuckets.clear();
    }
    
    */
    if (needsRetry) {
      String msg = "Failed to query all the partitioned region " +
        "dataset (buckets) after " + retry + " attempts.";
      
      if (isDebugEnabled) {
        logger.debug("{} Unable to query some of the buckets from the set :{}", msg, this.caclulateRetryBuckets());
      }
      throw new QueryException(msg);

      /*
      if (anyOfTheseBucketsHasStorage(this.bucketsToQuery)) {
        if (retry >= MAX_PR_QUERY_RETRIES) {
          String msg = "Query failed to get all results after " + retry + " attempts";
          throw new QueryException(msg);
        } else {
          failMissingBuckets();
        }
      } else {
        String msg = "Data loss detected during query "
          + this.query.getQueryString()
          + " subsequent query results should be suspect.";
        throw new QueryException(msg);
      }
      */
    }

    addResultsToResultSet();
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

  private boolean anyOfTheseBucketsHasStorage(Set<Integer> failedBuckets) {
    boolean haveStorage = false;
    for (Integer bid : failedBuckets) {
      if (this.pr.getRegionAdvisor().isStorageAssignedForBucket(bid)) {
        Set ownrs = this.pr.getRegionAdvisor().getBucketOwners(bid);
        for (Iterator boi = ownrs.iterator(); boi.hasNext(); ) {
          InternalDistributedMember mem = (InternalDistributedMember)boi.next();
          TaintableArrayList tal = (TaintableArrayList)this.resultsPerMember.get(mem);
          if (tal == null || !tal.isTainted()) {
            haveStorage = true;
          }
        }
      }
    }
    return haveStorage;
    
    /*
    boolean haveStorage = false;
    for (Iterator i = failedBuckets.iterator(); i.hasNext(); ) {
      final Integer bid = i.next();
      if (this.pr.getRegionAdvisor().isStorageAssignedForBucket(bid)) {
        Set ownrs = this.pr.getRegionAdvisor().getBucketOwners(bid);
        for (Iterator boi = ownrs.iterator(); boi.hasNext(); ) {
          InternalDistributedMember mem = (InternalDistributedMember)boi.next();
          TaintableArrayList tal = (TaintableArrayList)this.resultsPerMember.get(mem);
          if (tal == null || !tal.isTainted()) {
            haveStorage = true;
          }
        }
      }
    }
    return haveStorage;
    */
  }
  
  
  private Set<Integer> caclulateRetryBuckets() {
    Iterator memberToBucketList = node2bucketIds.entrySet().iterator();
    final HashSet<Integer> retryBuckets = new HashSet<Integer>();
    while (memberToBucketList.hasNext()) {
      Map.Entry<InternalDistributedMember, ArrayList<Integer>> e = (Map.Entry)memberToBucketList.next();
      InternalDistributedMember m = e.getKey();
      if (!this.resultsPerMember.containsKey(m)
          || (!((MemberResultsList) this.resultsPerMember.get(m))
              .isLastChunkReceived())) {
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

  private void addResultsToResultSet() throws QueryException {
    int numElementsInResult = 0;
    boolean isStructBag = false;
    boolean isResultBag = false;

    boolean isDistinct = false;
    boolean isCount = false;
   
    int limit = -1; //-1 indicates no limit wsa specified in the query
    //passed as null. Not sure if it can happen in real life situation.
    //So instead of modifying test , using a null check in constructor
    CompiledSelect cs =null;
    
    // Indicates whether to check for PdxInstance and convert them to 
    // domain object. 
    // In case of local queries the domain objects are stored in the result set
    // for client/server queries PdxInstance are stored in result set. 
    boolean getDomainObjectForPdx;
    //indicated results from remote nodes need to be deserialized
    //for local queries
    boolean getDeserializedObject = false;
    
    if(this.query != null) {
      cs =  this.query.getSimpleSelect();
      limit = this.query.getLimit(parameters);
      isDistinct = (cs != null)? cs.isDistinct():true;
      isCount = (cs != null)? cs.isCount():false;
    }
    
    if (isCount && !isDistinct) {
      addTotalCountForMemberToResults(limit);
      return;
    } 
    
    if (this.cumulativeResults instanceof StructBag) {
    	isStructBag = true;
    } else if (this.cumulativeResults instanceof ResultsBag) {
    	isResultBag = true;
    	//TODO:Asif: Idealy the isOrdered should  be the sufficient condtion. Remove the orderbyAttribs null check
    } else if (this.cumulativeResults.getCollectionType().isOrdered() && cs.getOrderByAttrs() != null) {
      // If its a sorted result set, sort local and remote results using query.
      buildSortedResult(cs, limit);
      return;
    }

    final DistributedMember me = this.pr.getMyId();

    if (DefaultQuery.testHook != null) {
      DefaultQuery.testHook.doTestHook(4);
    }
    boolean localResults = false;
    for (Map.Entry<InternalDistributedMember, Collection<Collection>> e : this.resultsPerMember.entrySet()) {
      checkLowMemory();
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
        if(!getDeserializedObject && !((DefaultQuery)this.query).isKeepSerialized()){
          getDeserializedObject = true;
        }
      }

      final boolean isDebugEnabled = logger.isDebugEnabled();
      
      for (Collection res : e.getValue()) {
        checkLowMemory();
        //final TaintableArrayList res = (TaintableArrayList) e.getValue();      
        if (res != null) {
          if (isDebugEnabled) {
            logger.debug("Query Result from member :{}: {}", e.getKey(), res.size());
          }
          if (limit == -1 && !getDomainObjectForPdx && !getDeserializedObject && 
              (!isDistinct && localResults /* This check is to convert PdxString in projection lists to String */)) {
            this.cumulativeResults.addAll(res);
          } else {
            if (numElementsInResult == limit) {
              break;
            }
            for (Object obj : res) {
              checkLowMemory();
              int occurence = 0;
              if (isStructBag) {
                StructImpl simpl = (StructImpl) obj;
                if (getDomainObjectForPdx) {
                  try {
                    if (simpl.isHasPdx()) {
                      occurence = ((ResultsBag) this.cumulativeResults).addAndGetOccurence(simpl.getPdxFieldValues());
                    } else {
                      occurence = ((ResultsBag) this.cumulativeResults).addAndGetOccurence(simpl.getFieldValues());
                    }
                  } catch (Exception ex) {
                    throw new QueryException(
                        "Unable to retrieve domain object from PdxInstance while building the ResultSet. "
                        + ex.getMessage());
                  }
                } else {
                  Object[] values = simpl.getFieldValues();
                  if(getDeserializedObject){
                    for (int i = 0; i < values.length; i++) {
                      if(values[i] instanceof VMCachedDeserializable){
                        values[i] = ((VMCachedDeserializable)values[i]).getDeserializedForReading();
                      }
                    }
                  }
                  /* This is to convert PdxString to String */                
                  if (simpl.isHasPdx() && isDistinct && localResults) {
                    for (int i = 0; i < values.length; i++) {
                      if(values[i] instanceof PdxString){
                        values[i] = ((PdxString)values[i]).toString();
                      }
                    }
                  }
                  occurence = ((ResultsBag) this.cumulativeResults).addAndGetOccurence(values);
                }
              } else {
                if (getDomainObjectForPdx) {
                  if(obj instanceof PdxInstance){
                    try {
                      obj = ((PdxInstance) obj).getObject();
                    } catch (Exception ex) {
                      throw new QueryException(
                          "Unable to retrieve domain object from PdxInstance while building the ResultSet. "
                          + ex.getMessage());
                    }
                  }
                  else if (obj instanceof PdxString){
                    obj = ((PdxString)obj).toString();
                  }
                } else if (isDistinct && localResults && obj instanceof PdxString) {
                  /* This is to convert PdxString to String */
                  obj = ((PdxString)obj).toString();
                }
                
                if (isResultBag) {
                  if(getDeserializedObject && obj instanceof VMCachedDeserializable) {
                    obj = ((VMCachedDeserializable)obj).getDeserializedForReading();
                  }
                    occurence = ((ResultsBag) this.cumulativeResults)
                  .addAndGetOccurence(obj);
                } else {
                  if(getDeserializedObject && obj instanceof VMCachedDeserializable) {
                      obj = ((VMCachedDeserializable)obj).getDeserializedForReading();
                  } 
                  
                  // Resultset or StructSet, SortedResultSet, SortedStructSet.
                  // Once we start passing Object[] in the List , the below should
                  // change for StructSet and possibly SortedStructSet
                  occurence = this.cumulativeResults.add(obj) ? 1 : 0;
                }
              }

              // Asif: (Unique i.e first time occurence) or subsequent occurence
              // for non distinct query
              if (occurence == 1 || (occurence > 1 && !isDistinct)) {
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
        DefaultQuery.testHook.doTestHook("Create PR Query Trace String");
      }
      StringBuilder sb = new StringBuilder();
      sb.append(LocalizedStrings.PartitionedRegion_QUERY_TRACE_LOG.toLocalizedString(this.query.getQueryString())).append("\n");
      for (PRQueryTraceInfo queryTraceInfo: prQueryTraceInfoList) {
        sb.append(queryTraceInfo.createLogLine(me)).append("\n");
      }
      logger.info(sb.toString());;
    }
  }
  
  private void checkLowMemory() {
    if (QueryMonitor.isLowMemory()) {
      String reason = LocalizedStrings.QueryMonitor_LOW_MEMORY_WHILE_GATHERING_RESULTS_FROM_PARTITION_REGION
          .toLocalizedString();
      query.setCanceled(true, new QueryExecutionLowMemoryException(reason));
      if (DefaultQuery.testHook != null) {
        DefaultQuery.testHook.doTestHook(5);
      }
      throw query.getQueryCanceledException();
    }
  }


  /**
   * Adds all counts from all member buckets to cumulative results.
   * @param limit
   */
  private void addTotalCountForMemberToResults(int limit) {
    int count = 0;
    for (Collection<Collection> results: this.resultsPerMember.values()) {
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
   * Applies order-by on the results returned from PR nodes and puts the results in 
   * the cumulative result set.
   * The order-by is applied by running a generated query on the each result returned
   * by the remote nodes.
   * Example generated query: SELECT DISTINCT * FROM $1 p ORDER BY p.ID
   * Where results are passed as bind parameter.
   * This is added as quick turn-around, this is added based on most commonly used
   * queries, needs to be investigated further.
   */   
  private void buildSortedResult(CompiledSelect cs, int limit) throws QueryException {
    List projAttrs = cs.getProjectionAttributes();
    List orderByAttrs = cs.getOrderByAttrs();
    //boolean isCount = cs.isCount();
    //List pIterators = cs.getIterators();
    
    String eMsg = "Unable to apply order-by on the partition region cumulative results.";
    Assert.assertTrue(!(orderByAttrs == null), eMsg + " Null order-by attributes.");
    List iterators = cs.getIterators();
   
    String projFields = null;
    //Map<String, Boolean> orderByFields = new LinkedHashMap<String,Boolean>();
    List<String> projectionFields = new LinkedList<String>();
    
    try {
      // Evaluate projection attributes.
      //Create local execution context
    //If order by clause is present , then compute dependency locally
      
      ExecutionContext localContext = new QueryExecutionContext(this.parameters,
          this.pr.cache);

      localContext.newScope(0);

      Iterator iter = iterators.iterator();
      while (iter.hasNext()) {

        CompiledIteratorDef iterDef = (CompiledIteratorDef)iter.next();
        // compute dependencies on this iter first before adding its
        // RuntimeIterator to the current scope.
        // this makes sure it doesn't bind attributes to itself
        localContext.addDependencies(cs, iterDef
            .computeDependencies(localContext));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(localContext);
        localContext.addToIndependentRuntimeItrMap(iterDef);
        localContext.bindIterator(rIter);

      }

      ObjectType type = cs.prepareResultType(localContext);
      StringBuffer tempQueryBuffer = new StringBuffer(" order by ");
      if (type.isStructType()) {
        StructType structType = (StructType)type;
        String[] fieldNames = structType.getFieldNames();
        if (projAttrs == null) {
          // Evaluate path iterators, in case of multiple paths appropriate
          // alias needs to be added.
          // E.g.: select distinct * from /r p, p.positions.values pos order by
          // p.ID =>
          // select distinct * from $1 m order by r.p.ID
          List<RuntimeIterator> runTimeItrs = localContext
              .getCurrentIterators();
          Iterator<RuntimeIterator> itr = runTimeItrs.iterator();
          while (itr.hasNext()) {
            StringBuffer temp = new StringBuffer();
            RuntimeIterator rIter = itr.next();
            rIter.setIndexInternalID(null);
            rIter.generateCanonicalizedExpression(temp, localContext);
            projectionFields.add(temp.toString());
          }

        }
        else {
          Iterator<Object[]> itr = projAttrs.iterator();
          while (itr.hasNext()) {
            StringBuffer temp = new StringBuffer();
            Object[] values = itr.next();
            ((CompiledValue)values[1]).generateCanonicalizedExpression(temp,
                localContext);
            projectionFields.add(temp.toString());
          }
        }
        // Evaluate order by attributes.
        for (int i = 0; i < orderByAttrs.size(); i++) {
          Object o = orderByAttrs.get(i);
          if (o instanceof CompiledSortCriterion) {
            CompiledSortCriterion csc = (CompiledSortCriterion)o;
            CompiledValue cv = csc.getExpr();
            StringBuffer temp = new StringBuffer();
            cv.generateCanonicalizedExpression(temp, localContext);
            Iterator<String> projFieldItr = projectionFields.iterator();
            int index = 0;
            boolean foundMatch = false;
            String orderBy = temp.toString();
            while (projFieldItr.hasNext() && !foundMatch) {
              String projStr = projFieldItr.next();
              // int indexOfDot = orderBy.indexOf('.');
              if (orderBy.equals(projStr)) {
                // exact match , just append the field name
                tempQueryBuffer.append(' ');
                tempQueryBuffer.append(fieldNames[index]);
                tempQueryBuffer.append(' ');
                tempQueryBuffer.append(csc.getCriterion() ? " desc " : " asc ");
                tempQueryBuffer.append(',');
                foundMatch = true;
              }
              else if (orderBy.startsWith(projStr)) {
                tempQueryBuffer.append(fieldNames[index]);
                tempQueryBuffer.append(temp.substring(projStr.length()));

                tempQueryBuffer.append(' ');
                tempQueryBuffer.append(csc.getCriterion() ? " desc " : " asc ");
                tempQueryBuffer.append(',');
                foundMatch = true;
              }
              ++index;
            }
            if (!foundMatch) {
              throw new QueryException("Order by clause " + orderBy
                  + " not derivable from any projection attribute");
            }

            // orderByFields.put(temp.toString(), !csc.getCriterion());
          }
        }
        tempQueryBuffer.deleteCharAt(tempQueryBuffer.length() - 1);

      }
      else {
        String projStr = null;
        if (projAttrs == null) {
          List<RuntimeIterator> runTimeItrs = localContext
              .getCurrentIterators();
          Iterator<RuntimeIterator> itr = runTimeItrs.iterator();

          StringBuffer temp = new StringBuffer();
          RuntimeIterator rIter = itr.next();
          rIter.setIndexInternalID(null);
          rIter.generateCanonicalizedExpression(temp, localContext);
          projStr = temp.toString();
        }

        else {
          Iterator<Object[]> itr = projAttrs.iterator();
          StringBuffer temp = new StringBuffer();
          Object[] values = itr.next();
          ((CompiledValue)values[1]).generateCanonicalizedExpression(temp,
              localContext);
          projStr = temp.toString();

        }
        // Evaluate order by attributes.
        for (int i = 0; i < orderByAttrs.size(); i++) {
          Object o = orderByAttrs.get(i);

          if (o instanceof CompiledSortCriterion) {
            CompiledSortCriterion csc = (CompiledSortCriterion)o;
            CompiledValue cv = csc.getExpr();
            StringBuffer temp = new StringBuffer();
            cv.generateCanonicalizedExpression(temp, localContext);

            String orderBy = temp.toString();
            // int indexOfDot = temp.indexOf(".");

            if (orderBy.equals(projStr)) {
              // exact match , just append the field name
              tempQueryBuffer.append(' ');
              tempQueryBuffer.append("iter");
              tempQueryBuffer.append(' ');

            }
            else if (orderBy.startsWith(projStr)) {
              tempQueryBuffer.append(' ');
              String attr = temp.substring(projStr.length() + 1);
              // escape reserved keywords
              attr = checkReservedKeyword(attr);
              tempQueryBuffer.append(attr);
            }
            else {
              throw new QueryException("Order by clause " + orderBy
                  + " not derivable from projection attribute " + projStr);
            }

            tempQueryBuffer.append(' ');
            tempQueryBuffer.append(csc.getCriterion() ? " desc " : " asc ");
            tempQueryBuffer.append(',');
          }
        }
        tempQueryBuffer.deleteCharAt(tempQueryBuffer.length() - 1);
      }

      tempQueryBuffer.insert(0, " SELECT DISTINCT * FROM $1 iter ");
      
      if (logger.isDebugEnabled()) {
        logger.debug("The temp query generated to evaluate order-by on PR commulative results: {}", tempQueryBuffer.toString());
      }

      DefaultQuery q = (DefaultQuery)this.pr.getCache().getQueryService()
          .newQuery(tempQueryBuffer.toString());
      ExecutionContext context;
      
      final DistributedMember me = this.pr.getMyId();

      for (Collection<Collection> memberResults : this.resultsPerMember.values()) {
        for (Collection res : memberResults) {
          if (res != null) {
            context = new QueryExecutionContext((new Object[] { res }), this.pr
                .getCache(), this.cumulativeResults, q);
            q.executeUsingContext(context);
          }
        }
      }
    } catch (Exception ex) {
      throw new QueryException("Unable to apply order-by on the partition region cumulative results.", ex);
    }
  }

  //returns attribute with escape quotes #51085 and #51886
  private String checkReservedKeyword(String attr) {
    if (attr != null && attr.length() > 0 && attr.contains(".")) {
      String[] splits = attr.split("[.]");
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < splits.length; i++) {
        sb.append(checkReservedKeyword(splits[i]) + ".");
      } 
      
      if (sb.length() <= 1) {
        attr = sb.toString(); 
      } 
      else {
        attr = sb.substring(0, sb.length() - 1);
      }
    }
    else if(DefaultQuery.reservedKeywords.contains(attr.toLowerCase())) {
      attr = "\"" + attr + "\"";
    }
    return attr;
  }

  /**
   * This returns the query clause as represented in the application query.
   * E.g.: returns p.status, p.getStatus() as represented by passed compiledValue.
   */
  private String getQueryAttributes(CompiledValue cv, StringBuffer fromPath) throws QueryException {
    // field with multiple level like p.pos.secId
    String clause = "";
    if (cv instanceof CompiledID)  {
      // It will be p.pos.secId
      clause = ((CompiledID)cv).getId() + clause;
    } else {
      do {
        if (cv instanceof CompiledPath || cv instanceof CompiledIndexOperation) {
          if (cv instanceof CompiledIndexOperation) {
            CompiledIndexOperation cio = (CompiledIndexOperation)cv;
            CompiledLiteral cl = (CompiledLiteral)cio.getExpression();
            StringBuffer sb = new StringBuffer();
            cl.generateCanonicalizedExpression(sb, null);
            cv = ((CompiledIndexOperation)cv).getReceiver();
            if (sb.length() > 0) {
              clause = "[" + sb.toString() + "]" + clause;
            }
          }
          clause = ("." + ((CompiledPath)cv).getTailID() + clause);
        } else if (cv instanceof CompiledOperation) {
          // Function call.
          clause = "." + ((CompiledOperation)cv).getMethodName() + "()" + clause;
        } else {
          throw new QueryException("Failed to evaluate order by attributes, found unsupported type  " + cv.getType() + 
          " Unable to apply order-by on the partition region cumulative results.");
        }

        cv = cv.getReceiver();
      } while (!(cv instanceof CompiledID));

      if (cv instanceof CompiledID) {
        clause = ((CompiledID)cv).getId() + clause;
        // Append region iterator alias. p
        if (fromPath != null) {
          fromPath.append(((CompiledID)cv).getId());
        }
      }      
    }
    return clause;
  }

  private void buildSortedResultBackup(CompiledSelect cs, int limit) throws QueryException {
    List projAttrs = cs.getProjectionAttributes();
    List orderByAttrs = cs.getOrderByAttrs();
    List pIterators = cs.getIterators();
    //boolean isDistinct = (cs != null)? cs.isDistinct():true;
    
    String eMsg = "Unable to apply order-by on the partition region cumulative results.";
    Assert.assertTrue(!(orderByAttrs == null), eMsg + " Null order-by attributes.");
    
    StringBuffer fromPath =  new StringBuffer();
    String projFields = null;
    HashMap<String, String> orderByFields = new HashMap<String, String>();
    
    try {
      // Evaluate projection attributes.
      String fromIter = "";
      if (projAttrs == null) {
        // Evaluate path iterators, in case of multiple paths appropriate alias needs to be added.
        // E.g.: select distinct * from /r p, p.positions.values pos order by p.ID =>
        //         select distinct * from $1 m order by r.p.ID
        for (int i = 1; i < pIterators.size(); i++) {
          CompiledIteratorDef iterDef = (CompiledIteratorDef) pIterators.get(i-1);   
          fromIter += (iterDef.getName() + ("."));
        }
      } else if (projAttrs.size() == 1) {
        // In case single projections, it should be treated as the ordered field.
        // E.g: select distinct status from /r order by status => select distinct * from $1 p order by p 
        Object projDef[] = (Object[])projAttrs.get(0);
        if (projDef[1] instanceof CompiledID) {
          projFields = ((CompiledID)projDef[1]).getId();
        } else if (projDef[1] instanceof CompiledPath) {
          CompiledPath cp = (CompiledPath)projDef[1];
          projFields = ((CompiledID)cp.getReceiver()).getId() + "." + cp.getTailID();
        } else if (projDef[1] instanceof CompiledOperation) {
          // Function call.
          CompiledOperation cp = (CompiledOperation)projDef[1];
          projFields = ((CompiledID)cp.getReceiver(null)).getId() + "." + cp.getMethodName() + "()";
        } else {
          throw new QueryException("Failed to evaluate projection attributes. " + eMsg);
        }
      }
     
      // Evaluate order by attributes.
      for (int i = 0; i < orderByAttrs.size(); i++) {
        Object o = orderByAttrs.get(i);
        String orderByClause = "";
        if (o instanceof CompiledSortCriterion) {
          CompiledSortCriterion csc = (CompiledSortCriterion)o;
          CompiledValue cv = csc.getExpr();  
          
          // field with multiple level like p.pos.secId
          if (cv instanceof CompiledID)  {
            // It will be p.pos.secId
            orderByClause = ((CompiledID)cv).getId() + orderByClause;
          } else {
            do {
            if (cv instanceof CompiledPath || cv instanceof CompiledIndexOperation) {
              if (cv instanceof CompiledIndexOperation) {
                CompiledIndexOperation cio = (CompiledIndexOperation)cv;
                CompiledLiteral cl = (CompiledLiteral)cio.getExpression();
                StringBuffer sb = new StringBuffer();
                cl.generateCanonicalizedExpression(sb, null);
                cv = ((CompiledIndexOperation)cv).getReceiver();
                if (sb.length() > 0) {
                  orderByClause = "[" + sb.toString() + "]" + orderByClause;
                }
              }
              orderByClause = ("." + ((CompiledPath)cv).getTailID() + orderByClause);
            } else if (cv instanceof CompiledOperation) {
              // Function call.
              orderByClause = "." + ((CompiledOperation)cv).getMethodName() + "()" + orderByClause;
            } else if (cv instanceof CompiledIndexOperation) {
              StringBuffer sb = new StringBuffer();
              
              //((CompiledIndexOperation)cv).generateCanonicalizedExpression(sb, null);
              //if (cv2 instanceof CompiledPath) {
              
              //orderByClause = "." + ((CompiledIndexOperation)cv).+ "()";
              //}
            } else {
              throw new QueryException("Failed to evaluate order by attributes, found unsupported type  " + cv.getType() + " " + eMsg);
            }
            // Ignore subsequent paths.
            //do {
                cv = cv.getReceiver();
            } while (!(cv instanceof CompiledID));
            
            if (cv instanceof CompiledID) {
              orderByClause = ((CompiledID)cv).getId() + orderByClause;
              // Append region iterator alias. p
              if (i == 0) {
                fromPath.append(((CompiledID)cv).getId());
                //if ((i+1) < orderByAttrs.size()) {
                //  fromPath.append(", ");
                //}
              }
            }
            
          } 
          /*
          else if (cv instanceof CompiledOperation) {
            orderByClause = ((CompiledID)cv).getId() + orderByClause;
          } else {
            throw new QueryException("Failed to evaluate order-by attributes. " + eMsg);              
          }
          */
          orderByFields.put(fromIter + orderByClause, (csc.getCriterion()? " desc " : " asc "));
        }
      }

      StringBuffer tmpSortQuery =  new StringBuffer("SELECT DISTINCT * FROM $1 ");    
      if (projFields != null && orderByFields.containsKey(projFields)) {
        // Select distinct p.status from /region p order by p.status asc
        // => Select distinct * from $1 p order by p asc
        if (fromPath.length() > 0) {
          tmpSortQuery.append(fromPath).append(" ORDER BY ").append(fromPath).append(" ").append(orderByFields.get(projFields));
        } else {
          tmpSortQuery.append(fromPath).append("p ORDER BY p").append(orderByFields.get(projFields));
        }
      } else {
        /*
        if (fromPath.length() > 0) {
          tmpSortQuery.append(fromPath).append(" ORDER BY ").append(fromPath).append(".");
        } else {
        */
        tmpSortQuery.append(fromPath).append(" ORDER BY ");
        //}
        Iterator iter = orderByFields.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<String, String> e = (Map.Entry<String, String>)iter.next();
          tmpSortQuery.append(e.getKey()).append(" ").append(e.getValue());
          if (iter.hasNext()) {
            tmpSortQuery.append(", ");
          }
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug("The temp query generated to evaluate order-by on PR commulative results: {}", tmpSortQuery);
      }
      
      DefaultQuery q = (DefaultQuery)this.pr.getCache().getQueryService().newQuery(tmpSortQuery.toString());
      ExecutionContext context;
      for (Iterator i=this.resultsPerMember.values().iterator(); i.hasNext(); ) {
        final TaintableArrayList res = (TaintableArrayList)i.next();
        if (res!=null && res.isConsumable()) {
          context = new QueryExecutionContext((new Object[] {res}), this.pr.getCache(), this.cumulativeResults, q);
          q.executeUsingContext(context);
          res.clear();
        }
      }   
    } catch (Exception ex) {
      throw new QueryException("Unable to apply order-by on the partition region cumulative results.", ex);
    }
  }
  /**
   * Generates a map with key as PR node and value as the list as a subset of
   * the bucketIds hosted by the node.
   *
   * @return the node-to-bucket map
   */
  
  // (package access, and returns map for unit test purposes)
  Map buildNodeToBucketMap() throws QueryException
  {
    return buildNodeToBucketMapForBuckets(this.bucketsToQuery);
  }

  /**
   * @param bucketIdsToConsider
   * @return Map of {@link InternalDistributedMember} to {@link ArrayList} of Integers
   */
  private Map buildNodeToBucketMapForBuckets(final Set<Integer> bucketIdsToConsider) 
  throws QueryException {
    
    final HashMap<InternalDistributedMember, ArrayList<Integer>> ret = new 
    HashMap<InternalDistributedMember, ArrayList<Integer>>();
    
    if (bucketIdsToConsider.isEmpty()) {
      return ret;
    }

    final ArrayList<Integer> bucketIds = new ArrayList<Integer>();
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
    
    final List allNodes = new ArrayList(this.pr.getRegionAdvisor().adviseDataStore());
    /*
    for(Map.Entry<InternalDistributedMember, Collection<Collection>> entry : resultsPerMember.entrySet()) {
      InternalDistributedMember member = entry.getKey();
      TaintableArrayList list = entry.getValue();
      if(list.isTainted()) {
        taintedMembers.add(member);
      }
    }*/
     
    //Put the failed members on the end of the list.
    if(failedMembers != null && !failedMembers.isEmpty()) {
      allNodes.removeAll(failedMembers);
      //Collections.shuffle(allNodes, PartitionedRegion.rand);
      allNodes.addAll(failedMembers);
    }
    
    for (Iterator dsItr = allNodes.iterator(); dsItr.hasNext() && (bucketIds.size() < totalBucketsToQuery); ) {
      InternalDistributedMember nd = (InternalDistributedMember)dsItr.next();
      
      /*
      if(taintedMembers.contains(nd)) {
        //clear the tainted state
        resultsPerMember.get(nd).untaint();
      }
      */
      
      final ArrayList<Integer> buckets = new ArrayList<Integer>();
      for (Integer bid : bucketIdsToConsider) {
        if (!bucketIds.contains(bid)) {
          final Set owners = pr.getRegionAdvisor().getBucketOwners(bid.intValue());
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
      throw new QueryException("Data loss detected, unable to find the hosting " +
          " node for some of the dataset. [dataset/bucket ids:" + bucketIdsToConsider + "]");
    }
    
    if (logger.isDebugEnabled()) {
      logger.debug("Node to bucketId map: {}", ret);
    }
    return ret;
  }

  /**
   * Executes query on local data store. 
   * 
   * @throws QueryException, InterruptedException
   * @return true if the local query needs to be retried, otherwise false
   */
  private boolean executeQueryOnLocalNode() throws QueryException, InterruptedException
  {
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
      //Create PRQueryResultCollector here.
      //RQueryResultCollector resultCollector = new PRQueryResultCollector();

      List bucketList = (List)this.node2bucketIds.get(me);
      //try {
        
        //this.pr.getDataStore().queryLocalNode(this.query, this.parameters,
        //    bucketList, resultCollector);
      try {
        PRQueryProcessor qp = new PRQueryProcessor(this.pr, query, parameters, bucketList);
        MemberResultsList resultCollector = new MemberResultsList();
        
        // Execute Query.
        qp.executeQuery(resultCollector);
        
        //Only wrap/copy results when copy on read is set and an index is used on a local query
        //This is because when an index is used, the results are actual references to values in the cache
        //Currently as 7.0.1 when indexes are not used, iteration uses non tx entries to retrieve the value.
        //The non tx entry already checks copy on read and returns a copy.
        //The rest of the pr query will be copies from their respective nodes
        if (!this.query.isRemoteQuery()
            && pr.getCompressor() == null
            && pr.getCache().isCopyOnRead()
            && (!DefaultQueryService.COPY_ON_READ_AT_ENTRY_LEVEL || (qp.isIndexUsed() && DefaultQueryService.COPY_ON_READ_AT_ENTRY_LEVEL))) {
          MemberResultsList tmpResultCollector = new MemberResultsList();
          for (Object o: resultCollector) {
            Collection tmpResults;
            if (o instanceof Collection) {
              Collection results = (Collection) o;
              tmpResults = new ArrayList();

              for (Object collectionObject: results) {
                tmpResults.add(CopyHelper.copy(collectionObject));
              }
              tmpResultCollector.add(tmpResults);
            }
            else {
              tmpResultCollector.add(CopyHelper.copy(o));
            }
          }
          resultCollector = tmpResultCollector;
        }
      
        //Adds a query trace info object to the results list
        if (query.isTraced() && prQueryTraceInfoList != null) {
          if (DefaultQuery.testHook != null) {
            DefaultQuery.testHook.doTestHook("Create PR Query Trace Info From Local Node");
          }
          PRQueryTraceInfo queryTraceInfo = new PRQueryTraceInfo();
          queryTraceInfo.setNumResults(queryTraceInfo.calculateNumberOfResults(resultCollector));
          queryTraceInfo.setTimeInMillis((NanoTimer.getTime() - startTime) / 1.0e6f);
          queryTraceInfo.setSender(me);
          //Due to the way trace info is populated, we will rely on the query execution logging
          //index usage for us.
          prQueryTraceInfoList.add(queryTraceInfo);        
        }
        
        resultCollector.setLastChunkReceived(true);
        // Add results to the results-list.
        this.resultsPerMember.put(me, resultCollector);
        
      } catch (ForceReattemptException retryRequired) {
        if (logger.isDebugEnabled()) {
          logger.debug("Caught exception during local portion of query {}",this.query.getQueryString(), retryRequired);
        }
        return true;
      } 
        /*
        ExecutionContext context = new ExecutionContext(parameters, this.pr.getCache());
        context.setBucketList(bucketList);
        try {
          SelectResults results = (SelectResults)this.query.executeUsingContext(context);
          addToResultCollector(results, resultCollector, me);
        } catch (BucketMovedException bme) {
          return true;
        }
        
        //this.successfulBuckets.addAll(context.getSuccessfulBuckets());
        for (Object o: context.getBucketList()) {
          Integer bId = (Integer)o;
          this.successfulBuckets.add(bId.intValue());
        }
        
      /*
      } catch (ForceReattemptException retryRequired) {
        return true;
      }
      */
      /*
      int tokenCount = 0;
      final int numBuckets = bucketList.size();
      // finished when we get the nth END_OF_STREAM token, where n is the number of buckets
      boolean toContinue = true;
      Object o = null;
      while (tokenCount < numBuckets) {
        o = resultCollector.get();
        if (o instanceof PRQueryProcessor.EndOfBucket) {
          int bucketId = ((PRQueryProcessor.EndOfBucket)o).getBucketId();
          synchronized (this.successfulBuckets) {
            this.successfulBuckets.add(bucketId);
          }
          tokenCount++;
        } else {
          if (o == DefaultQuery.NULL_RESULT) {
            o = null;
          }
          saveDataForMember(o, me);
        }
      }
      Assert.assertTrue(resultCollector.isEmpty());
				*/
    }
    return false;
  }
  
		/*
  private void saveDataForMember(final Object data, final InternalDistributedMember member) {
    TaintableArrayList existing = this.resultsPerMember.get(member);
    if (existing == null) {
      synchronized (member) {
        existing = new TaintableArrayList();
        this.resultsPerMember.putIfAbsent(member, existing);
      }
    }
    existing.add(data);
  }
  */

  protected void memberStreamCorrupted(InternalDistributedMember sender) {
    this.resultsPerMember.remove(sender);
    /*
    final TaintableArrayList tainted = new TaintableArrayList();
    tainted.taint();
    TaintableArrayList existing = 
      (TaintableArrayList)this.resultsPerMember.putIfAbsent(sender, tainted);
    if (existing != null) {
      existing.taint();
    }

    ArrayList bucketIds = (ArrayList)this.node2bucketIds.get(sender);
    if (bucketIds != null) {
      ArrayList removedBucketIds = null;
      for (Iterator i = bucketIds.iterator(); i.hasNext(); ) {
        Integer bid = (Integer)i.next();
        synchronized(this.successfulBuckets) {
          if (this.successfulBuckets.remove(bid.intValue())) {
            if (removedBucketIds == null) {
              removedBucketIds = new ArrayList();
            }
            removedBucketIds.add(bid);
          }
        }
      }

    }
    */
  }

  // @todo need to throw a better exception than QueryException
  /**
   * Fail due to not getting all the data back for all the buckets,
   * reporting which buckets failed on which nodes.
   *
   * @throws QueryException always throws
   * since QueryException should be abstract
   */
  private void failMissingBuckets() throws QueryException {
    // convert to Map of nodes to bucket ids for error message
    Map n2b = new HashMap();
    for (Integer bId : this.bucketsToQuery) {
      InternalDistributedMember node = findNodeForBucket(bId);
      List listOfInts = (List)n2b.get(node);
      if (listOfInts == null) {
        listOfInts = new ArrayList<Integer>();
        n2b.put(node, listOfInts);
      }
      listOfInts.add(bId);
    }
    
    /*
    Iterator = this.bucketsToQuery.iterator();
    int sz = intArray.length;
    Map n2b = new HashMap();
    for (int i = 0; i < sz; i++) {
      Integer bucketId = Integer.valueOf(intArray[i]);
      InternalDistributedMember node = findNodeForBucket(bucketId);
      List listOfInts = (List)n2b.get(node);
      if (listOfInts == null) {
        listOfInts = new ArrayList();
        n2b.put(node, listOfInts);
      }
      listOfInts.add(bucketId);
    }
    */
    
    // One last check, after all else is said and done: 
    // if the system is closing, don't fail the query, but
    // generate a much more serious error...
    this.pr.getCancelCriterion().checkCancelInProgress(null);
    
    // the failure
    String msg = "Query failed; unable to get results from the following node/buckets: "
    + n2b;

    logger.fatal(msg);
    throw new QueryException( // @todo what is a better exception to throw here?
        msg);
    
    /* alternative strategy: re-query
    queryBuckets();
    */
    
  }
  
  private InternalDistributedMember findNodeForBucket(Integer bucketId) {
    for (Iterator itr = this.node2bucketIds.entrySet().iterator(); itr.hasNext(); ) {
      Map.Entry entry = (Map.Entry)itr.next();
      List blist = (List)entry.getValue();
      for (Iterator itr2 = blist.iterator(); itr2.hasNext(); ) {
        Integer bid = (Integer)itr2.next();
        if (bid.equals(bucketId)) {
          return (InternalDistributedMember)entry.getKey();
        }
      }
    }
    String msg = "Unable to get node for bucket id " + bucketId + " node to bucket map is " + this.node2bucketIds;
    logger.fatal(msg);
    throw new InternalGemFireError(msg);
  }

  /**
   * To test the returned value from each member.
   */
  public Map getResultsPerMember() {
    return this.resultsPerMember;
  }

  /**
   * This class is used to accumulate information about indexes used
   * in multipleThreads and results gained from buckets.
   * In future this can be used for adding for more information to final
   * query running info from pool threads.
   * @author shobhit
   * @since 6.6
   */
  public static class PRQueryResultCollector {

    private BlockingQueue resultQueue;
    private final Map<String, IndexInfo> usedIndexInfoMap;

    public PRQueryResultCollector() {
      this.resultQueue = new LinkedBlockingQueue();;
      this.usedIndexInfoMap = new Object2ObjectOpenHashMap<String, IndexInfo>(); //{indexName, IndexInfo} Map
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
    
    public int size(){
      return resultQueue.size();
    }
    
    public Object get() throws InterruptedException{
      return resultQueue.take();
    }
    
    public void put(Object obj) throws InterruptedException{
      resultQueue.put(obj);
    }
  }
  
  public class StreamingQueryPartitionResponse extends StreamingPartitionOperation.StreamingPartitionResponse {

    public StreamingQueryPartitionResponse(InternalDistributedSystem system,
        Set members) {
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
        StreamingReplyMessage m = (StreamingReplyMessage)msg;
        boolean isLast = true; // is last message for this member?
        List objects = m.getObjects();

        if (m.isCanceled()) {
          String reason = LocalizedStrings.QueryMonitor_LOW_MEMORY_WHILE_GATHERING_RESULTS_FROM_PARTITION_REGION.toLocalizedString();
          query.setCanceled(true, new QueryExecutionLowMemoryException(reason));
          this.abort = true;
        }
        
        //we will process null objects if it is a query msg and it is canceled.  This allows us to signal the query processor about dropped objects due to low memory
        if (objects != null) {  // CONSTRAINT: objects should only be null if there's no data at all
          // Bug 37461: don't allow abort flag to be cleared
          boolean isAborted = this.abort; // volatile fetch
          if (!isAborted) {
            isAborted = !processChunk(objects, m.getSender(),
                m.getMessageNumber(), m.isLastMessage());
            if (isAborted) {
              this.abort = true; // volatile store
            }
          }
          isLast = isAborted || trackMessage(m); // interpret msgNum
          // @todo ezoerner send an abort message to data provider if
          // !doContinue (region was destroyed or cache closed);
          // also provide ability to explicitly cancel
        }
        else {
          // if a null chunk was received (no data), then
          // we're done with that member
          isLast = true;
        }
       if (isLast) { //commented by Suranjan watch this out
          super.process(msg, false); // removes from members and cause us to
                                     // ignore future messages received from that member
        }
      }
      finally {
        this.msgsBeingProcessed.decrementAndGet();
        checkIfDone(); // check to see if decrementing msgsBeingProcessed requires signalling to proceed
      }          
    }  
    
  }
}
