/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.cache.execute;

import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionFunctionStreamingMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * ResultSender needs ResultCollector in which to add results one by one.
 * In case of localExecution it just adds result to the resultCollector whereas for remote cases it 
 * takes help of PRFunctionExecutionStreamer to send results to the calling node. The results will be received 
 * in the ResultReciever.
 * ResultSender will be instantiated in executeOnDatastore and set in FunctionContext.
 * 
 * @author skumar
 */

public final class PartitionedRegionFunctionResultSender implements
    InternalResultSender {

  private static final Logger logger = LogService.getLogger();
  
  PartitionedRegionFunctionStreamingMessage msg = null;

  private final DM dm;

  private final PartitionedRegion pr;

  private final long time;

  private final boolean forwardExceptions;
  
  private ResultCollector rc;

  private ServerToClientFunctionResultSender serverSender;

  private boolean localLastResultRecieved = false;

  private boolean onlyLocal = false;

  private boolean onlyRemote = false;

  private boolean completelyDoneFromRemote = false;

  private final Function function;

  private boolean enableOrderedResultStreming;

  private Set<Integer> bucketSet;
  
  /**
   * Have to combine next two constructor in one and make a new class which will 
   * send Results back.
   * @param msg
   * @param dm
   * @param pr
   * @param time
   */
  public PartitionedRegionFunctionResultSender(DM dm, PartitionedRegion pr,
      long time, PartitionedRegionFunctionStreamingMessage msg,
      Function function, Set<Integer> bucketSet) {
    this.msg = msg;
    this.dm = dm;
    this.pr = pr;
    this.time = time;
    this.function = function;
    this.bucketSet = bucketSet;
    
    forwardExceptions = false;
  }

  /**
   * Have to combine next two constructor in one and make a new class which will
   * send Results back.
   * @param dm
   * @param partitionedRegion
   * @param time
   * @param rc
   */
  public PartitionedRegionFunctionResultSender(DM dm,
      PartitionedRegion partitionedRegion, long time, ResultCollector rc,
      ServerToClientFunctionResultSender sender, boolean onlyLocal, boolean onlyRemote,
      boolean forwardExceptions, Function function, Set<Integer> bucketSet) {
    this.dm = dm;
    this.pr = partitionedRegion;
    this.time = time;
    this.rc = rc;
    this.serverSender = sender;
    this.onlyLocal = onlyLocal;
    this.onlyRemote = onlyRemote;
    this.forwardExceptions = forwardExceptions;
    this.function = function;
    this.bucketSet = bucketSet;
  }

  public void lastResult(Object oneResult) {
    if (!this.function.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if (!(forwardExceptions && oneResult instanceof Throwable) 
        && !pr.getDataStore().areAllBucketsHosted(bucketSet)) {
      throw new BucketMovedException(
          LocalizedStrings.FunctionService_BUCKET_MIGRATED_TO_ANOTHER_NODE
              .toLocalizedString());
    }
    if (this.serverSender != null) { // Client-Server
      if(this.localLastResultRecieved){
        return;
      }
      if (onlyLocal) {
        lastClientSend(dm.getDistributionManagerId(), oneResult);
        this.rc.endResults();
        this.localLastResultRecieved = true;
      }
      else {
      //call a synchronized method as local node is also waiting to send lastResult 
        lastResult(oneResult, rc, false, true, dm.getDistributionManagerId());
      }
    }
    else { // P2P

      if (this.msg != null) {
        try {          
          this.msg.sendReplyForOneResult(dm, pr, time, oneResult, true, enableOrderedResultStreming);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      else {
        if(this.localLastResultRecieved){
          return;
        }
        if (onlyLocal) {
          this.rc.addResult(dm.getDistributionManagerId(), oneResult);
          this.rc.endResults();
          this.localLastResultRecieved = true;
        }
        else {
        //call a synchronized method as local node is also waiting to send lastResult 
          lastResult(oneResult, rc, false, true, dm.getDistributionManagerId());
        }
        FunctionStats.getFunctionStats(function.getId(),
            this.dm.getSystem()).incResultsReceived();
      }
      // incrementing result sent stats.
      // Bug : remote node as well as local node calls this method to send
      // the result When the remote nodes are added to the local result collector at that
      // time the stats for the result sent is again incremented : Once the PR team comes with the concept of the Streaming FunctionOperation
      // for the partitioned Region then it will be simple to fix this problem.
      FunctionStats.getFunctionStats(function.getId(),
          this.dm.getSystem()).incResultsReturned();
    }
  } 

  private synchronized void lastResult(Object oneResult,
      ResultCollector collector, boolean lastRemoteResult,
      boolean lastLocalResult, DistributedMember memberID) {

    if(lastRemoteResult){
      this.completelyDoneFromRemote = true;
    }
    
    if(lastLocalResult) {
      this.localLastResultRecieved = true;
    }
    
    if (this.serverSender != null) { // Client-Server
      if (this.completelyDoneFromRemote && this.localLastResultRecieved) {
        lastClientSend(memberID, oneResult);
        collector.endResults();
      }
      else {
        clientSend(oneResult, memberID);
      }
    }
    else { // P2P
      if (this.completelyDoneFromRemote && this.localLastResultRecieved) {
        collector.addResult(memberID, oneResult);
        collector.endResults();
      }
      else {
        collector.addResult(memberID, oneResult);
      }
    }
  }

  public void lastResult(Object oneResult, boolean completelyDone,
      ResultCollector reply, DistributedMember memberID) {
    logger.debug("PartitionedRegionFunctionResultSender Sending lastResult {}", oneResult);

    if (this.serverSender != null) { // Client-Server
      
      if (completelyDone) {
        if (this.onlyRemote) {
          lastClientSend(memberID, oneResult);
          reply.endResults();
        }
        else {
          //call a synchronized method as local node is also waiting to send lastResult 
          lastResult(oneResult, reply, true, false, memberID);
        }
      }
      else {
        clientSend(oneResult, memberID);
      }
    }
    else{
      if (completelyDone) {
        if (this.onlyRemote) {
          reply.addResult(memberID, oneResult);
          reply.endResults();
        }
        else {
        //call a synchronized method as local node is also waiting to send lastResult 
          lastResult(oneResult, reply, true, false, memberID);
        }
      }
      else {
        reply.addResult(memberID, oneResult);
      }
      FunctionStats.getFunctionStats(function.getId(),
          this.dm == null ? null : this.dm.getSystem()).incResultsReceived();
    }
    FunctionStats.getFunctionStats(function.getId(),
        this.dm == null ? null : this.dm.getSystem()).incResultsReturned();
  }
      
  public void sendResult(Object oneResult) {
    if (!this.function.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if (this.serverSender != null) {
      logger.debug("PartitionedRegionFunctionResultSender sending result from local node to client {}", oneResult);
      clientSend(oneResult, dm.getDistributionManagerId());
    }
    else { // P2P
      if (this.msg != null) {
        try {
          logger.debug("PartitionedRegionFunctionResultSender sending result from remote node {}", oneResult);
          this.msg.sendReplyForOneResult(dm, pr, time, oneResult, false, enableOrderedResultStreming);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      else {
        logger.debug("PartitionedRegionFunctionResultSender adding result to ResultCollector on local node {}", oneResult);
        this.rc.addResult(dm.getDistributionManagerId(), oneResult);
        FunctionStats.getFunctionStats(function.getId(),
            this.dm.getSystem()).incResultsReceived();
      }
    //incrementing result sent stats.
      FunctionStats.getFunctionStats(function.getId(),
          this.dm.getSystem()).incResultsReturned();
    }
  }  
  
  private void clientSend(Object oneResult, DistributedMember memberID) {
    this.serverSender.sendResult(oneResult, memberID);
  }

  private void lastClientSend(DistributedMember memberID,
      Object lastResult) {
    this.serverSender.lastResult(lastResult, memberID);
  }

  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(
        exception);
    this.lastResult(iFunxtionException);
    this.localLastResultRecieved = true;
  }
  
  public void setException(Throwable exception) {
    if (this.serverSender != null) {
      this.serverSender.setException(exception);
    }
    else {
      ((LocalResultCollector)this.rc).setException(exception);
      logger.fatal(LocalizedMessage.create(
          LocalizedStrings.PartitionedRegionFunctionResultSender_UNEXPECTED_EXCEPTION_DURING_FUNCTION_EXECUTION_ON_LOCAL_NODE),
          exception);
    }
    this.rc.endResults();
    this.localLastResultRecieved = true;
  }
  
  public void enableOrderedResultStreming(boolean enable) {
    this.enableOrderedResultStreming = enable;
  }

  public boolean isLocallyExecuted()
  {
    return this.msg == null;
  }

  public boolean isLastResultReceived() {
    return localLastResultRecieved;
  }

}
