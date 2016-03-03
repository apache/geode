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

package com.gemstone.gemfire.internal.cache.execute;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.MemberFunctionStreamingMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
/**
 * 
 *
 */
public final class MemberFunctionResultSender implements InternalResultSender {

  private static final Logger logger = LogService.getLogger();
  
  MemberFunctionStreamingMessage msg = null;

  private final DM dm;

  private ResultCollector rc;

  private final Function function;

  private boolean localLastResultRecieved = false;

  private boolean onlyLocal = false;

  private boolean onlyRemote = false;

  private boolean completelyDoneFromRemote = false;
  private boolean enableOrderedResultStreming;

  private ServerToClientFunctionResultSender serverSender;

  /**
   * Have to combine next two construcotr in one and make a new class which will 
   * send Results back.
   * @param msg
   * @param dm
   */
  public MemberFunctionResultSender(DM dm, 
      MemberFunctionStreamingMessage msg, Function function) {
    this.msg = msg;
    this.dm = dm;
    this.function = function;

  }

  /**
   * Have to combine next two construcotr in one and make a new class which will
   * send Results back.
   * 
   * @param dm
   * @param rc
   */
  public MemberFunctionResultSender(DM dm, ResultCollector rc,
      Function function, boolean onlyLocal, boolean onlyRemote,
      ServerToClientFunctionResultSender sender) {
    this.dm = dm;
    this.rc = rc;
    this.function = function;
    this.onlyLocal = onlyLocal;
    this.onlyRemote = onlyRemote;
    this.serverSender = sender;
  }

  public void lastResult(Object oneResult) {
    if (!this.function.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if (this.serverSender != null) { // client-server
      if (this.localLastResultRecieved) {
        return;
      }
      if (onlyLocal) {
        this.serverSender.lastResult(oneResult);
        this.rc.endResults();
        this.localLastResultRecieved = true;
      }
      else {
        lastResult(oneResult, rc, false, true, this.dm.getId());
      }
    }
    else { // P2P
      if (this.msg != null) {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, true, enableOrderedResultStreming);
        }
        catch (QueryException e) {
          throw new FunctionException(e);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      else {
        if (this.localLastResultRecieved) {
          return;
        }
        if (onlyLocal) {
        this.rc.addResult(this.dm.getDistributionManagerId(), oneResult);
        this.rc.endResults();
        this.localLastResultRecieved = true;
      }
        else {
        //call a synchronized method as local node is also waiting to send lastResult 
          lastResult(oneResult, rc, false, true, this.dm
            .getDistributionManagerId());
        }
        FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
      }
    }
    FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();    
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
        this.serverSender.lastResult(oneResult, memberID);
        collector.endResults();
      }
      else {
        this.serverSender.sendResult(oneResult, memberID);
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
    if (this.serverSender != null) { // Client-Server
      if (completelyDone) {
        if (onlyRemote) {
          this.serverSender.lastResult(oneResult, memberID);
          reply.endResults();
        }
        else {
          lastResult(oneResult, reply, true, false, memberID);
        }
      }
      else {
        this.serverSender.sendResult(oneResult, memberID);
      }
    }
    else { // P2P
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
      FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
    }
    FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();
  }
      
  public void sendResult(Object oneResult) {
    if (!this.function.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if (this.serverSender != null) { // Client-Server
      if(logger.isDebugEnabled()){
        logger.debug("MemberFunctionResultSender sending result from local node to client {}", oneResult);
      }
      this.serverSender.sendResult(oneResult);
    }
    else { // P2P
      if (this.msg != null) {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, false, enableOrderedResultStreming);
        }
        catch (QueryException e) {
          throw new FunctionException(e);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      else {
        this.rc.addResult(this.dm.getDistributionManagerId(), oneResult);
        FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
      }
    //incrementing result sent stats.
      FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();
    }
  }
  
  
  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(
        exception);
    this.lastResult(iFunxtionException);
    this.localLastResultRecieved = true;
  }
  
  public void setException(Throwable exception) {
    ((LocalResultCollector)this.rc).setException(exception);
    //this.lastResult(exception);
    logger.info(LocalizedMessage.create(
        LocalizedStrings.MemberResultSender_UNEXPECTED_EXCEPTION_DURING_FUNCTION_EXECUTION_ON_LOCAL_NODE), exception);
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
    return this.localLastResultRecieved;
  }
}
