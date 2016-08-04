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
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.cache.DistributedRegionFunctionStreamingMessage;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
/**
 * 
 *
 */
public final class DistributedRegionFunctionResultSender implements
    InternalResultSender {

  private static final Logger logger = LogService.getLogger();
  
  DistributedRegionFunctionStreamingMessage msg = null;

  private final DM dm;

  private ResultCollector rc;

  private boolean isLocal;

  private ServerToClientFunctionResultSender sender;
  
  private final Function functionObject;

  private boolean enableOrderedResultStreming;

  private boolean localLastResultRecieved = false;

  /**
   * Have to combine next two construcotr in one and make a new class which will
   * send Results back.
   * 
   * @param msg
   * @param dm
   */
  public DistributedRegionFunctionResultSender(DM dm,
      DistributedRegionFunctionStreamingMessage msg, Function function) {
    this.msg = msg;
    this.dm = dm;
    this.functionObject = function;
  }

  /**
   * Have to combine next two construcotr in one and make a new class which will
   * send Results back.
   *
   */
  public DistributedRegionFunctionResultSender(DM dm, ResultCollector rc,
      Function function, final ServerToClientFunctionResultSender sender) {
    this.dm = dm;
    this.isLocal = true;  
    this.rc = rc;
    this.functionObject = function;
    this.sender = sender;
  }

  public void lastResult(Object oneResult) {
    if (!this.functionObject.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if(this.localLastResultRecieved){
      return;
    }
    this.localLastResultRecieved = true;
    if (this.sender != null) { // Client-Server
      sender.lastResult(oneResult);
      if(this.rc != null) {
        this.rc.endResults();
      }
    }
    else {
      if (isLocal) {
        this.rc.addResult(dm.getDistributionManagerId(), oneResult);
        this.rc.endResults();
        FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem()).incResultsReceived();
      }
      else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, true, enableOrderedResultStreming);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
    //incrementing result sent stats.
      FunctionStats.getFunctionStats(functionObject.getId(),
              this.dm.getSystem()).incResultsReturned();
    }
  
  }
  
  public void lastResult(Object oneResult, DistributedMember memberID) {
    if (!this.functionObject.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    this.localLastResultRecieved = true;
    if (this.sender != null) { // Client-Server
      sender.lastResult(oneResult, memberID);
      if (this.rc != null) {
        this.rc.endResults();
      }
    }
    else {
      if (isLocal) {
        this.rc.addResult(memberID, oneResult);
        this.rc.endResults();
      }
      else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, true, enableOrderedResultStreming);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      if (this.dm == null) {
        FunctionStats.getFunctionStats(functionObject.getId()).incResultsReceived();
      } else {
        FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem()).incResultsReceived();
      }
    }

  }

  public synchronized void sendResult(Object oneResult) {
    if (!this.functionObject.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if (this.sender != null) { // Client-Server
      sender.sendResult(oneResult);
    }
    else {
      if (isLocal) {
        this.rc.addResult(dm.getDistributionManagerId(), oneResult);
        FunctionStats.getFunctionStats(functionObject.getId(),
                this.dm.getSystem()).incResultsReceived();
      }
      else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, false, enableOrderedResultStreming);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
    //incrementing result sent stats.
      FunctionStats.getFunctionStats(functionObject.getId(),
              this.dm.getSystem()).incResultsReturned();
    }
  }
  
  public synchronized void sendResult(Object oneResult,
      DistributedMember memberID) {
    if (!this.functionObject.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if (this.sender != null) { // Client-Server
      sender.sendResult(oneResult, memberID);
    }
    else {
      if (isLocal) {
        this.rc.addResult(memberID, oneResult);
        if (this.dm == null) {
          FunctionStats.getFunctionStats(functionObject.getId()).incResultsReceived();
        } else {
          FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem()).incResultsReceived();
        }
      }
      else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, false, enableOrderedResultStreming);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      if (this.dm == null) {
        FunctionStats.getFunctionStats(functionObject.getId()).incResultsReturned();
      } else {
        FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem()).incResultsReturned();
      }
    }
  }
  
  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(
        exception);
    this.lastResult(iFunxtionException);
    this.localLastResultRecieved = true;
  }
  
  public void setException(Throwable exception) {
    if (this.sender != null) {
      this.sender.setException(exception);
      //this.sender.lastResult(exception);
    }
    else {
      ((LocalResultCollector)this.rc).setException(exception);
      //this.lastResult(exception);
      logger.info(LocalizedMessage.create(
          LocalizedStrings.DistributedRegionFunctionResultSender_UNEXPECTED_EXCEPTION_DURING_FUNCTION_EXECUTION_ON_LOCAL_NODE), exception);
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
    return this.localLastResultRecieved ;
  }
}
