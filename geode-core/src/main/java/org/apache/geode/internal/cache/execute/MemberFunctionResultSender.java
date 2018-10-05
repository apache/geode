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

package org.apache.geode.internal.cache.execute;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.MemberFunctionStreamingMessage;
import org.apache.geode.internal.logging.LogService;

public class MemberFunctionResultSender implements InternalResultSender {

  private static final Logger logger = LogService.getLogger();

  MemberFunctionStreamingMessage msg = null;

  private final DistributionManager dm;

  private ResultCollector rc;

  private final Function function;

  private boolean localLastResultReceived = false;

  private boolean onlyLocal = false;

  private boolean onlyRemote = false;

  private boolean completelyDoneFromRemote = false;
  private boolean enableOrderedResultStreming;

  private ServerToClientFunctionResultSender serverSender;

  /**
   * Have to combine next two construcotr in one and make a new class which will send Results back.
   *
   */
  public MemberFunctionResultSender(DistributionManager dm, MemberFunctionStreamingMessage msg,
      Function function) {
    this.msg = msg;
    this.dm = dm;
    this.function = function;

  }

  /**
   * Have to combine next two construcotr in one and make a new class which will send Results back.
   *
   */
  public MemberFunctionResultSender(DistributionManager dm, ResultCollector rc, Function function,
      boolean onlyLocal, boolean onlyRemote, ServerToClientFunctionResultSender sender) {
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
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (this.serverSender != null) { // client-server
      if (this.localLastResultReceived) {
        return;
      }
      if (onlyLocal) {
        this.serverSender.lastResult(oneResult);
        this.rc.endResults();
        this.localLastResultReceived = true;
      } else {
        lastResult(oneResult, rc, false, true, this.dm.getId());
      }
    } else { // P2P
      if (this.msg != null) {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, true, enableOrderedResultStreming);
        } catch (QueryException e) {
          throw new FunctionException(e);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      } else {
        if (this.localLastResultReceived) {
          return;
        }
        if (onlyLocal) {
          this.rc.addResult(this.dm.getDistributionManagerId(), oneResult);
          this.rc.endResults();
          this.localLastResultReceived = true;
        } else {
          // call a synchronized method as local node is also waiting to send lastResult
          lastResult(oneResult, rc, false, true, this.dm.getDistributionManagerId());
        }
        FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
      }
    }
    FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();
  }

  private synchronized void lastResult(Object oneResult, ResultCollector collector,
      boolean lastRemoteResult, boolean lastLocalResult, DistributedMember memberID) {

    if (lastRemoteResult) {
      this.completelyDoneFromRemote = true;
    }
    if (lastLocalResult) {
      this.localLastResultReceived = true;

    }
    if (this.serverSender != null) { // Client-Server
      if (this.completelyDoneFromRemote && this.localLastResultReceived) {
        this.serverSender.lastResult(oneResult, memberID);
        collector.endResults();
      } else {
        this.serverSender.sendResult(oneResult, memberID);
      }
    } else { // P2P
      if (this.completelyDoneFromRemote && this.localLastResultReceived) {
        collector.addResult(memberID, oneResult);
        collector.endResults();
      } else {
        collector.addResult(memberID, oneResult);
      }
    }
  }

  public void lastResult(Object oneResult, boolean completelyDone, ResultCollector reply,
      DistributedMember memberID) {
    if (this.serverSender != null) { // Client-Server
      if (completelyDone) {
        if (onlyRemote) {
          this.serverSender.lastResult(oneResult, memberID);
          reply.endResults();
        } else {
          lastResult(oneResult, reply, true, false, memberID);
        }
      } else {
        this.serverSender.sendResult(oneResult, memberID);
      }
    } else { // P2P
      if (completelyDone) {
        if (this.onlyRemote) {
          reply.addResult(memberID, oneResult);
          reply.endResults();
        } else {
          // call a synchronized method as local node is also waiting to send lastResult
          lastResult(oneResult, reply, true, false, memberID);
        }
      } else {
        reply.addResult(memberID, oneResult);
      }
      FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
    }
    FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();
  }

  public void sendResult(Object oneResult) {
    if (!this.function.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (this.serverSender != null) { // Client-Server
      if (logger.isDebugEnabled()) {
        logger.debug("MemberFunctionResultSender sending result from local node to client {}",
            oneResult);
      }
      this.serverSender.sendResult(oneResult);
    } else { // P2P
      if (this.msg != null) {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, false, enableOrderedResultStreming);
        } catch (QueryException e) {
          throw new FunctionException(e);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      } else {
        this.rc.addResult(this.dm.getDistributionManagerId(), oneResult);
        FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
      }
      // incrementing result sent stats.
      FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();
    }
  }


  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(exception);
    this.lastResult(iFunxtionException);
    this.localLastResultReceived = true;
  }

  public void setException(Throwable exception) {
    ((LocalResultCollector) this.rc).setException(exception);
    // this.lastResult(exception);
    logger.info("Unexpected exception during function execution local member",
        exception);
    this.rc.endResults();
    this.localLastResultReceived = true;
  }

  public void enableOrderedResultStreming(boolean enable) {
    this.enableOrderedResultStreming = enable;
  }

  public boolean isLocallyExecuted() {
    return this.msg == null;
  }

  public boolean isLastResultReceived() {
    return this.localLastResultReceived;
  }
}
