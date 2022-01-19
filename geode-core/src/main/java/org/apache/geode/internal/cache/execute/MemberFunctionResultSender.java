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
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.logging.internal.log4j.api.LogService;

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
    serverSender = sender;
  }

  @Override
  public void lastResult(Object oneResult) {
    if (!function.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (serverSender != null) { // client-server
      if (localLastResultReceived) {
        return;
      }
      if (onlyLocal) {
        serverSender.lastResult(oneResult);
        rc.endResults();
        localLastResultReceived = true;
      } else {
        lastResult(oneResult, rc, false, true, dm.getId());
      }
    } else { // P2P
      if (msg != null) {
        try {
          msg.sendReplyForOneResult(dm, oneResult, true, enableOrderedResultStreming);
        } catch (QueryException e) {
          throw new FunctionException(e);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      } else {
        if (localLastResultReceived) {
          return;
        }
        if (onlyLocal) {
          rc.addResult(dm.getDistributionManagerId(), oneResult);
          rc.endResults();
          localLastResultReceived = true;
        } else {
          // call a synchronized method as local node is also waiting to send lastResult
          lastResult(oneResult, rc, false, true, dm.getDistributionManagerId());
        }
        FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
            .incResultsReceived();
      }
    }
    FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
        .incResultsReturned();
  }

  private synchronized void lastResult(Object oneResult, ResultCollector collector,
      boolean lastRemoteResult, boolean lastLocalResult, DistributedMember memberID) {

    if (lastRemoteResult) {
      completelyDoneFromRemote = true;
    }
    if (lastLocalResult) {
      localLastResultReceived = true;

    }
    if (serverSender != null) { // Client-Server
      if (completelyDoneFromRemote && localLastResultReceived) {
        serverSender.lastResult(oneResult, memberID);
        collector.endResults();
      } else {
        serverSender.sendResult(oneResult, memberID);
      }
    } else { // P2P
      if (completelyDoneFromRemote && localLastResultReceived) {
        collector.addResult(memberID, oneResult);
        collector.endResults();
      } else {
        collector.addResult(memberID, oneResult);
      }
    }
  }

  public void lastResult(Object oneResult, boolean completelyDone, ResultCollector reply,
      DistributedMember memberID) {
    if (serverSender != null) { // Client-Server
      if (completelyDone) {
        if (onlyRemote) {
          serverSender.lastResult(oneResult, memberID);
          reply.endResults();
        } else {
          lastResult(oneResult, reply, true, false, memberID);
        }
      } else {
        serverSender.sendResult(oneResult, memberID);
      }
    } else { // P2P
      if (completelyDone) {
        if (onlyRemote) {
          reply.addResult(memberID, oneResult);
          reply.endResults();
        } else {
          // call a synchronized method as local node is also waiting to send lastResult
          lastResult(oneResult, reply, true, false, memberID);
        }
      } else {
        reply.addResult(memberID, oneResult);
      }
      FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
          .incResultsReceived();
    }
    FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
        .incResultsReturned();
  }

  @Override
  public void sendResult(Object oneResult) {
    if (!function.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (serverSender != null) { // Client-Server
      if (logger.isDebugEnabled()) {
        logger.debug("MemberFunctionResultSender sending result from local node to client {}",
            oneResult);
      }
      serverSender.sendResult(oneResult);
    } else { // P2P
      if (msg != null) {
        try {
          msg.sendReplyForOneResult(dm, oneResult, false, enableOrderedResultStreming);
        } catch (QueryException e) {
          throw new FunctionException(e);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      } else {
        rc.addResult(dm.getDistributionManagerId(), oneResult);
        FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
            .incResultsReceived();
      }
      // incrementing result sent stats.
      FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
          .incResultsReturned();
    }
  }


  @Override
  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(exception);
    lastResult(iFunxtionException);
    localLastResultReceived = true;
  }

  @Override
  public void setException(Throwable exception) {
    ((LocalResultCollector) rc).setException(exception);
    // this.lastResult(exception);
    logger.info("Unexpected exception during function execution local member",
        exception);
    rc.endResults();
    localLastResultReceived = true;
  }

  @Override
  public void enableOrderedResultStreming(boolean enable) {
    enableOrderedResultStreming = enable;
  }

  @Override
  public boolean isLocallyExecuted() {
    return msg == null;
  }

  @Override
  public boolean isLastResultReceived() {
    return localLastResultReceived;
  }
}
