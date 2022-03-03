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
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.DistributedRegionFunctionStreamingMessage;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class DistributedRegionFunctionResultSender implements InternalResultSender {

  private static final Logger logger = LogService.getLogger();

  DistributedRegionFunctionStreamingMessage msg = null;

  private final DistributionManager dm;

  private ResultCollector rc;

  private boolean isLocal;

  private ServerToClientFunctionResultSender sender;

  private final Function functionObject;

  private boolean enableOrderedResultStreming;

  private boolean localLastResultReceived = false;

  /**
   * Have to combine next two construcotr in one and make a new class which will send Results back.
   *
   */
  public DistributedRegionFunctionResultSender(DistributionManager dm,
      DistributedRegionFunctionStreamingMessage msg, Function function) {
    this.msg = msg;
    this.dm = dm;
    functionObject = function;
  }

  /**
   * Have to combine next two construcotr in one and make a new class which will send Results back.
   *
   */
  public DistributedRegionFunctionResultSender(DistributionManager dm, ResultCollector rc,
      Function function, final ServerToClientFunctionResultSender sender) {
    this.dm = dm;
    isLocal = true;
    this.rc = rc;
    functionObject = function;
    this.sender = sender;
  }

  @Override
  public void lastResult(Object oneResult) {
    if (!functionObject.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (localLastResultReceived) {
      return;
    }
    localLastResultReceived = true;
    if (sender != null) { // Client-Server
      sender.lastResult(oneResult);
      if (rc != null) {
        rc.endResults();
      }
    } else {
      if (isLocal) {
        rc.addResult(dm.getDistributionManagerId(), oneResult);
        rc.endResults();
        FunctionStatsManager.getFunctionStats(functionObject.getId(), dm.getSystem())
            .incResultsReceived();
      } else {
        try {
          msg.sendReplyForOneResult(dm, oneResult, true, enableOrderedResultStreming);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      FunctionStatsManager.getFunctionStats(functionObject.getId(), dm.getSystem())
          .incResultsReturned();
    }

  }

  public void lastResult(Object oneResult, DistributedMember memberID) {
    if (!functionObject.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    localLastResultReceived = true;
    if (sender != null) { // Client-Server
      sender.lastResult(oneResult, memberID);
      if (rc != null) {
        rc.endResults();
      }
    } else {
      if (isLocal) {
        rc.addResult(memberID, oneResult);
        rc.endResults();
      } else {
        try {
          msg.sendReplyForOneResult(dm, oneResult, true, enableOrderedResultStreming);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      if (dm == null) {
        FunctionStatsManager.getFunctionStats(functionObject.getId()).incResultsReceived();
      } else {
        FunctionStatsManager.getFunctionStats(functionObject.getId(), dm.getSystem())
            .incResultsReceived();
      }
    }

  }

  @Override
  public synchronized void sendResult(Object oneResult) {
    if (!functionObject.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (sender != null) { // Client-Server
      sender.sendResult(oneResult);
    } else {
      if (isLocal) {
        rc.addResult(dm.getDistributionManagerId(), oneResult);
        FunctionStatsManager.getFunctionStats(functionObject.getId(), dm.getSystem())
            .incResultsReceived();
      } else {
        try {
          msg.sendReplyForOneResult(dm, oneResult, false, enableOrderedResultStreming);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      FunctionStatsManager.getFunctionStats(functionObject.getId(), dm.getSystem())
          .incResultsReturned();
    }
  }

  public synchronized void sendResult(Object oneResult, DistributedMember memberID) {
    if (!functionObject.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (sender != null) { // Client-Server
      sender.sendResult(oneResult, memberID);
    } else {
      if (isLocal) {
        rc.addResult(memberID, oneResult);
        if (dm == null) {
          FunctionStatsManager.getFunctionStats(functionObject.getId()).incResultsReceived();
        } else {
          FunctionStatsManager.getFunctionStats(functionObject.getId(), dm.getSystem())
              .incResultsReceived();
        }
      } else {
        try {
          msg.sendReplyForOneResult(dm, oneResult, false, enableOrderedResultStreming);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      if (dm == null) {
        FunctionStatsManager.getFunctionStats(functionObject.getId()).incResultsReturned();
      } else {
        FunctionStatsManager.getFunctionStats(functionObject.getId(), dm.getSystem())
            .incResultsReturned();
      }
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
    if (sender != null) {
      sender.setException(exception);
      // this.sender.lastResult(exception);
    } else {
      ((LocalResultCollector) rc).setException(exception);
      // this.lastResult(exception);
      logger.info("Unexpected exception during function execution on local node Distributed Region",
          exception);
    }
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
