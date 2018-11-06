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
import org.apache.geode.internal.logging.LogService;

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
    this.functionObject = function;
  }

  /**
   * Have to combine next two construcotr in one and make a new class which will send Results back.
   *
   */
  public DistributedRegionFunctionResultSender(DistributionManager dm, ResultCollector rc,
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
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (this.localLastResultReceived) {
      return;
    }
    this.localLastResultReceived = true;
    if (this.sender != null) { // Client-Server
      sender.lastResult(oneResult);
      if (this.rc != null) {
        this.rc.endResults();
      }
    } else {
      if (isLocal) {
        this.rc.addResult(dm.getDistributionManagerId(), oneResult);
        this.rc.endResults();
        FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem())
            .incResultsReceived();
      } else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, true, enableOrderedResultStreming);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem())
          .incResultsReturned();
    }

  }

  public void lastResult(Object oneResult, DistributedMember memberID) {
    if (!this.functionObject.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    this.localLastResultReceived = true;
    if (this.sender != null) { // Client-Server
      sender.lastResult(oneResult, memberID);
      if (this.rc != null) {
        this.rc.endResults();
      }
    } else {
      if (isLocal) {
        this.rc.addResult(memberID, oneResult);
        this.rc.endResults();
      } else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, true, enableOrderedResultStreming);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      if (this.dm == null) {
        FunctionStats.getFunctionStats(functionObject.getId()).incResultsReceived();
      } else {
        FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem())
            .incResultsReceived();
      }
    }

  }

  public synchronized void sendResult(Object oneResult) {
    if (!this.functionObject.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (this.sender != null) { // Client-Server
      sender.sendResult(oneResult);
    } else {
      if (isLocal) {
        this.rc.addResult(dm.getDistributionManagerId(), oneResult);
        FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem())
            .incResultsReceived();
      } else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, false, enableOrderedResultStreming);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem())
          .incResultsReturned();
    }
  }

  public synchronized void sendResult(Object oneResult, DistributedMember memberID) {
    if (!this.functionObject.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (this.sender != null) { // Client-Server
      sender.sendResult(oneResult, memberID);
    } else {
      if (isLocal) {
        this.rc.addResult(memberID, oneResult);
        if (this.dm == null) {
          FunctionStats.getFunctionStats(functionObject.getId()).incResultsReceived();
        } else {
          FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem())
              .incResultsReceived();
        }
      } else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, false, enableOrderedResultStreming);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      if (this.dm == null) {
        FunctionStats.getFunctionStats(functionObject.getId()).incResultsReturned();
      } else {
        FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem())
            .incResultsReturned();
      }
    }
  }

  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(exception);
    this.lastResult(iFunxtionException);
    this.localLastResultReceived = true;
  }

  public void setException(Throwable exception) {
    if (this.sender != null) {
      this.sender.setException(exception);
      // this.sender.lastResult(exception);
    } else {
      ((LocalResultCollector) this.rc).setException(exception);
      // this.lastResult(exception);
      logger.info("Unexpected exception during function execution on local node Distributed Region",
          exception);
    }
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
