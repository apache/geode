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

import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionFunctionStreamingMessage;
import org.apache.geode.internal.logging.LogService;

/**
 * ResultSender needs ResultCollector in which to add results one by one. In case of localExecution
 * it just adds result to the resultCollector whereas for remote cases it takes help of
 * PRFunctionExecutionStreamer to send results to the calling node. The results will be received in
 * the ResultReceiver. ResultSender will be instantiated in executeOnDatastore and set in
 * FunctionContext.
 *
 */

public class PartitionedRegionFunctionResultSender implements InternalResultSender {

  private static final Logger logger = LogService.getLogger();

  PartitionedRegionFunctionStreamingMessage msg = null;

  private final DistributionManager dm;

  private final PartitionedRegion pr;

  private final long time;

  private final boolean forwardExceptions;

  private ResultCollector rc;

  private ServerToClientFunctionResultSender serverSender;

  private boolean localLastResultReceived = false;

  private boolean onlyLocal = false;

  private boolean onlyRemote = false;

  private boolean completelyDoneFromRemote = false;

  private final Function function;

  private boolean enableOrderedResultStreming;

  private Set<Integer> bucketSet;

  private BucketMovedException bme;


  public Version getClientVersion() {
    if (serverSender != null && serverSender.sc != null) { // is a client-server connection
      return serverSender.sc.getClientVersion();
    }
    return null;
  }

  /**
   * Have to combine next two constructor in one and make a new class which will send Results back.
   *
   */
  public PartitionedRegionFunctionResultSender(DistributionManager dm, PartitionedRegion pr,
      long time, PartitionedRegionFunctionStreamingMessage msg, Function function,
      Set<Integer> bucketSet) {
    this.msg = msg;
    this.dm = dm;
    this.pr = pr;
    this.time = time;
    this.function = function;
    this.bucketSet = bucketSet;

    forwardExceptions = false;
  }

  /**
   * Have to combine next two constructor in one and make a new class which will send Results back.
   *
   */
  public PartitionedRegionFunctionResultSender(DistributionManager dm,
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

  private void checkForBucketMovement(Object oneResult) {
    if (!(forwardExceptions && oneResult instanceof Throwable)
        && !pr.getDataStore().areAllBucketsHosted(bucketSet)) {
      // making sure that we send all the local results first
      // before sending this exception to client
      bme = new BucketMovedException(
          "Bucket migrated to another node. Please retry.");
      if (function.isHA()) {
        throw bme;
      }
    }


  }

  // this must be getting called directly from function
  public void lastResult(Object oneResult) {
    if (!this.function.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    // this could be done before doing end result
    // so that client receives all the results before

    if (this.serverSender != null) { // Client-Server
      if (this.localLastResultReceived) {
        return;
      }
      if (onlyLocal) {
        checkForBucketMovement(oneResult);
        if (bme != null) {
          clientSend(oneResult, dm.getDistributionManagerId());
          lastClientSend(dm.getDistributionManagerId(), bme);
        } else {
          lastClientSend(dm.getDistributionManagerId(), oneResult);
        }
        this.rc.endResults();
        this.localLastResultReceived = true;

      } else {
        // call a synchronized method as local node is also waiting to send lastResult
        lastResult(oneResult, rc, false, true, dm.getDistributionManagerId());
      }
    } else { // P2P

      if (this.msg != null) {
        checkForBucketMovement(oneResult);
        try {
          if (this.bme != null) {
            this.msg.sendReplyForOneResult(dm, pr, time, oneResult, false,
                enableOrderedResultStreming);
            throw bme;
          } else {
            this.msg.sendReplyForOneResult(dm, pr, time, oneResult, true,
                enableOrderedResultStreming);
          }

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
          checkForBucketMovement(oneResult);
          if (bme != null) {
            this.rc.addResult(dm.getDistributionManagerId(), oneResult);
            this.rc.addResult(dm.getDistributionManagerId(), bme);
          } else {
            this.rc.addResult(dm.getDistributionManagerId(), oneResult);
          }
          // exception thrown will do end result
          this.rc.endResults();
          this.localLastResultReceived = true;
        } else {
          // call a synchronized method as local node is also waiting to send lastResult
          lastResult(oneResult, rc, false, true, dm.getDistributionManagerId());
        }
        FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
      }
      // incrementing result sent stats.
      // Bug : remote node as well as local node calls this method to send
      // the result When the remote nodes are added to the local result collector at that
      // time the stats for the result sent is again incremented : Once the PR team comes with the
      // concept of the Streaming FunctionOperation
      // for the partitioned Region then it will be simple to fix this problem.
      FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();
    }
  }

  private synchronized void lastResult(Object oneResult, ResultCollector collector,
      boolean lastRemoteResult, boolean lastLocalResult, DistributedMember memberID) {


    boolean completedLocal = lastLocalResult || this.localLastResultReceived;
    if (lastRemoteResult) {
      this.completelyDoneFromRemote = true;
    }


    if (this.serverSender != null) { // Client-Server
      if (this.completelyDoneFromRemote && completedLocal) {
        if (lastLocalResult) {
          checkForBucketMovement(oneResult);
          if (bme != null) {
            clientSend(oneResult, dm.getDistributionManagerId());
            lastClientSend(dm.getDistributionManagerId(), bme);
          } else {
            lastClientSend(memberID, oneResult);
          }
        } else {
          lastClientSend(memberID, oneResult);
        }

        collector.endResults();
      } else {
        if (lastLocalResult) {
          checkForBucketMovement(oneResult);
          if (bme != null) {
            clientSend(oneResult, memberID);
            clientSend(bme, memberID);
          } else {
            clientSend(oneResult, memberID);
          }
        } else {
          clientSend(oneResult, memberID);
        }

      }
    } else { // P2P
      if (this.completelyDoneFromRemote && completedLocal) {
        if (lastLocalResult) {
          checkForBucketMovement(oneResult);
          if (bme != null) {
            collector.addResult(memberID, oneResult);
            collector.addResult(memberID, bme);
          } else {
            collector.addResult(memberID, oneResult);
          }
        } else {
          collector.addResult(memberID, oneResult);
        }
        collector.endResults();
      } else {
        if (lastLocalResult) {
          checkForBucketMovement(oneResult);
          if (bme != null) {
            collector.addResult(memberID, oneResult);
            collector.addResult(memberID, bme);
          } else {
            collector.addResult(memberID, oneResult);
          }
        } else {
          collector.addResult(memberID, oneResult);
        }
      }
    }
    if (lastLocalResult) {
      this.localLastResultReceived = true;
    }
  }

  public synchronized void lastResult(Object oneResult, boolean completelyDoneFromRemote,
      ResultCollector reply, DistributedMember memberID) {
    logger.debug("PartitionedRegionFunctionResultSender Sending lastResult {}", oneResult);

    if (this.serverSender != null) { // Client-Server

      if (completelyDoneFromRemote) {
        if (this.onlyRemote) {
          lastClientSend(memberID, oneResult);
          reply.endResults();
        } else {
          // call a synchronized method as local node is also waiting to send lastResult
          lastResult(oneResult, reply, true, false, memberID);
        }
      } else {
        clientSend(oneResult, memberID);
      }
    } else {
      if (completelyDoneFromRemote) {
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
      if (this.dm == null) {
        FunctionStats.getFunctionStats(function.getId()).incResultsReceived();
      } else {
        FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
      }
    }
    if (this.dm == null) {
      FunctionStats.getFunctionStats(function.getId()).incResultsReturned();
    } else {
      FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();
    }
  }

  public void sendResult(Object oneResult) {
    if (!this.function.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (this.serverSender != null) {
      logger.debug(
          "PartitionedRegionFunctionResultSender sending result from local node to client {}",
          oneResult);
      clientSend(oneResult, dm.getDistributionManagerId());
    } else { // P2P
      if (this.msg != null) {
        try {
          logger.debug("PartitionedRegionFunctionResultSender sending result from remote node {}",
              oneResult);
          this.msg.sendReplyForOneResult(dm, pr, time, oneResult, false,
              enableOrderedResultStreming);
        } catch (ForceReattemptException e) {
          throw new FunctionException(e);
        } catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      } else {
        logger.debug(
            "PartitionedRegionFunctionResultSender adding result to ResultCollector on local node {}",
            oneResult);
        this.rc.addResult(dm.getDistributionManagerId(), oneResult);
        FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
      }
      // incrementing result sent stats.
      FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();
    }
  }

  private void clientSend(Object oneResult, DistributedMember memberID) {
    this.serverSender.sendResult(oneResult, memberID);
  }

  private void lastClientSend(DistributedMember memberID, Object lastResult) {
    this.serverSender.lastResult(lastResult, memberID);
  }

  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(exception);
    this.lastResult(iFunxtionException);
    this.localLastResultReceived = true;
  }

  public void setException(Throwable exception) {
    if (this.serverSender != null) {
      this.serverSender.setException(exception);
    } else {
      ((LocalResultCollector) this.rc).setException(exception);
      logger.info("Unexpected exception during function execution on local node Partitioned Region",
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
    return localLastResultReceived;
  }

}
