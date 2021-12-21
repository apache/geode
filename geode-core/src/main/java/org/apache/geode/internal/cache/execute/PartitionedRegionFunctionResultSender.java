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
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionFunctionStreamingMessage;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.log4j.api.LogService;

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

  private final int[] bucketArray;

  private BucketMovedException bme;


  public KnownVersion getClientVersion() {
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
      int[] bucketArray) {
    this.msg = msg;
    this.dm = dm;
    this.pr = pr;
    this.time = time;
    this.function = function;
    this.bucketArray = bucketArray;

    forwardExceptions = false;
  }

  /**
   * Have to combine next two constructor in one and make a new class which will send Results back.
   *
   */
  public PartitionedRegionFunctionResultSender(DistributionManager dm,
      PartitionedRegion partitionedRegion, long time, ResultCollector rc,
      ServerToClientFunctionResultSender sender, boolean onlyLocal, boolean onlyRemote,
      boolean forwardExceptions, Function function, int[] bucketArray) {
    this.dm = dm;
    pr = partitionedRegion;
    this.time = time;
    this.rc = rc;
    serverSender = sender;
    this.onlyLocal = onlyLocal;
    this.onlyRemote = onlyRemote;
    this.forwardExceptions = forwardExceptions;
    this.function = function;
    this.bucketArray = bucketArray;
  }

  private void checkForBucketMovement(Object oneResult) {
    if (!(forwardExceptions && oneResult instanceof Throwable)
        && !pr.getDataStore().areAllBucketsHosted(bucketArray)) {
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
  @Override
  public void lastResult(Object oneResult) {
    if (!function.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    // this could be done before doing end result
    // so that client receives all the results before

    if (serverSender != null) { // Client-Server
      if (localLastResultReceived) {
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
        rc.endResults();
        localLastResultReceived = true;

      } else {
        // call a synchronized method as local node is also waiting to send lastResult
        lastResult(oneResult, rc, false, true, dm.getDistributionManagerId());
      }
    } else { // P2P

      if (msg != null) {
        checkForBucketMovement(oneResult);
        try {
          if (bme != null) {
            msg.sendReplyForOneResult(dm, pr, time, oneResult, false,
                enableOrderedResultStreming);
            throw bme;
          } else {
            msg.sendReplyForOneResult(dm, pr, time, oneResult, true,
                enableOrderedResultStreming);
          }

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
          checkForBucketMovement(oneResult);
          if (bme != null) {
            rc.addResult(dm.getDistributionManagerId(), oneResult);
            rc.addResult(dm.getDistributionManagerId(), bme);
          } else {
            rc.addResult(dm.getDistributionManagerId(), oneResult);
          }
          // exception thrown will do end result
          rc.endResults();
          localLastResultReceived = true;
        } else {
          // call a synchronized method as local node is also waiting to send lastResult
          lastResult(oneResult, rc, false, true, dm.getDistributionManagerId());
        }
        FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
            .incResultsReceived();
      }
      // incrementing result sent stats.
      // Bug : remote node as well as local node calls this method to send
      // the result When the remote nodes are added to the local result collector at that
      // time the stats for the result sent is again incremented : Once the PR team comes with the
      // concept of the Streaming FunctionOperation
      // for the partitioned Region then it will be simple to fix this problem.
      FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
          .incResultsReturned();
    }
  }

  private synchronized void lastResult(Object oneResult, ResultCollector collector,
      boolean lastRemoteResult, boolean lastLocalResult, DistributedMember memberID) {


    boolean completedLocal = lastLocalResult || localLastResultReceived;
    if (lastRemoteResult) {
      completelyDoneFromRemote = true;
    }


    if (serverSender != null) { // Client-Server
      if (completelyDoneFromRemote && completedLocal) {
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
      if (completelyDoneFromRemote && completedLocal) {
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
      localLastResultReceived = true;
    }
  }

  public synchronized void lastResult(Object oneResult, boolean completelyDoneFromRemote,
      ResultCollector reply, DistributedMember memberID) {
    logger.debug("PartitionedRegionFunctionResultSender Sending lastResult {}", oneResult);

    if (serverSender != null) { // Client-Server

      if (completelyDoneFromRemote) {
        if (onlyRemote) {
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
      if (dm == null) {
        FunctionStatsManager.getFunctionStats(function.getId()).incResultsReceived();
      } else {
        FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
            .incResultsReceived();
      }
    }
    if (dm == null) {
      FunctionStatsManager.getFunctionStats(function.getId()).incResultsReturned();
    } else {
      FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
          .incResultsReturned();
    }
  }

  @Override
  public void sendResult(Object oneResult) {
    if (!function.hasResult()) {
      throw new IllegalStateException(
          String.format("Cannot %s result as the Function#hasResult() is false",
              "send"));
    }
    if (serverSender != null) {
      logger.debug(
          "PartitionedRegionFunctionResultSender sending result from local node to client {}",
          oneResult);
      clientSend(oneResult, dm.getDistributionManagerId());
    } else { // P2P
      if (msg != null) {
        try {
          logger.debug("PartitionedRegionFunctionResultSender sending result from remote node {}",
              oneResult);
          msg.sendReplyForOneResult(dm, pr, time, oneResult, false,
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
        rc.addResult(dm.getDistributionManagerId(), oneResult);
        FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
            .incResultsReceived();
      }
      // incrementing result sent stats.
      FunctionStatsManager.getFunctionStats(function.getId(), dm.getSystem())
          .incResultsReturned();
    }
  }

  private void clientSend(Object oneResult, DistributedMember memberID) {
    serverSender.sendResult(oneResult, memberID);
  }

  private void lastClientSend(DistributedMember memberID, Object lastResult) {
    serverSender.lastResult(lastResult, memberID);
  }

  @Override
  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(exception);
    lastResult(iFunxtionException);
    localLastResultReceived = true;
  }

  @Override
  public void setException(Throwable exception) {
    if (serverSender != null) {
      serverSender.setException(exception);
    } else {
      ((LocalResultCollector) rc).setException(exception);
      logger.info("Unexpected exception during function execution on local node Partitioned Region",
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
