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

package org.apache.geode.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.FunctionStreamingOrderedReplyMessage;
import org.apache.geode.internal.cache.FunctionStreamingReplyMessage;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.execute.FunctionRemoteContext;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class PartitionedRegionFunctionStreamingMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  private boolean replyLastMsg;

  private int replyMsgNum;

  private Object result;

  private FunctionRemoteContext context;

  public PartitionedRegionFunctionStreamingMessage() {
    super();
  }

  public PartitionedRegionFunctionStreamingMessage(InternalDistributedMember recipient,
      int regionId, ReplyProcessor21 processor, FunctionRemoteContext context) {
    super(recipient, regionId, processor);
    this.context = context;
  }

  public PartitionedRegionFunctionStreamingMessage(DataInput in)
      throws IOException, ClassNotFoundException {
    fromData(in, InternalDataSerializer.createDeserializationContext(in));
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.REGION_FUNCTION_EXECUTION_EXECUTOR;
  }

  /**
   * An operation upon the messages partitioned region. Here we have to execute the function and
   * send the result one by one.
   */
  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion r,
      long startTime) {
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE,
          "PartitionedRegionFunctionResultStreamerMessage operateOnRegion: {}", r.getFullPath());
    }


    if (this.context.getFunction() == null) {
      sendReply(getSender(), getProcessorId(), dm,
          new ReplyException(new FunctionException(
              String.format("Function named %s is not registered to FunctionService",
                  this.context.getFunctionId()))),
          r, startTime);
      return false;
    }
    PartitionedRegionDataStore ds = r.getDataStore();
    if (ds != null) {
      // check if the routingKeyorKeys is null
      // if null call executeOnDataStore otherwise execute on LocalBuckets
      ds.executeOnDataStore(context.getFilter(), context.getFunction(), context.getArgs(),
          getProcessorId(), context.getBucketSet(), context.isReExecute(), this, startTime, null,
          0);

      if (!this.replyLastMsg && context.getFunction().hasResult()) {
        sendReply(getSender(), getProcessorId(), dm,
            new ReplyException(new FunctionException(
                String.format("The function, %s, did not send last result",
                    context.getFunction().getId()))),
            r, startTime);
        return false;
      }
    } else {
      throw new InternalError(
          "PartitionedRegionFunctionResultStreamerMessage sent to an accessor vm :"
              + dm.getId().getId());
    }
    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  /**
   * It sends message one by one until it gets lastMessage. Have to handle scenario when message is
   * large, in that case message will be broken into chunks and then sent across.
   */
  public synchronized boolean sendReplyForOneResult(DistributionManager dm, PartitionedRegion pr,
      long startTime, Object oneResult, boolean lastResult, boolean sendResultsInOrder)
      throws CacheException, ForceReattemptException, InterruptedException {
    if (this.replyLastMsg) {
      return false;
    }
    if (Thread.interrupted())
      throw new InterruptedException();
    int msgNum = this.replyMsgNum;
    this.replyLastMsg = lastResult;

    sendReply(getSender(), this.processorId, dm, null, oneResult, pr, startTime, msgNum, lastResult,
        sendResultsInOrder);

    if (logger.isDebugEnabled()) {
      logger.debug("Sending reply message count: {} to co-ordinating node");
    }

    this.replyMsgNum++;
    return false;
  }


  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, Object result, PartitionedRegion pr, long startTime, int msgNum,
      boolean lastResult, boolean sendResultsInOrder) {
    // if there was an exception, then throw out any data
    if (ex != null) {
      this.result = null;
      this.replyMsgNum = 0;
      this.replyLastMsg = true;
    }
    if (this.replyLastMsg) {
      if (pr != null && startTime > 0) {
        pr.getPrStats().endPartitionMessagesProcessing(startTime);
      }
    }
    if (sendResultsInOrder) {
      FunctionStreamingOrderedReplyMessage.send(member, procId, ex, dm, result, msgNum, lastResult);
    } else {
      FunctionStreamingReplyMessage.send(member, procId, ex, dm, result, msgNum, lastResult);
    }
  }

  @Override
  public int getDSFID() {
    return PR_FUNCTION_STREAMING_MESSAGE;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.context = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(this.context, out);
  }


  @Override
  public boolean canStartRemoteTransaction() {
    return true;
  }

}
