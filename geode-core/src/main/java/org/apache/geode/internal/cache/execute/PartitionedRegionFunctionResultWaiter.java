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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.PRFunctionStreamingResultCollector;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionFunctionStreamingMessage;

/**
 * ResultReciever (which could be resultCollector?)will be instantiated and will be used to send
 * messages and receive results from other nodes. It takes a set of nodes to which functionExecution
 * message has to be sent. Creates one message for each and sends it to each of them. Then it gets
 * result in processData where it adds them to the resultCollector.
 *
 *
 */
public class PartitionedRegionFunctionResultWaiter extends StreamingFunctionOperation {

  private ResultCollector reply;

  private final int regionId;

  private Set<InternalDistributedMember> recipients = null;

  public PartitionedRegionFunctionResultWaiter(InternalDistributedSystem sys, int regionId,
      ResultCollector rc, final Function function, PartitionedRegionFunctionResultSender sender) {
    super(sys, rc, function, sender);
    this.regionId = regionId;
  }


  @Override
  public DistributionMessage createRequestMessage(Set<InternalDistributedMember> singleton,
      FunctionStreamingResultCollector processor, boolean isReExecute,
      boolean isFnSerializationReqd) {
    return null;
  }

  /**
   * Returns normally if succeeded to get data, otherwise throws an exception Have to wait outside
   * this function and when getResult() is called. For the time being get the correct results.
   */
  public ResultCollector getPartitionedDataFrom(
      Map<InternalDistributedMember, FunctionRemoteContext> recipMap, PartitionedRegion pr,
      AbstractExecution execution) {

    if (recipMap.isEmpty()) {
      return this.rc;
    }
    Set<InternalDistributedMember> recipientsSet = new HashSet<InternalDistributedMember>();
    for (InternalDistributedMember member : recipMap.keySet()) {
      recipientsSet.add(member);
    }
    this.recipients = recipientsSet;

    PRFunctionStreamingResultCollector processor = new PRFunctionStreamingResultCollector(this,
        this.sys, recipientsSet, this.rc, functionObject, pr, execution);

    this.reply = processor;

    for (Map.Entry<InternalDistributedMember, FunctionRemoteContext> entry : recipMap.entrySet()) {
      FunctionRemoteContext context = entry.getValue();
      DistributionMessage m = createRequestMessage(entry.getKey(), processor, context);
      this.sys.getDistributionManager().putOutgoing(m);
    }
    return processor;
  }

  protected DistributionMessage createRequestMessage(InternalDistributedMember recipient,
      ReplyProcessor21 processor, FunctionRemoteContext context) {
    PartitionedRegionFunctionStreamingMessage msg =
        new PartitionedRegionFunctionStreamingMessage(recipient, this.regionId, processor, context);

    return msg;
  }

  /**
   * This function processes the result data it receives. Adds the result to resultCollector as it
   * gets the objects. On getting the last msg from all the sender it will call endResult on
   * ResultSender.
   */

  public void processData(Object result, boolean lastMsg, DistributedMember memberID) {
    boolean completelyDone = false;
    if (lastMsg) {
      this.totalLastMsgRecieved++;
    }
    if (this.totalLastMsgRecieved == this.recipients.size()) {
      completelyDone = true;
    }
    ((PartitionedRegionFunctionResultSender) resultSender).lastResult(result, completelyDone,
        this.reply, memberID);

  }
}
