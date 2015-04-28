/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionFunctionStreamingMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PRFunctionStreamingResultCollector;

/**
 * ResultReciever (which could be resultCollector?)will be instantiated and will be used
 * to send messages and receive results from other nodes.
 * It takes a set of nodes to which functionExecution message has to be sent. Creates one message for each and sends
 * it to each of them. Then it gets result in processData where it adds them to the resultCollector.
 * 
 * @author skumar
 *
 */
public class PartitionedRegionFunctionResultWaiter extends StreamingFunctionOperation {

  private ResultCollector reply;

  private final int regionId;
  
  private Set<InternalDistributedMember> recipients = null;
  
  public PartitionedRegionFunctionResultWaiter(InternalDistributedSystem sys,
      int regionId, ResultCollector rc, final Function function,
      PartitionedRegionFunctionResultSender sender) {
    super(sys,rc,function,sender);
    this.regionId = regionId;
  }

  
  @Override
  public DistributionMessage createRequestMessage(
      Set<InternalDistributedMember> singleton,
      FunctionStreamingResultCollector processor, boolean isReExecute, boolean isFnSerializationReqd) {
    return null;
  }
  /**
   * Returns normally if succeeded to get data, otherwise throws an exception
   * Have to wait outside this function and when getResult() is called. For the
   * time being get the correct results.
   */
  public ResultCollector getPartitionedDataFrom(
      Map<InternalDistributedMember, FunctionRemoteContext> recipMap,
      PartitionedRegion pr, AbstractExecution execution) {

    if (recipMap.isEmpty()){
      return this.rc;
    }
    Set<InternalDistributedMember> recipientsSet = new HashSet<InternalDistributedMember>();
    for (InternalDistributedMember member : recipMap.keySet()){
      recipientsSet.add(member);
    }
    this.recipients = recipientsSet;
    
    PRFunctionStreamingResultCollector processor = new PRFunctionStreamingResultCollector(
        this, this.sys, recipientsSet, this.rc, functionObject, pr, execution);
    
    this.reply = processor;
    
    for (Map.Entry<InternalDistributedMember, FunctionRemoteContext> entry :  recipMap.entrySet()) {
      FunctionRemoteContext context = entry.getValue();
      DistributionMessage m = createRequestMessage(entry.getKey(), processor, context);
      this.sys.getDistributionManager().putOutgoing(m);
    }
    return processor;
  }

  protected DistributionMessage createRequestMessage(
      InternalDistributedMember recipient, ReplyProcessor21 processor,
      FunctionRemoteContext context) {

    PartitionedRegionFunctionStreamingMessage msg = new PartitionedRegionFunctionStreamingMessage(
        recipient, this.regionId, processor,context);

    return msg;
  }

  /**
   * This function processes the result data it receives. Adds the result to
   * resultCollector as it gets the objects. On getting the last msg from all
   * the sender it will call endResult on ResultSender.
   */

  public void processData(Object result, boolean lastMsg,
      DistributedMember memberID) {
    boolean completelyDone = false;
    if (lastMsg) {
      this.totalLastMsgRecieved++;
    }
    if (this.totalLastMsgRecieved == this.recipients.size()) {
      completelyDone = true;
    }
    ((PartitionedRegionFunctionResultSender)resultSender).lastResult(
        result, completelyDone, this.reply, memberID);

  }  
}
