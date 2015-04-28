/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.util.HashMap;
import java.util.Set;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.DistributedRegionFunctionStreamingMessage;
/**
 * 
 * @author ymahajan
 *
 */
public class DistributedRegionFunctionResultWaiter extends
    StreamingFunctionOperation {

  private Set filter;

  private String regionPath;

  public DistributedRegionFunctionResultWaiter(InternalDistributedSystem sys,
      String regionPath, ResultCollector rc, final Function function,
      final Set filter, Set recipients,
      final HashMap<InternalDistributedMember, Object> memberArgs,
      ResultSender resultSender) {
    super(sys, rc, function, memberArgs, recipients,resultSender);
    this.regionPath = regionPath;
    this.filter = filter;
  }
  
  @Override
  protected DistributionMessage createRequestMessage(Set recipients,
      FunctionStreamingResultCollector processor, boolean isReExecute, boolean isFnSerializationReqd) {
    DistributedRegionFunctionStreamingMessage msg = new DistributedRegionFunctionStreamingMessage(
        this.regionPath, this.functionObject, processor.getProcessorId(),
        this.filter, this.memberArgs.get(recipients.toArray()[0]), isReExecute, isFnSerializationReqd);
    msg.setRecipients(recipients);
    return msg;
  }
}
