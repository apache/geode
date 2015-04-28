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
import com.gemstone.gemfire.internal.cache.MemberFunctionStreamingMessage;
/**
 * 
 * @author ymahajan
 *
 */
public class MemberFunctionResultWaiter extends StreamingFunctionOperation {

  public MemberFunctionResultWaiter(InternalDistributedSystem sys,
      ResultCollector rc, final Function function,
      final HashMap<InternalDistributedMember, Object> memberArgs,
      Set recipients, ResultSender rs) {
    super(sys, rc, function, memberArgs, recipients, rs);
  } 
  
  protected DistributionMessage createRequestMessage(Set recipients,
      FunctionStreamingResultCollector processor, boolean isReExecute, boolean isFnSerializationReqd) {
    MemberFunctionStreamingMessage msg = new MemberFunctionStreamingMessage(
        this.functionObject, processor.getProcessorId(), memberArgs
            .get(recipients.toArray()[0]), isFnSerializationReqd, isReExecute);
    msg.setRecipients(recipients);
    return msg;
  }
}
