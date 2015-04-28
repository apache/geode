/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.util.HashMap;
import java.util.Map;
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
public class MultiRegionFunctionResultWaiter extends
    StreamingFunctionOperation {

  private Map<InternalDistributedMember, Set<String>> memberToRegions = null ;;

  public MultiRegionFunctionResultWaiter(InternalDistributedSystem sys,
      ResultCollector rc, final Function function, Set recipients,
      final HashMap<InternalDistributedMember, Object> memberArgs,
      ResultSender resultSender,
      Map<InternalDistributedMember, Set<String>> memberToRegions) {
    super(sys, rc, function, memberArgs, recipients,resultSender);
    this.memberToRegions = memberToRegions;
  }
  
  @Override
  protected DistributionMessage createRequestMessage(Set recipients,
      FunctionStreamingResultCollector processor, boolean isReExecute,
      boolean isFnSerializationReqd) {
    InternalDistributedMember target = (InternalDistributedMember)recipients.toArray()[0]; 
    MemberFunctionStreamingMessage msg = new MemberFunctionStreamingMessage(
        this.functionObject, processor.getProcessorId(),
        memberArgs.get(target), isFnSerializationReqd, this.memberToRegions
            .get(target), isReExecute);
    msg.setRecipients(recipients);
    return msg;
  }
}
