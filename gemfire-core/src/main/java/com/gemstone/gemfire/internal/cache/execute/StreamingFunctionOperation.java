/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * @author ymahajan
 */
public abstract class StreamingFunctionOperation {

  protected final InternalDistributedSystem sys;

  protected Set recipients = null;

  protected ResultCollector rc;

  protected Function functionObject;

  protected HashMap<InternalDistributedMember, Object> memberArgs;

  protected ResultSender resultSender = null;

  protected ResultCollector reply;

  protected int totalLastMsgRecieved = 0;

  /** Creates a new instance of StreamingOperation */
  public StreamingFunctionOperation(InternalDistributedSystem sys,
      ResultCollector rc, Function function,
      final HashMap<InternalDistributedMember, Object> memberArgs,
      Set recipients, ResultSender resultSender) {
    this.sys = sys;
    this.rc = rc;
    this.functionObject = function;
    this.memberArgs = memberArgs;
    this.recipients = recipients;
    this.resultSender = resultSender;
  }

  /** Creates a new instance of StreamingOperation */
  public StreamingFunctionOperation(InternalDistributedSystem sys,
      ResultCollector rc, Function function, ResultSender resultSender) {
    this.sys = sys;
    this.rc = rc;
    this.functionObject = function;
    this.resultSender = resultSender;
  }

  public void processData(Object result, boolean lastMsg,
      DistributedMember memberID) {
    boolean completelyDone = false;
    if (lastMsg) {
      this.totalLastMsgRecieved++;
    }
    if (this.totalLastMsgRecieved == this.recipients.size()) {
      completelyDone = true;
    }

    if (resultSender instanceof MemberFunctionResultSender) {
      MemberFunctionResultSender rs = (MemberFunctionResultSender)resultSender;
      rs.lastResult(result, completelyDone, this.reply, memberID);
    }
    else {
      if (completelyDone) {
        ((DistributedRegionFunctionResultSender)resultSender).lastResult(
            result, memberID);
      }
      else {
        ((DistributedRegionFunctionResultSender)resultSender).sendResult(
            result, memberID);
      }
    }
  }

  public ResultCollector getFunctionResultFrom(Set recipients,
      Function function, AbstractExecution execution) {
    if (recipients.isEmpty())
      return rc;

    FunctionStreamingResultCollector processor = new FunctionStreamingResultCollector(
        this, this.sys, recipients, rc, function, execution);
    this.reply = processor;
    for (InternalDistributedMember recip : this.memberArgs.keySet()) {
      DistributionMessage m = null;
      if (execution instanceof DistributedRegionFunctionExecutor
          || execution instanceof MultiRegionFunctionExecutor) {
        m = createRequestMessage(Collections.singleton(recip), processor,
            execution.isReExecute(), execution.isFnSerializationReqd());
      }
      else {
        m = createRequestMessage(Collections.singleton(recip), processor,
            false, execution.isFnSerializationReqd());
      }
      this.sys.getDistributionManager().putOutgoing(m);
    }
    return processor;
  }

  protected abstract DistributionMessage createRequestMessage(
      Set<InternalDistributedMember> singleton,
      FunctionStreamingResultCollector processor, boolean isReExecute,
      boolean isFnSerializationReqd); 
}
