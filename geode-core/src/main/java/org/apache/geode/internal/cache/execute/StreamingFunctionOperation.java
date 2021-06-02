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

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public abstract class StreamingFunctionOperation {

  protected final InternalDistributedSystem sys;

  protected Set recipients = null;

  protected ResultCollector rc;

  protected Function functionObject;

  protected HashMap<InternalDistributedMember, Object> memberArgs;

  protected ResultSender resultSender = null;

  protected ResultCollector reply;

  protected int totalLastMsgReceived = 0;

  /** Creates a new instance of StreamingOperation */
  public StreamingFunctionOperation(InternalDistributedSystem sys, ResultCollector rc,
      Function function, final HashMap<InternalDistributedMember, Object> memberArgs,
      Set recipients, ResultSender resultSender) {
    this.sys = sys;
    this.rc = rc;
    this.functionObject = function;
    this.memberArgs = memberArgs;
    this.recipients = recipients;
    this.resultSender = resultSender;
  }

  /** Creates a new instance of StreamingOperation */
  public StreamingFunctionOperation(InternalDistributedSystem sys, ResultCollector rc,
      Function function, ResultSender resultSender) {
    this.sys = sys;
    this.rc = rc;
    this.functionObject = function;
    this.resultSender = resultSender;
  }

  public void processData(Object result, boolean lastMsg, DistributedMember memberID) {
    boolean completelyDone = false;
    if (lastMsg) {
      this.totalLastMsgReceived++;
    }
    if (this.totalLastMsgReceived == this.recipients.size()) {
      completelyDone = true;
    }

    if (resultSender instanceof MemberFunctionResultSender) {
      MemberFunctionResultSender rs = (MemberFunctionResultSender) resultSender;
      rs.lastResult(result, completelyDone, this.reply, memberID);
    } else {
      if (completelyDone) {
        ((DistributedRegionFunctionResultSender) resultSender).lastResult(result, memberID);
      } else {
        ((DistributedRegionFunctionResultSender) resultSender).sendResult(result, memberID);
      }
    }
  }

  public ResultCollector getFunctionResultFrom(Set recipients, Function function,
      AbstractExecution execution) {
    if (recipients.isEmpty()) {
      return rc;
    }

    FunctionStreamingResultCollector processor =
        new FunctionStreamingResultCollector(this, this.sys, recipients, rc, function, execution);
    this.reply = processor;
    for (InternalDistributedMember recip : this.memberArgs.keySet()) {
      DistributionMessage m = null;
      if (execution instanceof DistributedRegionFunctionExecutor
          || execution instanceof MultiRegionFunctionExecutor) {
        m = createRequestMessage(Collections.singleton(recip), processor, execution.isReExecute(),
            execution.isFnSerializationReqd());
      } else {
        m = createRequestMessage(Collections.singleton(recip), processor, false,
            execution.isFnSerializationReqd());
      }
      this.sys.getDistributionManager().putOutgoing(m);
    }
    return processor;
  }

  protected abstract DistributionMessage createRequestMessage(
      Set<InternalDistributedMember> singleton, FunctionStreamingResultCollector processor,
      boolean isReExecute, boolean isFnSerializationReqd);
}
