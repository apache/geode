/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
