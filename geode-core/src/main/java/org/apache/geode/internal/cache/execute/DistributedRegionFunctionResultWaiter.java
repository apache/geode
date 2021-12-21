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

import java.util.HashMap;
import java.util.Set;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedRegionFunctionStreamingMessage;

public class DistributedRegionFunctionResultWaiter extends StreamingFunctionOperation {

  private final Set filter;

  private final String regionPath;

  public DistributedRegionFunctionResultWaiter(InternalDistributedSystem sys, String regionPath,
      ResultCollector rc, final Function function, final Set filter, Set recipients,
      final HashMap<InternalDistributedMember, Object> memberArgs, ResultSender resultSender) {
    super(sys, rc, function, memberArgs, recipients, resultSender);
    this.regionPath = regionPath;
    this.filter = filter;
  }

  @Override
  protected DistributionMessage createRequestMessage(Set recipients,
      FunctionStreamingResultCollector processor, boolean isReExecute,
      boolean isFnSerializationReqd) {
    DistributedRegionFunctionStreamingMessage msg = new DistributedRegionFunctionStreamingMessage(
        regionPath, functionObject, processor.getProcessorId(), filter,
        memberArgs.get(recipients.toArray()[0]), isReExecute, isFnSerializationReqd);
    msg.setRecipients(recipients);
    return msg;
  }
}
