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
package org.apache.geode.internal.cache;


import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.LogService;

public class FunctionStreamingOrderedReplyMessage extends FunctionStreamingReplyMessage {
  private static final Logger logger = LogService.getLogger();


  public static void send(InternalDistributedMember recipient, int processorId,
      ReplyException exception, DistributionManager dm, Object result, int msgNum,
      boolean lastMsg) {
    FunctionStreamingOrderedReplyMessage m = new FunctionStreamingOrderedReplyMessage();
    m.processorId = processorId;
    if (exception != null) {
      m.setException(exception);
      if (logger.isDebugEnabled()) {
        logger.debug("Replying with exception: {}", m, exception);
      }
    }
    m.setRecipient(recipient);
    m.msgNum = msgNum;
    m.lastMsg = lastMsg;
    m.result = result;
    dm.putOutgoing(m);
  }

  @Override
  public int getDSFID() {
    return FUNCTION_STREAMING_ORDERED_REPLY_MESSAGE;
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.SERIAL_EXECUTOR;
  }
}
