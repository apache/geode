/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;


import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * 
 * @author ymahajan
 *
 */
public class FunctionStreamingOrderedReplyMessage extends
    FunctionStreamingReplyMessage {
  private static final Logger logger = LogService.getLogger();


  public static void send(InternalDistributedMember recipient, int processorId,
      ReplyException exception, DM dm, Object result, int msgNum,
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

  final public int getProcessorType() {
    return DistributionManager.SERIAL_EXECUTOR;
  }
}
