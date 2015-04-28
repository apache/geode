/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;

/**
 * A message that can reply directly to the sender
 * 
 * 
 * @author dsmith
 *
 */
public interface DirectReplyMessage {
  /**
   * Called on the sending side. This reply processor
   * will be handed the responses from the message.
   */
  DirectReplyProcessor getDirectReplyProcessor();
  
  /**
   * Indicates whether the message could send an acknowledgement
   * back on the connection the request was sent on.  This flag 
   * only takes effect when {@link com.gemstone.gemfire.distributed.DistributedSystem#setThreadsSocketPolicy(boolean)} 
   * is set to <code>false</code>
   * If this flag is set to true, the process method <b> must </b> reply
   * by calling {@link DistributionMessage#getReplySender(com.gemstone.gemfire.distributed.internal.DM)} and using
   * the result to send the reply. the ReplySender determines whether to reply
   * directly or through the shared channel.
   * @return true if a direct acknowledgement is allowed
   * @see com.gemstone.gemfire.distributed.internal.direct.DirectChannel
   */
  boolean supportsDirectAck();
  
  /**
   * Called on the sending side. This method is invoked
   * if the message will end up using the shared channel.
   * The message is expected to register the processor
   * and send it's id to the receiving side.
   * 
   */
  void registerProcessor();
}
