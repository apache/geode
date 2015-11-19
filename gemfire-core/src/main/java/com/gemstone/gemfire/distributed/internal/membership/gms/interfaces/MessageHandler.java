package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import com.gemstone.gemfire.distributed.internal.DistributionMessage;

/**
 * MessageHandler processes a message received by Messenger.
 * Handlers are registered with Messenger to consume specific
 * classes of message.
 */
public interface MessageHandler {
  
  public void processMessage(DistributionMessage m);

}
