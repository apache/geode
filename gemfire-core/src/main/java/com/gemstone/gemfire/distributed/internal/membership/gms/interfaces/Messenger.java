package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

public interface Messenger extends Service {
  /**
   * adds a handler for the given class/interface of messages
   */
  void addHandler(Class c, MessageHandler h);

  /**
   * sends an asynchronous message.  Returns destinations that did not
   * receive the message due to no longer being in the view
   */
  Set<InternalDistributedMember> send(DistributionMessage m) throws IOException;

  /**
   * returns the endpoint ID for this member
   */
  InternalDistributedMember getMemberID();
}
