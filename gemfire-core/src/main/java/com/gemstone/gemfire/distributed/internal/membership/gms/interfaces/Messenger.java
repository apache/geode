package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import java.util.Set;

import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.QuorumChecker;

public interface Messenger extends Service {
  /**
   * adds a handler for the given class/interface of messages
   */
  void addHandler(Class c, MessageHandler h);

  /**
   * sends an asynchronous message.  Returns destinations that did not
   * receive the message due to no longer being in the view
   */
  Set<InternalDistributedMember> send(DistributionMessage m);

  /**
   * sends an asynchronous message.  Returns destinations that did not
   * receive the message due to no longer being in the view.  Does
   * not guarantee delivery of the message (no retransmissions)
   */
  Set<InternalDistributedMember> sendUnreliably(DistributionMessage m);

  /**
   * returns the endpoint ID for this member
   */
  InternalDistributedMember getMemberID();
  
  /**
   * retrieves the quorum checker that is used during auto-reconnect attempts
   */
  QuorumChecker getQuorumChecker();
  
  /**
   * test whether multicast is not only turned on but is working
   * @return true multicast is enabled and working
   */
  boolean testMulticast(long timeout) throws InterruptedException;
}
