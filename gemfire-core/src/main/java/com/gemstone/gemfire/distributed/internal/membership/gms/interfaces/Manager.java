package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import java.io.NotSerializableException;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetMember;

/**
 * Manager presents the GMS services to the outside world and
 * handles startup/shutdown race conditions.  It is also the
 * default MessageHandler
 */
public interface Manager extends Service, MessageHandler {

  /**
   * After all services have been started this is used to
   * join the distributed system
   */
  void joinDistributedSystem();

  /**
   * Sends a message using a selected distribution channel
   * (e.g. Messenger or DirectChannel)
   */
  void send(DistributionMessage m) throws NotSerializableException;

  InternalDistributedMember getMemberID(NetMember m);

  void forceDisconnect(String reason);

  void addSurpriseMemberForTesting(DistributedMember mbr, long birthTime);

  boolean isShunned(DistributedMember mbr);

  DistributedMember getLeadMember();

  DistributedMember getCoordinator();
  
  boolean isMulticastAllowed();
  
  void setShutdownCause(Exception e);
  
  Throwable getShutdownCause();
  
  boolean shutdownInProgress();
  
  void membershipFailure(String message, Exception cause);
}
