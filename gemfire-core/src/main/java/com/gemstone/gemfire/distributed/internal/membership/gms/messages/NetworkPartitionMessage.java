package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.util.Collection;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

public class NetworkPartitionMessage extends HighPriorityDistributionMessage {
  
  public NetworkPartitionMessage() {
  }
  
  public NetworkPartitionMessage(Collection<InternalDistributedMember> recipients) {
    setRecipients(recipients);
  }
  
  @Override
  public int getDSFID() {
    return NETWORK_PARTITION_MESSAGE;
  }

  @Override
  protected void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to be executed");
  }
  
}
