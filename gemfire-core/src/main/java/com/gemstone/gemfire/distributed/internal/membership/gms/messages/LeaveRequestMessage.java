package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;

public class LeaveRequestMessage extends HighPriorityDistributionMessage {
  private InternalDistributedMember memberID;
  
  public LeaveRequestMessage(InternalDistributedMember coord, InternalDistributedMember id) {
    super();
    setRecipient(coord);
    this.memberID = id;
  }
  
  public LeaveRequestMessage() {
    // no-arg constructor for serialization
  }

  @Override
  public int getDSFID() {
    return LEAVE_REQUEST_MESSAGE;
  }
  
  @Override
  public void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool"); 
  }

  public InternalDistributedMember getMemberID() {
    return memberID;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(memberID, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    memberID = DataSerializer.readObject(in);
  }

}
