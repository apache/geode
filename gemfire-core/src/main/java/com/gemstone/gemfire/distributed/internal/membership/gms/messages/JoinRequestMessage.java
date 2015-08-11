package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;

public class JoinRequestMessage extends HighPriorityDistributionMessage {
  private InternalDistributedMember memberID;
  private Object credentials;

  
  public JoinRequestMessage(InternalDistributedMember coord,
      InternalDistributedMember id, Object credentials) {
    super();
    setRecipient(coord);
    this.memberID = id;
    this.credentials = credentials;
  }
  
  public JoinRequestMessage() {
    // no-arg constructor for serialization
  }

  @Override
  public int getDSFID() {
    return JOIN_REQUEST;
  }
  
  @Override
  public void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool"); 
  }

  public InternalDistributedMember getMemberID() {
    return memberID;
  }

  public Object getCredentials() {
    return credentials;
  }
  
  @Override
  public String toString() {
    return getShortClassName() + "(" + memberID + (credentials==null? ")" : "; with credentials)");
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(memberID, out);
    DataSerializer.writeObject(credentials, out);
    // preserve the multicast setting so the receiver can tell
    // if this is a mcast join request
    out.writeBoolean(getMulticast());
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    memberID = DataSerializer.readObject(in);
    credentials = DataSerializer.readObject(in);
    setMulticast(in.readBoolean());
  }

}
