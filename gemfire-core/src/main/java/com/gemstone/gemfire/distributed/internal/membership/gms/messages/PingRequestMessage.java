package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;

public class PingRequestMessage extends HighPriorityDistributionMessage{

  int requestId;
  InternalDistributedMember target;
  
  public PingRequestMessage(InternalDistributedMember neighbour, int id) {
    requestId = id;
    this.target = neighbour;
  }
  
  public PingRequestMessage(){}
  
  public InternalDistributedMember getTarget() {
    return target;
  }
  
  @Override
  public int getDSFID() {
    return PING_REQUEST;
  }

  @Override
  protected void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }   

  @Override
  public String toString() {
    return "PingRequestMessage [requestId=" + requestId + "] from " + getSender();
  }

  public int getRequestId() {
    return requestId;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }  
  
  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(requestId);
    DataSerializer.writeObject(target, out);
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    requestId = in.readInt();
    target = DataSerializer.readObject(in);
  }
}
