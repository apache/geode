package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.internal.Version;

public class PingResponseMessage extends HighPriorityDistributionMessage {
  int requestId;
  
  public PingResponseMessage(int id) {
    requestId = id;
  }

  public PingResponseMessage(){}
  
  public int getRequestId() {
    return requestId;
  }


  @Override
  public int getDSFID() {
    return PING_RESPONSE;
  }

  @Override
  protected void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }
 
  @Override
  public String toString() {
    return "PingResponseMessage [requestId=" + requestId + "] from " + getSender();
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }  

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(requestId);
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    requestId = in.readInt();
  }
}
