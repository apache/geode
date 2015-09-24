package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.internal.Version;

public class CheckResponseMessage extends HighPriorityDistributionMessage {
  int requestId;
  
  public CheckResponseMessage(int id) {
    requestId = id;
  }

  public CheckResponseMessage(){}
  
  public int getRequestId() {
    return requestId;
  }


  @Override
  public int getDSFID() {
    return CHECK_RESPONSE;
  }

  @Override
  protected void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }
 
  @Override
  public String toString() {
    return "CheckResponseMessage [requestId=" + requestId + "]";
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
