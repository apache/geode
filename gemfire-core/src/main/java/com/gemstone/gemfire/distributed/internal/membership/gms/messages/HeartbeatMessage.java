package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.internal.Version;

public class HeartbeatMessage extends HighPriorityDistributionMessage {
  /**
   * RequestId identifies the HeartbeatRequestMessage for which this is a response.
   * If it is < 0 this is a periodic heartbeat message.
   */
  int requestId;
  
  public HeartbeatMessage(int id) {
    requestId = id;
  }

  public HeartbeatMessage(){}
  
  public int getRequestId() {
    return requestId;
  }


  @Override
  public int getDSFID() {
    return HEARTBEAT_RESPONSE;
  }

  @Override
  protected void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }
 
  @Override
  public String toString() {
    return getClass().getSimpleName()+" [requestId=" + requestId + "]";
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
