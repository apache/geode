package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

public class JoinResponseMessage extends HighPriorityDistributionMessage {

  private NetView currentView;
  private String rejectionMessage;
  private InternalDistributedMember memberID;
  private Object messengerData;
  
  public JoinResponseMessage(InternalDistributedMember memberID, NetView view) {
    this.currentView = view;
    this.memberID = memberID;
    setRecipient(memberID);
  }
  
  public JoinResponseMessage(String rejectionMessage) {
    this.rejectionMessage = rejectionMessage;
  }
  
  public JoinResponseMessage() {
    // no-arg constructor for serialization
  }

  public NetView getCurrentView() {
    return currentView;
  }
  
  public InternalDistributedMember getMemberID() {
    return memberID;
  }

  public String getRejectionMessage() {
    return rejectionMessage;
  }
  
  public Object getMessengerData() {
    return this.messengerData;
  }
  
  public void setMessengerData(Object data) {
    this.messengerData = data;
  }

  @Override
  public void process(DistributionManager dm) {
    throw new IllegalStateException("JoinResponse is not intended to be executed");
  }
  
  @Override
  public String toString() {
    return getShortClassName() + "("+memberID + "; "
        + (currentView==null? "" : currentView.toString())
        + (rejectionMessage==null? "" : rejectionMessage)
        + ")";
  }
  
  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return JOIN_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(currentView, out);
    DataSerializer.writeObject(memberID, out);
    DataSerializer.writeString(rejectionMessage, out);
    DataSerializer.writeObject(messengerData, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    currentView = DataSerializer.readObject(in);
    memberID = DataSerializer.readObject(in);
    rejectionMessage = DataSerializer.readString(in);
    messengerData = DataSerializer.readObject(in);
  }

}
