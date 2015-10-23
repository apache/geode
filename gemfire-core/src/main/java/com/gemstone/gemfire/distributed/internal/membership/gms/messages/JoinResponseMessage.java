package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;

public class JoinResponseMessage extends HighPriorityDistributionMessage {

  private NetView currentView;
  private String rejectionMessage;
  private InternalDistributedMember memberID;
  private Object messengerData;
  private boolean becomeCoordinator;
  private List<Integer> portsForMembers = Collections.<Integer>emptyList();
  
  public JoinResponseMessage(InternalDistributedMember memberID, NetView view) {
    this.currentView = view;
    this.memberID = memberID;
    setRecipient(memberID);
  }
  
  public JoinResponseMessage(InternalDistributedMember memberID, NetView view, boolean becomeCoordinator) {
    this.currentView = view;
    this.memberID = memberID;
    setRecipient(memberID);
    this.becomeCoordinator = becomeCoordinator;
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
  
  public boolean getBecomeCoordinator() {
    return becomeCoordinator;
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
        + "portsForMembers: " + portsForMembers
        + (rejectionMessage==null? "" : ("; "+rejectionMessage))
        + (becomeCoordinator? "; becomeCoordinator" : "")
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

  private void writeListOfInteger(List<Integer> list, DataOutput out) throws IOException {
    int size;
    if (list == null) {
      size = -1;
    } else {
      size = list.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (size > 0) {
      for (int i = 0; i < size; i++) {
        out.writeInt(list.get(i).intValue());
      }
    }
  }
  
  private List<Integer> readListOfInteger(DataInput in) throws IOException {
    int size = InternalDataSerializer.readArrayLength(in);
    if (size > 0) {
      List<Integer> list = new ArrayList<Integer>(size);
      for (int i = 0; i < size; i++) {
        list.add(Integer.valueOf(in.readInt()));
      }
      return list;
    }
    else if (size == 0) {
      return Collections.<Integer>emptyList();
    }
    else {
      return null;
    }
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(currentView, out);
    DataSerializer.writeObject(memberID, out);
    writeListOfInteger(portsForMembers, out);
    out.writeBoolean(becomeCoordinator);
    DataSerializer.writeString(rejectionMessage, out);
    DataSerializer.writeObject(messengerData, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    currentView = DataSerializer.readObject(in);
    memberID = DataSerializer.readObject(in);
    portsForMembers = readListOfInteger(in);
    becomeCoordinator = in.readBoolean();
    rejectionMessage = DataSerializer.readString(in);
    messengerData = DataSerializer.readObject(in);
  }

  public void setPortsForMembers(List<Integer> portsForMembers) {
    this.portsForMembers = portsForMembers;
  }

  public List<Integer> getPortsForMembers() {
    return this.portsForMembers;
  }
}
