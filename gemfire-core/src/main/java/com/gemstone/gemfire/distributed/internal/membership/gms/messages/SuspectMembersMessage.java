package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;

public class SuspectMembersMessage extends HighPriorityDistributionMessage {
  final List<SuspectRequest> suspectRequests;

  public SuspectMembersMessage(List<InternalDistributedMember> recipient, List<SuspectRequest> s) {
    super();
    setRecipients(recipient);
    this.suspectRequests = s;
  }

  public SuspectMembersMessage() {
    // no-arg constructor for serialization
    suspectRequests = new ArrayList<SuspectRequest>();
  }

  @Override
  public int getDSFID() {
    return SUSPECT_MEMBERS_MESSAGE;
  }

  @Override
  public void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }

  public List<SuspectRequest> getMembers() {
    return suspectRequests;
  }

  @Override
  public String toString() {
    return "SuspectMembersMessage [suspectRequests=" + suspectRequests + "]";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    if (suspectRequests != null) {
      out.writeInt(suspectRequests.size());
      for (SuspectRequest sr: suspectRequests) {        
        DataSerializer.writeObject(sr.getSuspectMember(), out);
        DataSerializer.writeString(sr.getReason(), out);
      }
    } else {
      out.writeInt(0);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      SuspectRequest sr = new SuspectRequest((InternalDistributedMember) DataSerializer.readObject(in), DataSerializer.readString(in));
      suspectRequests.add(sr);
    }
  }

}
