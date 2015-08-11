package com.gemstone.gemfire.distributed.internal.membership.gms.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

public class FindCoordinatorRequest implements DataSerializableFixedID, PeerLocatorRequest {

  public InternalDistributedMember memberID;
  
  public FindCoordinatorRequest(InternalDistributedMember myId) {
    this.memberID = myId;
  }
  
  public FindCoordinatorRequest() {
    // no-arg constructor for serialization
  }

  public InternalDistributedMember getMemberID() {
    return memberID;
  }
  
  @Override
  public String toString() {
    return "FindCoordinatorRequest(memberID="+memberID+")";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return FIND_COORDINATOR_REQ;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.memberID, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.memberID = DataSerializer.readObject(in);

  }

}
