/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal.membership.gms.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

public class FindCoordinatorRequest extends HighPriorityDistributionMessage
  implements PeerLocatorRequest {

  private InternalDistributedMember memberID;
  private Collection<InternalDistributedMember> rejectedCoordinators;
  private int lastViewId;
  
  public FindCoordinatorRequest(InternalDistributedMember myId) {
    this.memberID = myId;
  }
  
  public FindCoordinatorRequest(InternalDistributedMember myId, Collection<InternalDistributedMember> rejectedCoordinators, int lastViewId) {
    this.memberID = myId;
    this.rejectedCoordinators = rejectedCoordinators;
    this.lastViewId = lastViewId;
  }
  
  public FindCoordinatorRequest() {
    // no-arg constructor for serialization
  }

  public InternalDistributedMember getMemberID() {
    return memberID;
  }
  
  public Collection<InternalDistributedMember> getRejectedCoordinators() {
    return rejectedCoordinators;
  }
  
  public int getLastViewId() {
    return this.lastViewId;
  }
  
  @Override
  public String toString() {
    if (rejectedCoordinators != null) {
      return "FindCoordinatorRequest(memberID="+memberID
          +", rejected="+rejectedCoordinators+", lastViewId="+lastViewId+")";
    } else {
      return "FindCoordinatorRequest(memberID="+memberID+")";
    }
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
    if (this.rejectedCoordinators != null) {
      out.writeInt(this.rejectedCoordinators.size());
      for (InternalDistributedMember mbr: this.rejectedCoordinators) {
        DataSerializer.writeObject(mbr, out);
      }
    } else {
      out.writeInt(0);
    }
    out.writeInt(lastViewId);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.memberID = DataSerializer.readObject(in);
    int size = in.readInt();
    this.rejectedCoordinators = new ArrayList<InternalDistributedMember>(size);
    for (int i=0; i<size; i++) {
      this.rejectedCoordinators.add((InternalDistributedMember)DataSerializer.readObject(in));
    }
    this.lastViewId = in.readInt();
  }

  @Override
  protected void process(DistributionManager dm) {
    throw new IllegalStateException("this message should not be executed");
  }

}
