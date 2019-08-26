/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.distributed.internal.membership.gms.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.GMSUtil;
import org.apache.geode.distributed.internal.membership.gms.messages.AbstractGMSMessage;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.SerializationVersion;
import org.apache.geode.internal.serialization.StaticSerialization;

public class FindCoordinatorRequest extends AbstractGMSMessage
    implements PeerLocatorRequest {

  private GMSMember memberID;
  private Collection<GMSMember> rejectedCoordinators;
  private int lastViewId;
  private byte[] myPublicKey;
  private int requestId;
  private String dhalgo;

  public FindCoordinatorRequest(GMSMember myId) {
    this.memberID = myId;
    this.dhalgo = "";
  }

  public FindCoordinatorRequest(GMSMember myId,
      Collection<GMSMember> rejectedCoordinators, int lastViewId, byte[] pk,
      int requestId, String dhalgo) {
    this.memberID = myId;
    this.rejectedCoordinators = rejectedCoordinators;
    this.lastViewId = lastViewId;
    this.myPublicKey = pk;
    this.requestId = requestId;
    this.dhalgo = dhalgo;
  }

  public FindCoordinatorRequest() {
    // no-arg constructor for serialization
  }

  public GMSMember getMemberID() {
    return memberID;
  }

  public byte[] getMyPublicKey() {
    return myPublicKey;
  }

  public String getDHAlgo() {
    return dhalgo;
  }

  public Collection<GMSMember> getRejectedCoordinators() {
    return rejectedCoordinators;
  }

  public int getLastViewId() {
    return this.lastViewId;
  }

  @Override
  public String toString() {
    if (rejectedCoordinators != null) {
      return "FindCoordinatorRequest(memberID=" + memberID + ", rejected=" + rejectedCoordinators
          + ", lastViewId=" + lastViewId + ")";
    } else {
      return "FindCoordinatorRequest(memberID=" + memberID + ")";
    }
  }

  @Override
  public SerializationVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return FIND_COORDINATOR_REQ;
  }

  public int getRequestId() {
    return requestId;
  }

  // TODO serialization not backward compatible with 1.9 - may need InternalDistributedMember, not
  // GMSMember
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    GMSUtil.writeMemberID(memberID, out, context);
    if (this.rejectedCoordinators != null) {
      out.writeInt(this.rejectedCoordinators.size());
      for (GMSMember mbr : this.rejectedCoordinators) {
        GMSUtil.writeMemberID(mbr, out, context);
      }
    } else {
      out.writeInt(0);
    }
    out.writeInt(lastViewId);
    out.writeInt(requestId);
    StaticSerialization.writeString(dhalgo, out);
    StaticSerialization.writeByteArray(this.myPublicKey, out);
  }

  @Override
  public void fromData(DataInput in,
      SerializationContext context) throws IOException, ClassNotFoundException {
    this.memberID = GMSUtil.readMemberID(in, context);
    int size = in.readInt();
    this.rejectedCoordinators = new ArrayList<GMSMember>(size);
    for (int i = 0; i < size; i++) {
      this.rejectedCoordinators.add(GMSUtil.readMemberID(in, context));
    }
    this.lastViewId = in.readInt();
    this.requestId = in.readInt();
    this.dhalgo = StaticSerialization.readString(in);
    this.myPublicKey = StaticSerialization.readByteArray(in);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + lastViewId;
    result = prime * result + ((dhalgo == null) ? 0 : dhalgo.hashCode());
    result = prime * result + ((memberID == null) ? 0 : memberID.hashCode());
    result =
        prime * result + ((rejectedCoordinators == null) ? 0 : rejectedCoordinators.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FindCoordinatorRequest other = (FindCoordinatorRequest) obj;
    if (lastViewId != other.lastViewId)
      return false;
    if (!dhalgo.equals(other.dhalgo)) {
      return false;
    }
    if (memberID == null) {
      if (other.memberID != null)
        return false;
    } else if (!memberID.equals(other.memberID))
      return false;
    if (rejectedCoordinators == null) {
      if (other.rejectedCoordinators != null)
        return false;
    } else if (!rejectedCoordinators.equals(other.rejectedCoordinators))
      return false;
    return true;
  }
}
