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

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.messages.AbstractGMSMessage;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;

/**
 * FindCoordinatorRequest is a message intended to be sent via a TcpClient to a Locator.
 * It is used during startup to discover the cluster's membership coordinator.
 */
public class FindCoordinatorRequest<ID extends MemberIdentifier> extends AbstractGMSMessage<ID>
    implements PeerLocatorRequest {

  private ID memberID;
  private Collection<ID> rejectedCoordinators;
  private int lastViewId;
  private byte[] myPublicKey;
  private int requestId;
  private String dhalgo;

  public FindCoordinatorRequest(ID myId) {
    memberID = myId;
    dhalgo = "";
  }

  public FindCoordinatorRequest(ID myId,
      Collection<ID> rejectedCoordinators, int lastViewId, byte[] pk,
      int requestId, String dhalgo) {
    memberID = myId;
    this.rejectedCoordinators = rejectedCoordinators;
    this.lastViewId = lastViewId;
    myPublicKey = pk;
    this.requestId = requestId;
    this.dhalgo = dhalgo;
  }

  public FindCoordinatorRequest() {
    // no-arg constructor for serialization
  }

  public ID getMemberID() {
    return memberID;
  }

  public byte[] getMyPublicKey() {
    return myPublicKey;
  }

  public String getDHAlgo() {
    return dhalgo;
  }

  public Collection<ID> getRejectedCoordinators() {
    return rejectedCoordinators;
  }

  public int getLastViewId() {
    return lastViewId;
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
  public KnownVersion[] getSerializationVersions() {
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
    context.getSerializer().writeObject(memberID, out);
    if (rejectedCoordinators != null) {
      out.writeInt(rejectedCoordinators.size());
      for (ID mbr : rejectedCoordinators) {
        context.getSerializer().writeObject(mbr, out);
      }
    } else {
      out.writeInt(0);
    }
    out.writeInt(lastViewId);
    out.writeInt(requestId);
    StaticSerialization.writeString(dhalgo, out);
    StaticSerialization.writeByteArray(myPublicKey, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    memberID = context.getDeserializer().readObject(in);
    int size = in.readInt();
    rejectedCoordinators = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      rejectedCoordinators.add(context.getDeserializer().readObject(in));
    }
    lastViewId = in.readInt();
    requestId = in.readInt();
    dhalgo = StaticSerialization.readString(in);
    myPublicKey = StaticSerialization.readByteArray(in);
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
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    FindCoordinatorRequest<ID> other = (FindCoordinatorRequest<ID>) obj;
    if (lastViewId != other.lastViewId) {
      return false;
    }
    if (!dhalgo.equals(other.dhalgo)) {
      return false;
    }
    if (memberID == null) {
      if (other.memberID != null) {
        return false;
      }
    } else if (!memberID.equals(other.memberID)) {
      return false;
    }
    if (rejectedCoordinators == null) {
      return other.rejectedCoordinators == null;
    } else
      return rejectedCoordinators.equals(other.rejectedCoordinators);
  }
}
