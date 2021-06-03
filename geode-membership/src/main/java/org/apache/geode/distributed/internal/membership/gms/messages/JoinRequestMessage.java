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
package org.apache.geode.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A JoinRequestMessage is sent from a prospective member of the cluster to a node
 * that it believes is the coordinator. Members should retain this message in the
 * event that they become the coordinator and need to send out a membership view
 * allowing the prospective member to join. A member that is already filling the role
 * of coordinator will prepare and install a new membership view allowing the prospect
 * to join.<br>
 * A prospective member is not part of the cluster until an InstallViewMessage(INSTALL) has
 * been sent to the cluster. At that time the membership view will contain its valid
 * membership address, including it's view ID. This must be registered in the new member's
 * MemberData. Failure to do so may result in the new member being kicked out of the cluster.
 */
public class JoinRequestMessage<ID extends MemberIdentifier> extends AbstractGMSMessage<ID> {
  private ID memberID;
  private Object credentials;
  private int failureDetectionPort = -1;
  private int requestId;
  private boolean useMulticast;

  public JoinRequestMessage(ID coord, ID id,
      Object credentials, int fdPort, int requestId) {
    super();
    if (coord != null) {
      setRecipient(coord);
    }
    this.memberID = id;
    this.credentials = credentials;
    this.failureDetectionPort = fdPort;
    this.requestId = requestId;
  }

  public JoinRequestMessage() {
    // no-arg constructor for serialization
  }

  public int getRequestId() {
    return requestId;
  }

  @Override
  public int getDSFID() {
    return JOIN_REQUEST;
  }

  @Override
  public boolean getMulticast() {
    return useMulticast;
  }

  @Override
  public void setMulticast(boolean useMulticast) {
    this.useMulticast = useMulticast;
  }

  public ID getMemberID() {
    return memberID;
  }

  public Object getCredentials() {
    return credentials;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + memberID
        + (credentials == null ? ")" : "; with credentials)") + " failureDetectionPort:"
        + failureDetectionPort;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    context.getSerializer().writeObject(memberID, out);
    context.getSerializer().writeObject(credentials, out);
    out.writeInt(failureDetectionPort);
    // preserve the multicast setting so the receiver can tell
    // if this is a mcast join request
    out.writeBoolean(false);
    out.writeInt(requestId);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    memberID = context.getDeserializer().readObject(in);
    credentials = context.getDeserializer().readObject(in);
    failureDetectionPort = in.readInt();
    // setMulticast(in.readBoolean());
    in.readBoolean();
    requestId = in.readInt();
  }

  public int getFailureDetectionPort() {
    return failureDetectionPort;
  }

  @Override
  public int hashCode() {
    return Objects.hash(memberID);
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
    JoinRequestMessage<ID> other = (JoinRequestMessage<ID>) obj;
    if (credentials == null) {
      if (other.credentials != null) {
        return false;
      }
    } else if (!credentials.equals(other.credentials)) {
      return false;
    }
    if (failureDetectionPort != other.failureDetectionPort) {
      return false;
    }
    if (memberID == null) {
      if (other.memberID != null) {
        return false;
      }
    } else if (!memberID.equals(other.memberID)) {
      return false;
    }
    if (requestId != other.requestId) {
      return false;
    }
    return true;
  }
}
