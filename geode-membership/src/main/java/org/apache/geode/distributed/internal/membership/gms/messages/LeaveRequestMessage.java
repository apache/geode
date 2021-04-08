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
import java.util.List;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A LeaveRequestMessage is sent by a member of the cluster when it intends to shut down.
 * This informs other members of the cluster that they should not consider the shutdown to
 * be abnormal. No response is required.
 */
public class LeaveRequestMessage<ID extends MemberIdentifier> extends AbstractGMSMessage<ID>
    implements HasMemberID<ID> {
  private ID memberID;
  private String reason;

  public LeaveRequestMessage(List<ID> coords,
      ID id, String reason) {
    super();
    setRecipients(coords);
    this.memberID = id;
    this.reason = reason;
  }

  public LeaveRequestMessage(ID coord, ID id,
      String reason) {
    super();
    setRecipient(coord);
    this.memberID = id;
    this.reason = reason;
  }

  public LeaveRequestMessage() {
    // no-arg constructor for serialization
  }

  @Override
  public int getDSFID() {
    return LEAVE_REQUEST_MESSAGE;
  }

  @Override
  public ID getMemberID() {
    return memberID;
  }

  public String getReason() {
    return reason;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    context.getSerializer().writeObject(memberID, out);
    context.getSerializer().writeString(reason, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    memberID = context.getDeserializer().readObject(in);
    reason = context.getDeserializer().readString(in);
  }

  @Override
  public String toString() {
    return getShortClassName() + "(" + memberID + "; reason=" + reason + ")";
  }

}
