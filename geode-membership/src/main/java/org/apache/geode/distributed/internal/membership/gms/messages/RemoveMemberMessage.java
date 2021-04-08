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
 * A member of the cluster can request that another member be removed. This message is
 * sent to the cluster's membership coordinator. This message is also sent to non-members
 * that attempt to communicate with the cluster. They should respond by shutting down.
 * No response message is required.
 */
public class RemoveMemberMessage<ID extends MemberIdentifier> extends AbstractGMSMessage<ID>
    implements HasMemberID<ID> {
  private ID memberID;
  private String reason;


  public RemoveMemberMessage(ID recipient, ID id,
      String reason) {
    super();
    setRecipient(recipient);
    this.memberID = id;
    this.reason = reason;
  }

  public RemoveMemberMessage(List<ID> recipients,
      ID id, String reason) {
    super();
    setRecipients(recipients);
    this.memberID = id;
    this.reason = reason;
  }

  public RemoveMemberMessage() {
    // no-arg constructor for serialization
  }

  @Override
  public int getDSFID() {
    return REMOVE_MEMBER_REQUEST;
  }

  @Override
  public ID getMemberID() {
    return memberID;
  }

  public String getReason() {
    return reason;
  }

  @Override
  public String toString() {
    return getShortClassName() + "(" + memberID + "; reason=" + reason + ")";
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

}
