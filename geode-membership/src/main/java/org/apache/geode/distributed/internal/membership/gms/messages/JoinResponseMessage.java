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
import java.util.Arrays;
import java.util.Objects;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticDeserialization;
import org.apache.geode.internal.serialization.StaticSerialization;

// TODO this class has been made unintelligible with different combinations of response values.
// It needs to have an enum that indicates what type of response is in the message or it
// needs to be broken into multiple message classes.
// 1. a response saying the member has now joined
// 2. a response indicating that the coordinator is now a different process
// 3. a response containing the cluster encryption key

public class JoinResponseMessage<ID extends MemberIdentifier> extends AbstractGMSMessage<ID> {

  private GMSMembershipView<ID> currentView;
  private String rejectionMessage;
  private ID memberID;
  private byte[] messengerData;
  private int requestId;
  private byte[] secretPk;

  public JoinResponseMessage(ID memberID, GMSMembershipView<ID> view, int requestId) {
    this.currentView = view;
    this.memberID = memberID;
    this.requestId = requestId;
    setRecipient(memberID);
  }

  public JoinResponseMessage(ID memberID, byte[] sPk, int requestId) {
    this.memberID = memberID;
    this.requestId = requestId;
    this.secretPk = sPk;
    setRecipient(memberID);
  }

  public JoinResponseMessage(String rejectionMessage, int requestId) {
    this.rejectionMessage = rejectionMessage;
    this.requestId = requestId;
  }

  public JoinResponseMessage() {
    // no-arg constructor for serialization
  }

  public byte[] getSecretPk() {
    return secretPk;
  }

  public int getRequestId() {
    return requestId;
  }

  public GMSMembershipView<ID> getCurrentView() {
    return currentView;
  }

  public ID getMemberID() {
    return memberID;
  }

  public String getRejectionMessage() {
    return rejectionMessage;
  }

  public byte[] getMessengerData() {
    return this.messengerData;
  }

  public void setMessengerData(byte[] data) {
    this.messengerData = data;
  }

  @Override
  public String toString() {
    return getShortClassName() + "(" + memberID + "; "
        + (currentView == null ? "" : currentView.toString())
        + (rejectionMessage == null ? "" : ("; " + rejectionMessage)) + ")";
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return JOIN_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    context.getSerializer().writeObject(currentView, out);
    context.getSerializer().writeObject(memberID, out);
    StaticSerialization.writeString(rejectionMessage, out);
    StaticSerialization.writeByteArray(messengerData, out);
    StaticSerialization.writeByteArray(secretPk, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    currentView = context.getDeserializer().readObject(in);
    memberID = context.getDeserializer().readObject(in);
    rejectionMessage = StaticDeserialization.readString(in);
    messengerData = StaticDeserialization.readByteArray(in);
    secretPk = StaticDeserialization.readByteArray(in);
  }

  @Override
  public int hashCode() {
    return Objects.hash(memberID);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    JoinResponseMessage<ID> other = (JoinResponseMessage<ID>) obj;
    if (currentView == null) {
      if (other.currentView != null)
        return false;
    } else if (!currentView.equals(other.currentView))
      return false;
    if (memberID == null) {
      if (other.memberID != null)
        return false;
    } else if (!memberID.equals(other.memberID))
      return false;
    if (!Arrays.equals(messengerData, other.messengerData))
      return false;
    if (rejectionMessage == null) {
      if (other.rejectionMessage != null)
        return false;
    } else if (!rejectionMessage.equals(other.rejectionMessage))
      return false;
    // as we are not sending as part of JoinResposne
    /*
     * if (requestId != other.requestId) return false;
     */
    if (!Arrays.equals(secretPk, other.secretPk))
      return false;
    return true;
  }

}
