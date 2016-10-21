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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;

public class JoinResponseMessage extends HighPriorityDistributionMessage {

  private NetView currentView;
  private String rejectionMessage;
  private InternalDistributedMember memberID;
  private byte[] messengerData;
  private int requestId;
  private byte[] secretPk;

  public JoinResponseMessage(InternalDistributedMember memberID, NetView view, int requestId) {
    this.currentView = view;
    this.memberID = memberID;
    this.requestId = requestId;
    setRecipient(memberID);
  }

  public JoinResponseMessage(InternalDistributedMember memberID, byte[] sPk, int requestId) {
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

  public NetView getCurrentView() {
    return currentView;
  }

  public InternalDistributedMember getMemberID() {
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
  public void process(DistributionManager dm) {
    throw new IllegalStateException("JoinResponse is not intended to be executed");
  }

  @Override
  public String toString() {
    return getShortClassName() + "(" + memberID + "; "
        + (currentView == null ? "" : currentView.toString())
        + (rejectionMessage == null ? "" : ("; " + rejectionMessage)) + ")";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return JOIN_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(currentView, out);
    DataSerializer.writeObject(memberID, out);
    DataSerializer.writeString(rejectionMessage, out);
    DataSerializer.writeByteArray(messengerData, out);
    DataSerializer.writeByteArray(secretPk, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    currentView = DataSerializer.readObject(in);
    memberID = DataSerializer.readObject(in);
    rejectionMessage = DataSerializer.readString(in);
    messengerData = DataSerializer.readByteArray(in);
    secretPk = DataSerializer.readByteArray(in);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    JoinResponseMessage other = (JoinResponseMessage) obj;
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
