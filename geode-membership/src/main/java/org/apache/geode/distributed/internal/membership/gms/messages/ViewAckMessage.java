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

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

/**
 * Members receiving an InstallViewMessage must respond by sending a ViewAckMessage.
 */
public class ViewAckMessage<ID extends MemberIdentifier> extends AbstractGMSMessage<ID> {

  int viewId;
  boolean preparing;
  GMSMembershipView<ID> alternateView;

  public ViewAckMessage(ID recipient, int viewId, boolean preparing) {
    super();
    setRecipient(recipient);
    this.viewId = viewId;
    this.preparing = preparing;
  }

  public ViewAckMessage(int viewId, ID recipient, GMSMembershipView<ID> alternateView) {
    super();
    setRecipient(recipient);
    this.viewId = viewId;
    this.alternateView = alternateView;
    this.preparing = true;
  }

  public ViewAckMessage() {
    // no-arg constructor for serialization
  }

  public int getViewId() {
    return viewId;
  }

  public GMSMembershipView<ID> getAlternateView() {
    return this.alternateView;
  }

  public boolean isPrepareAck() {
    return preparing;
  }

  @Override
  public int getDSFID() {
    return VIEW_ACK_MESSAGE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeInt(this.viewId);
    out.writeBoolean(this.preparing);
    context.getSerializer().writeObject(this.alternateView, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    this.viewId = in.readInt();
    this.preparing = in.readBoolean();
    this.alternateView = context.getDeserializer().readObject(in);
  }

  @Override
  public String toString() {
    String s = getSender() == null ? getRecipients().toString() : "" + getSender();
    return "ViewAckMessage(" + s + "; " + this.viewId + "; preparing=" + preparing + "; altview="
        + this.alternateView + ")";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
