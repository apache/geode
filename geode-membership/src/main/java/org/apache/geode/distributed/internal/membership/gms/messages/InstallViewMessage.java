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
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A membership coordinator will send InstallViewMessages to other members when a node
 * in the cluster joins or leaves. There are three types of IVMs:<br>
 * 1) PREPARE informs other member of the intent to change membership. All members of the
 * cluster are required to respond to this message. A non-responding member will be suspect and
 * may be kicke out of the cluster. A member may respond to a PREPARE message to object
 * due to having received a conflicting PREPARE message.<br>
 * 2) INSTALL informs other members that all of the nodes recorded in the membership "view" have
 * responded to a PREPARE message and that the enclosed view defines the current cluster.<br>
 * 3) SYNC messages are periodically broadcast to ensure that all members of the cluster
 * know the current cluster membership.<br>
 * The first two types of InstallViewMessage require a ViewAckMessage in response.
 */
public class InstallViewMessage<ID extends MemberIdentifier> extends AbstractGMSMessage<ID> {

  enum messageType {
    INSTALL, PREPARE, SYNC
  }

  private GMSMembershipView<ID> view;
  private Object credentials;
  private messageType kind;
  private int previousViewId;

  public InstallViewMessage(GMSMembershipView<ID> view, Object credentials, boolean preparing) {
    this.view = view;
    this.kind = preparing ? messageType.PREPARE : messageType.INSTALL;
    this.credentials = credentials;
  }

  public InstallViewMessage(GMSMembershipView<ID> view, Object credentials, int previousViewId,
      boolean preparing) {
    this.view = view;
    this.kind = preparing ? messageType.PREPARE : messageType.INSTALL;
    this.credentials = credentials;
    this.previousViewId = previousViewId;
  }

  public InstallViewMessage() {
    // no-arg constructor for serialization
  }

  public boolean isRebroadcast() {
    return kind == messageType.SYNC;
  }

  public GMSMembershipView<ID> getView() {
    return view;
  }

  public Object getCredentials() {
    return credentials;
  }

  public boolean isPreparing() {
    return kind == messageType.PREPARE;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return INSTALL_VIEW_MESSAGE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeInt(previousViewId);
    out.writeInt(kind.ordinal());
    context.getSerializer().writeObject(this.view, out);
    context.getSerializer().writeObject(this.credentials, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    this.previousViewId = in.readInt();
    this.kind = messageType.values()[in.readInt()];
    this.view = context.getDeserializer().readObject(in);
    this.credentials = context.getDeserializer().readObject(in);
  }

  @Override
  public String toString() {
    return "InstallViewMessage(type=" + this.kind + "; Current ViewID=" + view.getViewId()
        + "; Previous View ID=" + previousViewId + "; " + this.view + "; cred="
        + (credentials == null ? "null" : "not null") + ")";
  }

  @Override
  public int hashCode() {
    return Objects.hash(view, previousViewId);
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
    InstallViewMessage<ID> other = (InstallViewMessage<ID>) obj;
    if (credentials == null) {
      if (other.credentials != null) {
        return false;
      }
    } else if (!credentials.equals(other.credentials)) {
      return false;
    }
    if (kind != other.kind) {
      return false;
    }
    if (previousViewId != other.previousViewId) {
      return false;
    }
    if (view == null) {
      if (other.view != null) {
        return false;
      }
    } else if (!view.equals(other.view)) {
      return false;
    }
    return true;
  }
}
