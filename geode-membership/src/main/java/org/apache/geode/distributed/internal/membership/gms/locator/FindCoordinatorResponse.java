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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.GMSUtil;
import org.apache.geode.distributed.internal.membership.gms.messages.AbstractGMSMessage;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticDeserialization;
import org.apache.geode.internal.serialization.StaticSerialization;

/**
 * FindCoordinatorResponse is sent as a response to a FindCoordinatorRequest. A Locator
 * sends this to a member that is attempting to join the cluster. The "registrants" field
 * will contain addresses of other nodes attempting to join at the same time. The "view"
 * field will contain a membership view, if one is known by the Locator. The coordinator
 * field will contain the Locator's best guess of who is the cluster's membership coordinator.
 * <p>
 * Good luck in your attempt to find the coordinator and join the cluster!
 *
 * @param <ID>
 */
public class FindCoordinatorResponse<ID extends MemberIdentifier> extends AbstractGMSMessage<ID>
    implements DataSerializableFixedID {

  private ID coordinator;
  private ID senderId;
  private boolean fromView;
  private GMSMembershipView<ID> view;
  private Set<ID> registrants;
  private boolean networkPartitionDetectionEnabled;
  private boolean usePreferredCoordinators;
  private boolean isShortForm;
  private byte[] coordinatorPublicKey;
  private String rejectionMessage;

  private int requestId;

  public FindCoordinatorResponse(ID coordinator,
      ID senderId, boolean fromView, GMSMembershipView<ID> view,
      HashSet<ID> registrants, boolean networkPartitionDectionEnabled,
      boolean usePreferredCoordinators, byte[] pk) {
    this.coordinator = coordinator;
    this.senderId = senderId;
    this.fromView = fromView;
    this.view = view;
    this.registrants = registrants;
    this.networkPartitionDetectionEnabled = networkPartitionDectionEnabled;
    this.usePreferredCoordinators = usePreferredCoordinators;
    this.isShortForm = false;
    this.coordinatorPublicKey = pk;
  }

  public FindCoordinatorResponse(ID coordinator,
      ID senderId, byte[] pk, int requestId) {
    this.coordinator = coordinator;
    this.senderId = senderId;
    this.isShortForm = true;
    this.coordinatorPublicKey = pk;
    this.requestId = requestId;
  }

  public FindCoordinatorResponse(String m) {
    this.rejectionMessage = m;
  }

  public FindCoordinatorResponse() {
    // no-arg constructor for serialization
  }

  public byte[] getCoordinatorPublicKey() {
    return coordinatorPublicKey;
  }

  public int getRequestId() {
    return requestId;
  }

  public String getRejectionMessage() {
    return rejectionMessage;
  }

  public boolean isNetworkPartitionDetectionEnabled() {
    return networkPartitionDetectionEnabled;
  }

  public boolean isUsePreferredCoordinators() {
    return usePreferredCoordinators;
  }

  public ID getCoordinator() {
    return coordinator;
  }

  /**
   * When the response comes from a locator via TcpClient this will return the locators member ID.
   * If the locator hasn't yet joined this may be null.
   */
  public ID getSenderId() {
    return senderId;
  }

  public boolean isFromView() {
    return fromView;
  }

  public GMSMembershipView<ID> getView() {
    return view;
  }

  public Set<ID> getRegistrants() {
    return registrants;
  }

  @Override
  public String toString() {
    if (this.isShortForm) {
      return "FindCoordinatorResponse(coordinator=" + coordinator + "; senderId=" + senderId + ")";
    } else {
      return "FindCoordinatorResponse(coordinator=" + coordinator + ", fromView=" + fromView
          + ", viewId=" + (view == null ? "null" : view.getViewId()) + ", registrants="
          + (registrants == null ? "none" : registrants) + ", senderId=" + senderId
          + ", network partition detection enabled=" + this.networkPartitionDetectionEnabled
          + ", locators preferred as coordinators=" + this.usePreferredCoordinators
          + ", view=" + view + ")";
    }
  }



  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return FIND_COORDINATOR_RESP;
  }

  // TODO serialization not backward compatible with 1.9 - may need InternalDistributedMember, not
  // GMSMember
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    context.getSerializer().writeObject(coordinator, out);
    context.getSerializer().writeObject(senderId, out);
    StaticSerialization.writeByteArray(coordinatorPublicKey, out);
    StaticSerialization.writeString(rejectionMessage, out);
    out.writeBoolean(isShortForm);
    out.writeBoolean(fromView);
    out.writeBoolean(networkPartitionDetectionEnabled);
    out.writeBoolean(usePreferredCoordinators);
    context.getSerializer().writeObject(view, out);
    GMSUtil.writeSetOfMemberIDs(registrants, out, context);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    coordinator = context.getDeserializer().readObject(in);
    senderId = context.getDeserializer().readObject(in);
    coordinatorPublicKey = StaticDeserialization.readByteArray(in);
    rejectionMessage = StaticDeserialization.readString(in);
    isShortForm = in.readBoolean();
    if (!isShortForm) {
      fromView = in.readBoolean();
      networkPartitionDetectionEnabled = in.readBoolean();
      usePreferredCoordinators = in.readBoolean();
      view = context.getDeserializer().readObject(in);
      registrants = GMSUtil.readHashSetOfMemberIDs(in, context);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(senderId, view, registrants, requestId);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FindCoordinatorResponse other = (FindCoordinatorResponse) obj;
    if (coordinator == null) {
      if (other.coordinator != null)
        return false;
    } else if (!coordinator.equals(other.coordinator))
      return false;
    if (!Arrays.equals(coordinatorPublicKey, other.coordinatorPublicKey))
      return false;
    if (fromView != other.fromView)
      return false;
    if (isShortForm != other.isShortForm)
      return false;
    if (networkPartitionDetectionEnabled != other.networkPartitionDetectionEnabled)
      return false;
    if (registrants == null) {
      if (other.registrants != null)
        return false;
    } else if (!registrants.equals(other.registrants))
      return false;
    // as we are not sending requestId as part of FinDCoordinator resposne
    /*
     * if (requestId != other.requestId) return false;
     */
    if (senderId == null) {
      if (other.senderId != null)
        return false;
    } else if (!senderId.equals(other.senderId))
      return false;
    if (usePreferredCoordinators != other.usePreferredCoordinators)
      return false;
    if (view == null) {
      if (other.view != null)
        return false;
    } else if (!view.equals(other.view))
      return false;
    return true;
  }


}
