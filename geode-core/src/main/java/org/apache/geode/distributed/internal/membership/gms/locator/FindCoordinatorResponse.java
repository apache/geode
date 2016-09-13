/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.distributed.internal.membership.gms.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;

public class FindCoordinatorResponse  extends HighPriorityDistributionMessage
    implements DataSerializableFixedID {

  private InternalDistributedMember coordinator;
  private InternalDistributedMember senderId;
  private boolean fromView;
  private NetView view;
  private Set<InternalDistributedMember> registrants;
  private boolean networkPartitionDetectionEnabled;
  private boolean usePreferredCoordinators;
  private boolean isShortForm;
  private byte[] coordinatorPublicKey;  
  private String rejectionMessage;

  private int requestId;
  
  public FindCoordinatorResponse(InternalDistributedMember coordinator,
      InternalDistributedMember senderId,
      boolean fromView, NetView view, HashSet<InternalDistributedMember> registrants,
      boolean networkPartitionDectionEnabled, boolean usePreferredCoordinators, byte[] pk) {
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
  
  public FindCoordinatorResponse(InternalDistributedMember coordinator,
      InternalDistributedMember senderId, byte[] pk, int requestId) {
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

  public InternalDistributedMember getCoordinator() {
    return coordinator;
  }
  
  /**
   * When the response comes from a locator via TcpClient this
   * will return the locators member ID.  If the locator hasn't
   * yet joined this may be null.
   */
  public InternalDistributedMember getSenderId() {
    return senderId;
  }
  
  public boolean isFromView() {
    return fromView;
  }
  
  public NetView getView() {
    return view;
  }
  
  public Set<InternalDistributedMember> getRegistrants() {
    return registrants;
  }
  
  @Override
  public String toString() {
    if (this.isShortForm) { 
      return "FindCoordinatorResponse(coordinator="+coordinator+")";
    } else {
      return "FindCoordinatorResponse(coordinator="+coordinator+", fromView="+fromView+", viewId="+(view==null? "nul" : view.getViewId())
        +", registrants=" + (registrants == null? 0 : registrants.size())
        +", senderId=" + senderId
        +", network partition detection enabled="+this.networkPartitionDetectionEnabled
        +", locators preferred as coordinators="+this.usePreferredCoordinators+")";
    }
  }



  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return FIND_COORDINATOR_RESP;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(coordinator, out);
    DataSerializer.writeObject(senderId, out);
    InternalDataSerializer.writeByteArray(coordinatorPublicKey, out);
    InternalDataSerializer.writeString(rejectionMessage, out);
    out.writeBoolean(isShortForm);
    out.writeBoolean(fromView);
    out.writeBoolean(networkPartitionDetectionEnabled);
    out.writeBoolean(usePreferredCoordinators);
    DataSerializer.writeObject(view, out);
    InternalDataSerializer.writeSet(registrants, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    coordinator = DataSerializer.readObject(in);
    senderId = DataSerializer.readObject(in);
    coordinatorPublicKey = InternalDataSerializer.readByteArray(in);
    rejectionMessage = InternalDataSerializer.readString(in);
    isShortForm = in.readBoolean();    
    if (!isShortForm) {
      fromView = in.readBoolean();
      networkPartitionDetectionEnabled = in.readBoolean();
      usePreferredCoordinators = in.readBoolean();
      view = DataSerializer.readObject(in);
      registrants = InternalDataSerializer.readHashSet(in);
    }
  }

  @Override
  protected void process(DistributionManager dm) {
    throw new IllegalStateException("this message should not be executed");
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
    //as we are not sending requestId as part of FinDCoordinator resposne
    /*if (requestId != other.requestId)
      return false;*/
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
