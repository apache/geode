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
package com.gemstone.gemfire.distributed.internal.membership.gms.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;

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
  
  
  public FindCoordinatorResponse(InternalDistributedMember coordinator,
      InternalDistributedMember senderId,
      boolean fromView, NetView view, HashSet<InternalDistributedMember> registrants,
      boolean networkPartitionDectionEnabled, boolean usePreferredCoordinators) {
    this.coordinator = coordinator;
    this.senderId = senderId;
    this.fromView = fromView;
    this.view = view;
    this.registrants = registrants;
    this.networkPartitionDetectionEnabled = networkPartitionDectionEnabled;
    this.usePreferredCoordinators = usePreferredCoordinators;
    this.isShortForm = false;
  }
  
  public FindCoordinatorResponse(InternalDistributedMember coordinator,
      InternalDistributedMember senderId) {
    this.coordinator = coordinator;
    this.senderId = senderId;
    this.isShortForm = true;
  }
  
  public FindCoordinatorResponse() {
    // no-arg constructor for serialization
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

}
