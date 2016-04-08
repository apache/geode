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
package com.gemstone.gemfire.internal.admin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * A PooledDistributionMessage for notifying admin members about changes in 
 * Client Membership received through BridgeMembership 
 * 
 */
public class ClientMembershipMessage extends PooledDistributionMessage {
  public static final int JOINED  = 0;
  public static final int LEFT    = 1;
  public static final int CRASHED = 2;
  

  private String clientId;
  private String clientHost;
  private int    eventType;

  /**
   * Default constructor(for serialization)
   */
  public ClientMembershipMessage() {
  }

  /**
   * Parameterized constructor
   * 
   * @param clientId
   *          Id of the client
   * @param clientHost
   *          host the client was running on (could be null)
   * @param eventType
   *          whether client joined, left or crashed. Should be one of
   *          ClientMembershipMessage.JOINED, ClientMembershipMessage.LEFT,
   *          ClientMembershipMessage.CRASHED
   */
  public ClientMembershipMessage(String clientId, String clientHost,
                                 int eventType) {
    this.clientId   = clientId;
    this.clientHost = clientHost;
    this.eventType  = eventType;
  }

  /** 
   * 
   * @see com.gemstone.gemfire.distributed.internal.DistributionMessage#process(com.gemstone.gemfire.distributed.internal.DistributionManager)
   */
  @Override
  protected void process(DistributionManager dm) {
    AdminDistributedSystemImpl adminDs = 
                      AdminDistributedSystemImpl.getConnectedInstance();
    
    /* 
     * Disconnect can be called on AdminDistributedSystem from Agent and it is 
     * not synchronous with processing of this message. Null check added to 
     * avoid null if disconnect has been called on AdminDistributedSystem
     */
    if (adminDs != null) {
      String senderId = null;
      InternalDistributedMember msgSender = this.getSender();
      if (msgSender != null) {
        senderId = msgSender.getId();
      }
  
      adminDs.processClientMembership(senderId, clientId, 
                                      clientHost, eventType);
    }
  }

  /**
   *  
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
  public int getDSFID() {
    return CLIENT_MEMBERSHIP_MESSAGE;
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.clientId, out);
    DataSerializer.writeString(this.clientHost, out);
    out.writeInt(this.eventType);
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    
    this.clientId   = DataSerializer.readString(in);
    this.clientHost = DataSerializer.readString(in);
    this.eventType  = in.readInt();
  }

  /**
   * @return the clientId
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * @return the clientHost
   */
  public String getClientHost() {
    return clientHost;
  }

  /**
   * @return the eventType
   */
  public int getEventType() {
    return eventType;
  }
  
  /**
   * @param eventType the eventType integer
   * @return String format of eventType
   */
  public static String getEventTypeString(int eventType) {
    switch (eventType) {
      case JOINED:
        return "Member JOINED";
      case LEFT:
        return "Member LEFT";
      case CRASHED:
        return "Member CRASHED";
      default:
        return "UNKNOWN";
    }
  }
  
  /**
   * String representation of this message.
   * 
   * @return String representation of this message.
   */
  @Override
  public String toString() {
    String clientMembership = "JOINED";
    
    switch (this.eventType) {
    case LEFT:
      clientMembership = "LEFT";
      break;

    case CRASHED:
      clientMembership = "CRASHED and left";
      break;

    default:
      break;
    }
    
    return "Client with Id: "+this.clientId + 
           " running on host: "+this.clientHost + 
           " "+clientMembership +
           " the server: "+this.getSender();
  }

}
