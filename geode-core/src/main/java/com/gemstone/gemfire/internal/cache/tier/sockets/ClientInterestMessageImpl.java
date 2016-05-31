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

package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.tier.InterestType;
import com.gemstone.gemfire.internal.cache.tier.MessageType;

import java.io.*;

/**
 * Class <code>ClientInterestMessageImpl</code> represents an update to the
 * a client's interest registrations made by the server on behalf of the
 * client.
 *
 *
 * @since GemFire 5.6
 */
public class ClientInterestMessageImpl implements ClientMessage {

  private static final long serialVersionUID = -797925585426839008L;

  /**
   * This <code>ClientMessage</code>'s <code>EventID</code>
   */
  private EventID eventId;

  /**
   * This <code>ClientMessage</code>'s key
   */
  private Object keyOfInterest;
  
  /**
   * This <code>ClientMessage</code>'s region name
   */
  private String regionName;
  
  /**
   * Whether the interest represented by this <code>ClientMessage</code>
   * is durable
   */
  private boolean isDurable;
  
  /**
   * Whether the create or update events for this <code>ClientMessage</code>
   * is sent as an invalidate
   * @since GemFire 6.0.3
   */
  private boolean forUpdatesAsInvalidates;
  
  /**
   * This <code>ClientMessage</code>'s interest type (key or regex)
   */
  private int interestType;
  
  /**
   * This <code>ClientMessage</code>'s interest result policy (none, key, key-value)
   */
  private byte interestResultPolicy;
  
  /**
   * This <code>ClientMessage</code>'s action (add or remove interest)
   */
  private byte action;
  
  /**
   * A byte representing a register interest message
   */
  protected static final byte REGISTER = (byte) 0;
  
  /**
   * A byte representing an unregister interest message
   */
  protected static final byte UNREGISTER = (byte) 1;

  /**
   * 
   * @param eventId The EventID of this message
   * @param regionName The name of the region whose interest is changing
   * @param keyOfInterest The key in the region whose interest is changing
   * @param action The action (add or remove interest)
   */
  public ClientInterestMessageImpl(EventID eventId, String regionName,
      Object keyOfInterest, int interestType, byte interestResultPolicy,
      boolean isDurable, boolean sendUpdatesAsInvalidates,
      byte action) {
    this.eventId = eventId;
    this.regionName = regionName;
    this.keyOfInterest = keyOfInterest;
    this.interestType = interestType;
    this.interestResultPolicy = interestResultPolicy;
    this.isDurable = isDurable;
    this.forUpdatesAsInvalidates = sendUpdatesAsInvalidates;
    this.action = action;
  }

  /**
   * Default constructor.
   */
  public ClientInterestMessageImpl() {
  }

  public Message getMessage(CacheClientProxy proxy, boolean notify) throws IOException {
    Version clientVersion = proxy.getVersion();
    Message message = null;
    if (clientVersion.compareTo(Version.GFE_57) >= 0) {
      message = getGFEMessage();
    } else {
      throw new IOException(
          "Unsupported client version for server-to-client message creation: "
              + clientVersion);
    }
      
    return message;
  }
  
  protected Message getGFEMessage() throws IOException {
    Message message = new Message(isRegister() ? 7 : 6, Version.CURRENT);
    message.setTransactionId(0);
    
    // Set the message type
    switch (this.action) {
    case REGISTER:
      message.setMessageType(MessageType.CLIENT_REGISTER_INTEREST);
      break;
    case UNREGISTER:
      message.setMessageType(MessageType.CLIENT_UNREGISTER_INTEREST);
      break;
    default:
      String s = "Unknown action: " + this.action;
      throw new IOException(s);
    }
    
    // Add the region name
    message.addStringPart(this.regionName);

    // Add the key
    message.addStringOrObjPart(this.keyOfInterest);
    
    // Add the interest type
    message.addObjPart(Integer.valueOf(this.interestType));
    
    // Add the interest result policy (if register interest)
    if (isRegister()) {
      message.addObjPart(Byte.valueOf(this.interestResultPolicy));
    }
    
    // Add the isDurable flag
    message.addObjPart(Boolean.valueOf(this.isDurable));
    
    // Add the forUpdatesAsInvalidates flag
    message.addObjPart(Boolean.valueOf(this.forUpdatesAsInvalidates));
    
    // Add the event id
    message.addObjPart(this.eventId);
    
    return message;
  }

  public boolean shouldBeConflated() {
    return false;
  }

  public int getDSFID() {
    return DataSerializableFixedID.CLIENT_INTEREST_MESSAGE;
  }
  
  public void writeExternal(ObjectOutput out) throws IOException {
    toData(out);
  }

  public void readExternal(ObjectInput in)
    throws IOException, ClassNotFoundException {
    fromData(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.eventId, out);
    DataSerializer.writeString(this.regionName, out);
    DataSerializer.writeObject(this.keyOfInterest, out);
    DataSerializer.writePrimitiveBoolean(this.isDurable, out);
    DataSerializer.writePrimitiveBoolean(this.forUpdatesAsInvalidates , out);
    DataSerializer.writePrimitiveInt(this.interestType, out);
    DataSerializer.writePrimitiveByte(this.interestResultPolicy, out);
    DataSerializer.writePrimitiveByte(this.action, out);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    this.eventId = (EventID) DataSerializer.readObject(in);
    this.regionName = DataSerializer.readString(in);
    this.keyOfInterest = DataSerializer.readObject(in);
    this.isDurable = DataSerializer.readPrimitiveBoolean(in);
    this.forUpdatesAsInvalidates = DataSerializer.readPrimitiveBoolean(in);
    this.interestType = DataSerializer.readPrimitiveInt(in);
    this.interestResultPolicy = DataSerializer.readPrimitiveByte(in);
    this.action = DataSerializer.readPrimitiveByte(in);
  }

  public EventID getEventId() {
    return this.eventId;
  }
  
  public String getRegionName() {
    return this.regionName;
  }
  
  public Object getKeyOfInterest() {
    return this.keyOfInterest;
  }
  
  public int getInterestType() {
    return this.interestType;
  }
  
  public boolean getIsDurable() {
    return this.isDurable;
  }
  
  public boolean getForUpdatesAsInvalidates() {
    return this.forUpdatesAsInvalidates;
  }
  
  public boolean isKeyInterest() {
    return this.interestType == InterestType.KEY;
  }
  
  public boolean isRegister() {
    return this.action == REGISTER;
  }

  public String getRegionToConflate() {
    return null;
  }

  public Object getKeyToConflate() {
    // This method can be called by HARegionQueue.
    // Use this to identify the message type.
    return "interest";
  }

  public Object getValueToConflate() {
    // This method can be called by HARegionQueue
    // Use this to identify the message type.
    return "interest";
  }

  public void setLatestValue(Object value){
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
