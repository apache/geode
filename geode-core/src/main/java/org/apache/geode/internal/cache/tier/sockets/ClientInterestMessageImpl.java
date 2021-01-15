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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Class <code>ClientInterestMessageImpl</code> represents an update to the a client's interest
 * registrations made by the server on behalf of the client.
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
   * Whether the interest represented by this <code>ClientMessage</code> is durable
   */
  private boolean isDurable;

  /**
   * Whether the create or update events for this <code>ClientMessage</code> is sent as an
   * invalidate
   *
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
  public ClientInterestMessageImpl(EventID eventId, String regionName, Object keyOfInterest,
      int interestType, byte interestResultPolicy, boolean isDurable,
      boolean sendUpdatesAsInvalidates, byte action) {
    this.eventId = eventId;
    this.regionName = regionName;
    this.keyOfInterest = keyOfInterest;
    this.interestType = interestType;
    this.interestResultPolicy = interestResultPolicy;
    this.isDurable = isDurable;
    forUpdatesAsInvalidates = sendUpdatesAsInvalidates;
    this.action = action;
  }

  public ClientInterestMessageImpl(DistributedSystem distributedSystem,
      ClientInterestMessageImpl message, Object keyOfInterest) {
    eventId = new EventID(distributedSystem);
    regionName = message.regionName;
    this.keyOfInterest = keyOfInterest;
    interestType = message.interestType;
    interestResultPolicy = message.interestResultPolicy;
    isDurable = message.isDurable;
    forUpdatesAsInvalidates = message.forUpdatesAsInvalidates;
    action = message.action;
  }

  /**
   * Default constructor.
   */
  public ClientInterestMessageImpl() {}

  @Override
  public Message getMessage(CacheClientProxy proxy, boolean notify) throws IOException {
    Message message = new Message(isRegister() ? 7 : 6, KnownVersion.CURRENT);
    message.setTransactionId(0);

    // Set the message type
    switch (action) {
      case REGISTER:
        message.setMessageType(MessageType.CLIENT_REGISTER_INTEREST);
        break;
      case UNREGISTER:
        message.setMessageType(MessageType.CLIENT_UNREGISTER_INTEREST);
        break;
      default:
        String s = "Unknown action: " + action;
        throw new IOException(s);
    }

    // Add the region name
    message.addStringPart(regionName, true);

    // Add the key
    message.addStringOrObjPart(keyOfInterest);

    // Add the interest type
    message.addObjPart(interestType);

    // Add the interest result policy (if register interest)
    if (isRegister()) {
      message.addObjPart(interestResultPolicy);
    }

    // Add the isDurable flag
    message.addObjPart(isDurable);

    // Add the forUpdatesAsInvalidates flag
    message.addObjPart(forUpdatesAsInvalidates);

    // Add the event id
    message.addObjPart(eventId);

    return message;
  }

  @Override
  public boolean shouldBeConflated() {
    return false;
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.CLIENT_INTEREST_MESSAGE;
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    toData(out, InternalDataSerializer.createSerializationContext(out));
  }

  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    fromData(in, InternalDataSerializer.createDeserializationContext(in));
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    context.getSerializer().writeObject(eventId, out);
    DataSerializer.writeString(regionName, out);
    context.getSerializer().writeObject(keyOfInterest, out);
    DataSerializer.writePrimitiveBoolean(isDurable, out);
    DataSerializer.writePrimitiveBoolean(forUpdatesAsInvalidates, out);
    DataSerializer.writePrimitiveInt(interestType, out);
    DataSerializer.writePrimitiveByte(interestResultPolicy, out);
    DataSerializer.writePrimitiveByte(action, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    eventId = context.getDeserializer().readObject(in);
    regionName = DataSerializer.readString(in);
    keyOfInterest = context.getDeserializer().readObject(in);
    isDurable = DataSerializer.readPrimitiveBoolean(in);
    forUpdatesAsInvalidates = DataSerializer.readPrimitiveBoolean(in);
    interestType = DataSerializer.readPrimitiveInt(in);
    interestResultPolicy = DataSerializer.readPrimitiveByte(in);
    action = DataSerializer.readPrimitiveByte(in);
  }

  @Override
  public EventID getEventId() {
    return eventId;
  }

  public String getRegionName() {
    return regionName;
  }

  public Object getKeyOfInterest() {
    return keyOfInterest;
  }

  public int getInterestType() {
    return interestType;
  }

  public byte getInterestResultPolicy() {
    return interestResultPolicy;
  }

  public boolean getIsDurable() {
    return isDurable;
  }

  public boolean getForUpdatesAsInvalidates() {
    return forUpdatesAsInvalidates;
  }

  public boolean isKeyInterest() {
    return interestType == InterestType.KEY;
  }

  public boolean isRegister() {
    return action == REGISTER;
  }

  @Override
  public String getRegionToConflate() {
    return null;
  }

  @Override
  public Object getKeyToConflate() {
    // This method can be called by HARegionQueue.
    // Use this to identify the message type.
    return "interest";
  }

  @Override
  public Object getValueToConflate() {
    // This method can be called by HARegionQueue
    // Use this to identify the message type.
    return "interest";
  }

  @Override
  public void setLatestValue(Object value) {}

  public String toString() {
    return getClass().getSimpleName() + "[" + "eventId="
        + eventId + "; regionName=" + regionName
        + "; keyOfInterest=" + keyOfInterest + "; isDurable="
        + isDurable + "; forUpdatesAsInvalidates="
        + forUpdatesAsInvalidates + "; interestType=" + interestType
        + "; interestResultPolicy=" + interestResultPolicy + "; action="
        + action + "]";
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
