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
import java.util.Arrays;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

public class ClientDataSerializerMessage extends ClientUpdateMessageImpl {
  private byte[][] serializedDataSerializer;

  private Class[][] supportedClasses;

  public ClientDataSerializerMessage(EnumListenerEvent operation, byte[][] dataSerializer,
      ClientProxyMembershipID memberId, EventID eventIdentifier, Class[][] supportedClasses) {
    super(operation, memberId, eventIdentifier);
    this.serializedDataSerializer = dataSerializer;
    this.supportedClasses = supportedClasses;
  }

  /**
   * default constructor
   *
   */
  public ClientDataSerializerMessage() {

  }

  @Override
  public boolean shouldBeConflated() {
    return false;
  }

  /**
   * Returns a <code>Message</code> generated from the fields of this
   * <code>ClientDataSerializerMessage</code>.
   *
   * @param latestValue byte[] containing the latest value to use. This could be the original value
   *        if conflation is not enabled, or it could be a conflated value if conflation is enabled.
   * @return a <code>Message</code> generated from the fields of this
   *         <code>ClientDataSerializerMessage</code>
   * @see org.apache.geode.internal.cache.tier.sockets.Message
   */
  @Override
  protected Message getMessage(CacheClientProxy proxy, byte[] latestValue) throws IOException {
    if (proxy.getVersion().compareTo(Version.GFE_6516) >= 0) {
      return getGFE6516Message(proxy.getVersion());
    } else if (proxy.getVersion().compareTo(Version.GFE_57) >= 0) {
      return getGFEMessage(proxy.getVersion());
    } else {
      throw new IOException("Unsupported client version for server-to-client message creation: "
          + proxy.getVersion());
    }
  }

  protected Message getGFEMessage(Version clientVersion) {
    Message message = null;
    int dataSerializerLength = this.serializedDataSerializer.length;
    message = new Message(dataSerializerLength + 1, clientVersion); // one for eventID
    // Set message type
    message.setMessageType(MessageType.REGISTER_DATASERIALIZERS);
    for (int i = 0; i < dataSerializerLength; i = i + 2) {
      message.addBytesPart(this.serializedDataSerializer[i]);
      message.addBytesPart(this.serializedDataSerializer[i + 1]);
    }
    message.setTransactionId(0);
    message.addObjPart(this.getEventId());
    return message;
  }

  protected Message getGFE6516Message(Version clientVersion) {
    Message message = null;

    // The format:
    // part 0: serializer1 classname
    // part 1: serializer1 id
    // part 2: serializer1 number of supported classes --|
    // part 3: serializer1 supported class1 name |
    // part 4: serializer1 supported class2 name |---> additional parts since 6.5.1.6
    // part 5: serializer1 supported classN name --|
    // part 6: serializer2 classname
    // part 7: serializer2 id
    // part 8: serializer2 number of supported classes
    // part 9: serializer2 supported class1 name
    // part 10: serializer2 supported classN name
    // ...
    // Last part: event ID

    int dsLength = this.serializedDataSerializer.length; // multiple of 2
    assert (dsLength % 2) == 0;
    int numOfDS = (this.supportedClasses != null) ? this.supportedClasses.length : 0;
    assert (dsLength / 2) == numOfDS;

    // Calculate total number of parts
    int numOfParts = dsLength + numOfDS;
    for (int i = 0; i < numOfDS; i++) {
      if (this.supportedClasses[i] != null) {
        numOfParts += this.supportedClasses[i].length;
      }
    }
    numOfParts += 1; // one for eventID
    // this._logger.fine("Number of parts for ClientDataSerializerMessage: "
    // + numOfParts + ", with eventID: " + this.getEventId());

    message = new Message(numOfParts, clientVersion);
    // Set message type
    message.setMessageType(MessageType.REGISTER_DATASERIALIZERS);
    for (int i = 0; i < dsLength; i = i + 2) {
      message.addBytesPart(this.serializedDataSerializer[i]); // part 0
      message.addBytesPart(this.serializedDataSerializer[i + 1]); // part 1

      int numOfClasses = this.supportedClasses[i / 2].length;
      byte[][] classBytes = new byte[numOfClasses][];
      try {
        for (int j = 0; j < numOfClasses; j++) {
          classBytes[j] = CacheServerHelper.serialize(this.supportedClasses[i / 2][j].getName());
        }
      } catch (IOException ioe) {
        numOfClasses = 0;
        classBytes = null;
      }
      message.addIntPart(numOfClasses); // part 2
      for (int j = 0; j < numOfClasses; j++) {
        message.addBytesPart(classBytes[j]); // part 3 onwards
      }
    }
    message.setTransactionId(0);
    message.addObjPart(this.getEventId()); // last part
    return message;
  }

  @Override
  public int getDSFID() {
    return CLIENT_DATASERIALIZER_MESSAGE;
  }

  /**
   * Writes an object to a <code>Datautput</code>.
   *
   * @throws IOException If this serializer cannot write an object to <code>out</code>.
   * @see DataSerializableFixedID#toData(DataOutput, SerializationContext)
   */
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {

    out.writeByte(_operation.getEventCode());
    int dataSerializerCount = this.serializedDataSerializer.length;
    out.writeInt(dataSerializerCount);
    for (int i = 0; i < dataSerializerCount; i++) {
      DataSerializer.writeByteArray(this.serializedDataSerializer[i], out);
    }
    context.getSerializer().writeObject(_membershipId, out);
    context.getSerializer().writeObject(_eventIdentifier, out);
  }

  /**
   * Reads an object from a <code>DataInput</code>.
   *
   * @throws IOException If this serializer cannot read an object from <code>in</code>.
   * @throws ClassNotFoundException If the class for an object being restored cannot be found.
   * @see DataSerializableFixedID#fromData(DataInput, DeserializationContext)
   */
  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    // Note: does not call super.fromData what a HACK
    _operation = EnumListenerEvent.getEnumListenerEvent(in.readByte());
    int dataSerializerCount = in.readInt();
    this.serializedDataSerializer = new byte[dataSerializerCount][];
    for (int i = 0; i < dataSerializerCount; i++) {
      this.serializedDataSerializer[i] = DataSerializer.readByteArray(in);
    }
    _membershipId = ClientProxyMembershipID.readCanonicalized(in);
    _eventIdentifier = (EventID) context.getDeserializer().readObject(in);
  }

  @Override
  public Object getKeyToConflate() {
    return null;
  }

  @Override
  public String getRegionToConflate() {
    return null;
  }

  @Override
  public Object getValueToConflate() {
    return null;
  }

  @Override
  public void setLatestValue(Object value) {}

  @Override
  public boolean isClientInterested(ClientProxyMembershipID clientId) {
    return true;
  }

  @Override
  public boolean needsNoAuthorizationCheck() {
    return true;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("ClientDataSerializerMessage[value=")
        .append(Arrays.toString(this.serializedDataSerializer))
        .append(";memberId=")
        .append(getMembershipId()).append(";eventId=").append(getEventId()).append("]");
    return buffer.toString();
  }
}
