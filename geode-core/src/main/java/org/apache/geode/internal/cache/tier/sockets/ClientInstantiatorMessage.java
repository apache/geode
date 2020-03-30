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

/**
 * Class <code>ClientInstantiatorMessage</code> represents a message that is to be sent to the
 * client from a server , when a new <code>Instantiator</code>. object is registerd on Server. This
 * message contains array of serailized instantiators along with the unique <code>EventID</code>
 *
 *
 * @since GemFire 5.0
 */
public class ClientInstantiatorMessage extends ClientUpdateMessageImpl {
  private static final long serialVersionUID = 2949326125521840437L;
  /**
   * Serialized 2D array of the instantiators
   */
  private byte[][] serializedInstantiators;

  /**
   * Constructor.
   *
   * @param operation The operation performed (e.g. AFTER_CREATE, AFTER_UPDATE, AFTER_DESTROY,
   *        AFTER_INVALIDATE, AFTER_REGION_DESTROY)
   * @param instantiator Serialized 2D array of the instantiators
   * @param memberId membership id of the originator of the event
   * @param eventIdentifier EventID of this message
   */
  public ClientInstantiatorMessage(EnumListenerEvent operation, byte[][] instantiator,
      ClientProxyMembershipID memberId, EventID eventIdentifier) {
    super(operation, memberId, eventIdentifier);
    this.serializedInstantiators = instantiator;
  }

  /*
   * (non-Javadoc) reimplemented to state that all clients are interested in this message.
   *
   * @see
   * org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl#isClientInterested(org.
   * apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID)
   */
  @Override
  public boolean isClientInterested(ClientProxyMembershipID clientId) {
    return true;
  }

  @Override
  public boolean needsNoAuthorizationCheck() {
    return true;
  }

  /**
   * default constructor
   *
   */
  public ClientInstantiatorMessage() {

  }

  // /**
  // * Returns the serialized value of Instantiators.
  // *
  // * @return the serialized value of Instantiators
  // */
  // public byte[][] getInstantiators()
  // {
  // return this.serializedInstantiators;
  // }

  /**
   * Determines whether or not to conflate this message.
   *
   * @return Whether to conflate this message
   */
  @Override
  public boolean shouldBeConflated() {
    return false;
  }

  @Override
  protected Message getMessage(CacheClientProxy proxy, byte[] latestValue) throws IOException {
    Version clientVersion = proxy.getVersion();
    Message message = null;
    if (clientVersion.compareTo(Version.GFE_57) >= 0) {
      message = getGFEMessage(proxy.getProxyID(), null, clientVersion);
    } else {
      throw new IOException(
          "Unsupported client version for server-to-client message creation: " + clientVersion);
    }

    return message;
  }

  @Override
  protected Message getGFEMessage(ClientProxyMembershipID proxy, byte[] latestValue,
      Version clientVersion) throws IOException {
    Message message = null;
    int instantiatorsLength = this.serializedInstantiators.length;
    message = new Message(instantiatorsLength + 1, clientVersion); // one for eventID
    // Set message type
    message.setMessageType(MessageType.REGISTER_INSTANTIATORS);
    for (int i = 0; i < instantiatorsLength; i = i + 3) {
      message.addBytesPart(this.serializedInstantiators[i]);
      message.addBytesPart(this.serializedInstantiators[i + 1]);
      message.addBytesPart(this.serializedInstantiators[i + 2]);
    }
    message.setTransactionId(0);
    message.addObjPart(this.getEventId());
    return message;
  }

  @Override
  public int getDSFID() {
    return CLIENT_INSTANTIATOR_MESSAGE;
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
    // Note: does not call super.toData what a HACK
    out.writeByte(_operation.getEventCode());
    int instantiatorCount = this.serializedInstantiators.length;
    out.writeInt(instantiatorCount);
    for (int i = 0; i < instantiatorCount; i++) {
      DataSerializer.writeByteArray(this.serializedInstantiators[i], out);
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
    int instantiatorCount = in.readInt(); // is byte suficient for this ?
    this.serializedInstantiators = new byte[instantiatorCount][];
    for (int i = 0; i < instantiatorCount; i++) {
      this.serializedInstantiators[i] = DataSerializer.readByteArray(in);
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
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("ClientInstantiatorMessage[value=")
        .append(Arrays.deepToString(this.serializedInstantiators))
        .append(";memberId=")
        .append(getMembershipId()).append(";eventId=").append(getEventId())
        .append("]");
    return buffer.toString();
  }

}
