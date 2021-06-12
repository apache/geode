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

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Class <code>ClientMarkerMessageImpl</code> is a marker message that is placed in the
 * <code>CacheClientProxy</code>'s queue when the client connects to notify the client that all of
 * its queued updates have been sent. This is to be used mostly by the durable clients, although all
 * clients receive it.
 *
 *
 * @since GemFire 5.5
 */
public class ClientMarkerMessageImpl implements ClientMessage {
  private static final long serialVersionUID = 5423895238521508743L;

  /**
   * This <code>ClientMessage</code>'s <code>EventID</code>
   */
  private EventID eventId;

  /**
   * Constructor.
   *
   * @param eventId This <code>ClientMessage</code>'s <code>EventID</code>
   */
  public ClientMarkerMessageImpl(EventID eventId) {
    this.eventId = eventId;
  }

  /**
   * Default constructor.
   */
  public ClientMarkerMessageImpl() {}

  @Override
  public Message getMessage(CacheClientProxy proxy, boolean notify) throws IOException {
    Message message = new Message(1, KnownVersion.CURRENT);
    message.setMessageType(MessageType.CLIENT_MARKER);
    message.setTransactionId(0);
    message.addObjPart(eventId);
    return message;
  }

  @Override
  public boolean shouldBeConflated() {
    return true;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeObject(eventId, out);
  }

  @Override
  public int getDSFID() {
    return CLIENT_MARKER_MESSAGE_IMPL;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    eventId = DataSerializer.readObject(in);
  }

  @Override
  public EventID getEventId() {
    return eventId;
  }

  @Override
  public String getRegionToConflate() {
    return "gemfire_reserved_region_name_for_durable_client_marker";
  }

  @Override
  public Object getKeyToConflate() {
    // This method can be called by HARegionQueue.
    // Use this to identify the message type.
    return "marker";
  }

  @Override
  public Object getValueToConflate() {
    // This method can be called by HARegionQueue
    // Use this to identify the message type.
    return "marker";
  }

  @Override
  public void setLatestValue(Object value) {}

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
