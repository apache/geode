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

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

public class ClientReAuthenticateMessage implements ClientMessage {
  @Immutable
  public static final KnownVersion RE_AUTHENTICATION_START_VERSION = KnownVersion.GEODE_1_15_0;
  /**
   * This {@code ClientMessage}'s {@code EventID}
   */
  private final EventID eventId;

  public ClientReAuthenticateMessage() {
    this(new EventID());
  }

  public ClientReAuthenticateMessage(EventID eventId) {
    this.eventId = eventId;
  }

  @Override
  public Message getMessage(CacheClientProxy proxy, boolean notify) throws IOException {
    Message message = new Message(1, KnownVersion.CURRENT);
    message.setMessageType(MessageType.CLIENT_RE_AUTHENTICATE);
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
    eventId.toData(out, context);
  }

  @Override
  public int getDSFID() {
    return CLIENT_RE_AUTHENTICATE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    eventId.fromData(in, context);
  }

  @Override
  public EventID getEventId() {
    return eventId;
  }

  @Override
  public String getRegionToConflate() {
    return "gemfire_reserved_region_name_for_client_re_authenticate";
  }

  @Override
  public Object getKeyToConflate() {
    // This method can be called by HARegionQueue.
    // Use this to identify the message type.
    return "re_authenticate";
  }

  @Override
  public Object getValueToConflate() {
    // This method can be called by HARegionQueue
    // Use this to identify the message type.
    return "re_authenticate";
  }

  @Override
  public void setLatestValue(Object value) {}

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
