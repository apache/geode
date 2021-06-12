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

import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Class <code>ClientPingMessageImpl</code> is a ping message that is periodically placed in the
 * <code>CacheClientProxy</code>'s queue to verify the client connection is still alive.
 *
 *
 * @since GemFire 6.6.2.x
 */
public class ClientPingMessageImpl implements ClientMessage {

  private static final long serialVersionUID = 5423895238521508743L;

  /**
   * Default constructor.
   */
  public ClientPingMessageImpl() {}

  @Override
  public Message getMessage(CacheClientProxy proxy, boolean notify) throws IOException {
    Message message = new Message(0, KnownVersion.CURRENT);
    message.setMessageType(MessageType.SERVER_TO_CLIENT_PING);
    message.setTransactionId(0);
    return message;
  }

  @Override
  public boolean shouldBeConflated() {
    return true;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {}

  @Override
  public int getDSFID() {
    return CLIENT_PING_MESSAGE_IMPL;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {}

  @Override
  public EventID getEventId() {
    return null;
  }

  @Override
  public String getRegionToConflate() {
    return "gemfire_reserved_region_name_for_client_ping";
  }

  @Override
  public Object getKeyToConflate() {
    // This method can be called by HARegionQueue.
    // Use this to identify the message type.
    return "ping";
  }

  @Override
  public Object getValueToConflate() {
    // This method can be called by HARegionQueue
    // Use this to identify the message type.
    return "ping";
  }

  @Override
  public void setLatestValue(Object value) {

  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
