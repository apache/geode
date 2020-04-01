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
package org.apache.geode.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

/**
 * A member of the cluster sends a HeartbeatRequestMessage to another member if it suspects
 * that member is gone. A member receiving one of these messages should respond with a
 * HeartbeatMessage having the same requestId as this message.
 */
public class HeartbeatRequestMessage<ID extends MemberIdentifier> extends AbstractGMSMessage<ID> {

  int requestId;
  ID target;

  public HeartbeatRequestMessage(ID neighbour, int id) {
    requestId = id;
    this.target = neighbour;
  }

  public HeartbeatRequestMessage() {}

  public ID getTarget() {
    return target;
  }

  /**
   * If no response is desired the requestId can be reset by invoking this method
   */
  public void clearRequestId() {
    requestId = -1;
  }

  @Override
  public int getDSFID() {
    return HEARTBEAT_REQUEST;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [requestId=" + requestId + "]";
  }

  public int getRequestId() {
    return requestId;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeInt(requestId);
    context.getSerializer().writeObject(target, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    requestId = in.readInt();
    target = context.getDeserializer().readObject(in);
  }

  @Override
  public boolean isHighPriority() {
    return true;
  }

}
