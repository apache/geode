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
 * Members send HeartbeatMessages to other members periodically in order to inform them
 * that the sender is alive and should not be subjected to suspect-processing.<br>
 * No response is required.
 */
public class HeartbeatMessage<ID extends MemberIdentifier> extends AbstractGMSMessage<ID> {
  /**
   * RequestId identifies the HeartbeatRequestMessage for which this is a response. If it is < 0
   * this is a periodic heartbeat message.
   */
  int requestId;

  public HeartbeatMessage(int id) {
    requestId = id;
  }

  public HeartbeatMessage() {}

  public int getRequestId() {
    return requestId;
  }


  @Override
  public int getDSFID() {
    return HEARTBEAT_RESPONSE;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [requestId=" + requestId + "]";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeInt(requestId);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    requestId = in.readInt();
  }

  @Override
  public boolean isHighPriority() {
    return true;
  }
}
