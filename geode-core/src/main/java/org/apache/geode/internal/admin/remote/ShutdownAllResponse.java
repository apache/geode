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
package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class ShutdownAllResponse extends AdminResponse {
  private transient boolean isToShutDown = true;

  public ShutdownAllResponse() {}

  @Override
  public boolean getInlineProcess() {
    return true;
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  @Override
  public boolean orderedDelivery() {
    return true;
  }

  public ShutdownAllResponse(InternalDistributedMember sender, boolean isToShutDown) {
    this.setRecipient(sender);
    this.isToShutDown = isToShutDown;
  }

  @Override
  public int getDSFID() {
    return SHUTDOWN_ALL_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeBoolean(isToShutDown);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.isToShutDown = in.readBoolean();
  }

  @Override
  public String toString() {
    return "ShutdownAllResponse from " + this.getSender() + " msgId=" + this.getMsgId()
        + " isToShutDown=" + this.isToShutDown;
  }

  public boolean isToShutDown() {
    return isToShutDown;
  }
}
