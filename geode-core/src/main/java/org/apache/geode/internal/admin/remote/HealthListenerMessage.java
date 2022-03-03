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

import org.apache.geode.DataSerializer;
import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.distributed.internal.AdminMessageType;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to a particular agent who was registered a health listener on a GemFireVM.
 * This message is only sent when the health status changes.
 *
 * @since GemFire 3.5
 */
public class HealthListenerMessage extends PooledDistributionMessage implements AdminMessageType {
  // instance variables
  private int listenerId;
  private GemFireHealth.Health status;

  public static HealthListenerMessage create(int listenerId, GemFireHealth.Health status) {
    HealthListenerMessage m = new HealthListenerMessage();
    m.listenerId = listenerId;
    m.status = status;
    return m;
  }

  @Override
  public void process(ClusterDistributionManager dm) {
    RemoteGfManagerAgent agent = dm.getAgent();
    if (agent != null) {
      RemoteGemFireVM mgr = agent.getMemberById(getSender());
      if (mgr != null) {
        mgr.callHealthListeners(listenerId, status);
      }
    }
  }

  @Override
  public int getDSFID() {
    return HEALTH_LISTENER_MESSAGE;
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(listenerId);
    DataSerializer.writeObject(status, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    listenerId = in.readInt();
    status = DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return String.format("The status of listener %s is %s",
        listenerId, status);
  }

}
