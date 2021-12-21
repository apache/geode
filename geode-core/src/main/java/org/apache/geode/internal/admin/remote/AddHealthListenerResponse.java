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

import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * The response to adding a health listener.
 *
 * @since GemFire 3.5
 */
public class AddHealthListenerResponse extends AdminResponse {
  // instance variables
  int listenerId;

  /**
   * Returns a <code>AddHealthListenerResponse</code> that will be returned to the specified
   * recipient.
   */
  public static AddHealthListenerResponse create(DistributionManager dm,
      InternalDistributedMember recipient, GemFireHealthConfig cfg) {
    AddHealthListenerResponse m = new AddHealthListenerResponse();
    m.setRecipient(recipient);
    dm.createHealthMonitor(recipient, cfg);
    m.listenerId = dm.getHealthMonitor(recipient).getId();
    return m;
  }

  // instance methods
  public int getHealthListenerId() {
    return listenerId;
  }

  @Override
  public int getDSFID() {
    return ADD_HEALTH_LISTENER_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(listenerId);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    listenerId = in.readInt();
  }

  @Override
  public String toString() {
    return "AddHealthListenerResponse from " + getRecipient() + " listenerId="
        + listenerId;
  }
}
