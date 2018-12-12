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
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.distributed.internal.DistributionManager;

/**
 * A message that is sent to a particular distribution manager to add a health listener.
 *
 * @since GemFire 3.5
 */
public class AddHealthListenerRequest extends AdminRequest {
  // instance variables
  private GemFireHealthConfig cfg;

  /**
   * Returns a <code>AddHealthListenerRequest</code> to be sent to the specified recipient.
   *
   * @throws NullPointerException If <code>cfg</code> is <code>null</code>
   */
  public static AddHealthListenerRequest create(GemFireHealthConfig cfg) {
    if (cfg == null) {
      throw new NullPointerException(
          "Null GemFireHealthConfig");
    }

    AddHealthListenerRequest m = new AddHealthListenerRequest();
    m.cfg = cfg;
    return m;
  }

  public AddHealthListenerRequest() {
    friendlyName =
        "Add health listener";
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return AddHealthListenerResponse.create(dm, this.getSender(), this.cfg);
  }

  public int getDSFID() {
    return ADD_HEALTH_LISTENER_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.cfg, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.cfg = (GemFireHealthConfig) DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "AddHealthListenerRequest from " + this.getRecipient() + " cfg=" + this.cfg;
  }
}
