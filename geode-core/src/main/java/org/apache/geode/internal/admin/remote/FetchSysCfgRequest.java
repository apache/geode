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

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to a particular distribution manager to get its current
 * {@link org.apache.geode.internal.Config}.
 */
public class FetchSysCfgRequest extends AdminRequest {
  /**
   * Returns a <code>FetchSysCfgRequest</code> to be sent to the specified recipient.
   */
  public static FetchSysCfgRequest create() {
    FetchSysCfgRequest m = new FetchSysCfgRequest();
    return m;
  }

  public FetchSysCfgRequest() {
    friendlyName =
        "Fetch configuration parameters";
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return FetchSysCfgResponse.create(dm, this.getSender());
  }

  @Override
  public int getDSFID() {
    return FETCH_SYS_CFG_REQUEST;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
  }

  @Override
  public String toString() {
    return "FetchSysCfgRequest sent to " + this.getRecipient() + " from " + this.getSender();
  }
}
