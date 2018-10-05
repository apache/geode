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
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.Config;

/**
 * A message that is sent to a particular distribution manager to modify its current
 * {@link org.apache.geode.internal.Config}.
 */
public class StoreSysCfgRequest extends AdminRequest {
  // instance variables
  Config sc;

  /**
   * Returns a <code>StoreSysCfgRequest</code> that states that a given set of distribution managers
   * are known by <code>dm</code> to be started.
   */
  public static StoreSysCfgRequest create(Config sc) {
    StoreSysCfgRequest m = new StoreSysCfgRequest();
    m.sc = sc;
    return m;
  }

  public StoreSysCfgRequest() {
    friendlyName =
        "Apply configuration parameters";
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return StoreSysCfgResponse.create(dm, this.getSender(), this.sc);
  }

  public int getDSFID() {
    return STORE_SYS_CFG_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.sc, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.sc = (Config) DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "StoreSysCfgRequest from " + this.getRecipient() + " syscfg=" + this.sc;
  }
}
