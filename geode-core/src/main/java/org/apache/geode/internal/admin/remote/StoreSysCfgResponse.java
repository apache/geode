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
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Config;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to a particular distribution manager to modify its current
 * {@link org.apache.geode.internal.Config}.
 */
public class StoreSysCfgResponse extends AdminResponse {
  // instance variables


  // static methods
  /**
   * Returns a <code>StoreSysCfgResponse</code> that states that a given set of distribution
   * managers are known by <code>dm</code> to be started.
   */
  public static StoreSysCfgResponse create(DistributionManager dm,
      InternalDistributedMember recipient, Config sc) {
    StoreSysCfgResponse m = new StoreSysCfgResponse();
    m.setRecipient(recipient);
    InternalDistributedSystem sys = dm.getSystem();
    Config conf = sys.getConfig();
    String[] names = conf.getAttributeNames();
    for (final String name : names) {
      if (conf.isAttributeModifiable(name)) {
        conf.setAttributeObject(name, sc.getAttributeObject(name), ConfigSource.runtime());
      }
    }

    return m;
  }

  // instance methods

  @Override
  public int getDSFID() {
    return STORE_SYS_CFG_RESPONSE;
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
    return "StoreSysCfgResponse from " + getRecipient();
  }
}
