/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
   
   
package org.apache.geode.internal.admin.remote;

import org.apache.geode.distributed.internal.*;
import org.apache.geode.internal.*;

import java.io.*;
import org.apache.geode.distributed.internal.membership.*;

/**
 * A message that is sent to a particular distribution manager to
 * modify its current {@link org.apache.geode.internal.Config}.
 */
public final class StoreSysCfgResponse extends AdminResponse {
  // instance variables


  // static methods
  /**
   * Returns a <code>StoreSysCfgResponse</code> that states that a
   * given set of distribution managers are known by <code>dm</code>
   * to be started.
   */
  public static StoreSysCfgResponse create(DistributionManager dm, InternalDistributedMember recipient, Config sc) {
    StoreSysCfgResponse m = new StoreSysCfgResponse();
    m.setRecipient(recipient);
    InternalDistributedSystem sys = dm.getSystem();
    Config conf = sys.getConfig();
    String[] names = conf.getAttributeNames();
    for (int i=0; i<names.length; i++) {
      if (conf.isAttributeModifiable(names[i])) {
        conf.setAttributeObject(names[i], sc.getAttributeObject(names[i]), ConfigSource.runtime());
      }
    }
      
    return m;
  }

  // instance methods
  
  public int getDSFID() {
    return STORE_SYS_CFG_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public String toString() {
    return "StoreSysCfgResponse from " + this.getRecipient();
  }
}
