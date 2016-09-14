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
import org.apache.geode.*;
import org.apache.geode.internal.*;
import java.io.*;
//import java.util.*;
import org.apache.geode.distributed.internal.membership.*;

/**
 * A message that is sent to a particular distribution manager to
 * get its current {@link Config}.
 */
public final class FetchSysCfgResponse extends AdminResponse {
  // instance variables
  Config sc;
  
  /**
   * Returns a <code>FetchSysCfgResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * config.
   */
  public static FetchSysCfgResponse create(DistributionManager dm, InternalDistributedMember recipient) {
    FetchSysCfgResponse m = new FetchSysCfgResponse();
    m.setRecipient(recipient);
    Config conf = dm.getSystem().getConfig();  
    if (conf instanceof RuntimeDistributionConfigImpl) {
      m.sc = ((RuntimeDistributionConfigImpl)conf).takeSnapshot();
    }
    return m;
  }

  // instance methods
  public Config getConfig() {
    return this.sc;
  }
  
  public int getDSFID() {
    return FETCH_SYS_CFG_RESPONSE;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.sc, out);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.sc = (Config)DataSerializer.readObject(in);
  }

  @Override  
  public String toString() {
    return "FetchSysCfgResponse from " + this.getRecipient() + " cfg=" + this.sc;
  }
}
