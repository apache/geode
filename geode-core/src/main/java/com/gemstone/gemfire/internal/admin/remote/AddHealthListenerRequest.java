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
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.GemFireHealthConfig;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular distribution manager to
 * add a health listener.
 * @since GemFire 3.5
 */
public final class AddHealthListenerRequest extends AdminRequest {
  // instance variables
  private GemFireHealthConfig cfg;

  /**
   * Returns a <code>AddHealthListenerRequest</code> to be sent to the
   * specified recipient.
   *
   * @throws NullPointerException
   *         If <code>cfg</code> is <code>null</code>
   */
  public static AddHealthListenerRequest create(GemFireHealthConfig cfg) {
    if (cfg == null) {
      throw new NullPointerException(LocalizedStrings.AddHealthListenerRequest_NULL_GEMFIREHEALTHCONFIG.toLocalizedString());
    }

    AddHealthListenerRequest m = new AddHealthListenerRequest();
    m.cfg = cfg;
    return m;
  }

  public AddHealthListenerRequest() {
    friendlyName = LocalizedStrings.AddHealthListenerRequest_ADD_HEALTH_LISTENER.toLocalizedString();
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
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.cfg = (GemFireHealthConfig)DataSerializer.readObject(in);
  }

  @Override  
  public String toString() {
    return "AddHealthListenerRequest from " + this.getRecipient() + " cfg=" + this.cfg;
  }
}
