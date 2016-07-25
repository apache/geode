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

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.GemFireHealth;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular agent who was registered
 * a health listener on a GemFireVM. This message is only sent when
 * the health status changes.
 * @since GemFire 3.5
 */
public final class HealthListenerMessage extends PooledDistributionMessage implements AdminMessageType {
  //instance variables
  private int listenerId;
  private GemFireHealth.Health status;

  public static HealthListenerMessage create(int listenerId,
                                             GemFireHealth.Health status){
    HealthListenerMessage m = new HealthListenerMessage();
    m.listenerId = listenerId;
    m.status = status;
    return m;
  }

  @Override  
  public void process(DistributionManager dm){
    RemoteGfManagerAgent agent = dm.getAgent();
    if (agent != null) {
      RemoteGemFireVM mgr = agent.getMemberById(this.getSender());
      if (mgr != null) {
        mgr.callHealthListeners(this.listenerId, this.status);
      }
    }
  }

  public int getDSFID() {
    return HEALTH_LISTENER_MESSAGE;
  }
  
  @Override
  public boolean sendViaUDP() {
    return true;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.listenerId);
    DataSerializer.writeObject(this.status, out);
  }

  @Override  
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.listenerId = in.readInt();
    this.status = (GemFireHealth.Health) DataSerializer.readObject(in);
  }

  @Override  
  public String toString() {
    return LocalizedStrings.HealthListenerMessage_THE_STATUS_OF_LISTENER_0_IS_1.toLocalizedString(new Object[] {Integer.valueOf(this.listenerId), this.status});
  }
  
}
